"""Cloudflare backend relay for BalancerWithVSCode.

This file opens a persistent WebSocket connection to Cloudflare Worker and
hosts a local SOCKS5 listener on port 60000. Local backend processes can
connect to this upstream when the Cloudflare worker is acting as a relay.
"""

import json
import os
import socket
import ssl
import struct
import threading
import time
from urllib.parse import urlparse, urlunparse

import websocket
from config import load_or_create_config

DEFAULTS = {
    "Local_Host": "127.0.0.1",
    "Local_Port": 60000,
    "worker_url": "https://your-worker.workers.dev/ws",
    "worker_channel": "default",
    "worker_token": "PUT_YOUR_WORKER_TOKEN_HERE",
    "MAX_CHUNK_SIZE": 32768,
}

_cf_defaults = DEFAULTS
_cf_cfg = load_or_create_config("cf_back.config.json", _cf_defaults)
Local_Host = _cf_cfg.get("Local_Host", _cf_defaults["Local_Host"])
Local_Port = _cf_cfg.get("Local_Port", _cf_defaults["Local_Port"])
WORKER_URL = _cf_cfg.get("worker_url", _cf_defaults["worker_url"])
WORKER_CHANNEL = _cf_cfg.get("worker_channel", _cf_defaults["worker_channel"])
WORKER_TOKEN = _cf_cfg.get("worker_token", _cf_defaults["worker_token"])
MAX_CHUNK_SIZE = _cf_cfg.get("MAX_CHUNK_SIZE", _cf_defaults["MAX_CHUNK_SIZE"])
WORKER_ROLE = "back"

_sessions = {}
_worker_session_map = {}
_sessions_lock = threading.Lock()
_session_counter = 1

ws = None
ws_lock = threading.Lock()
_ws_ready = threading.Event()
_stop_event = threading.Event()


def _next_session_id() -> int:
    global _session_counter
    with _sessions_lock:
        sid = _session_counter
        _session_counter += 1
        if _session_counter >= 2**31:
            _session_counter = 1
        return sid


def _build_worker_url() -> str:
    parsed = urlparse(WORKER_URL)
    path = parsed.path or "/"
    if path.endswith("/"):
        path = path.rstrip("/") + "/ws"
    elif not path.endswith("/ws"):
        path = path + "/ws"

    query = parsed.query
    if query:
        query = f"{query}&role={WORKER_ROLE}&channel={WORKER_CHANNEL}"
    else:
        query = f"role={WORKER_ROLE}&channel={WORKER_CHANNEL}"

    return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, query, parsed.fragment))


def _connect_worker():
    headers = []
    if WORKER_TOKEN:
        headers.append(f"X-Worker-Token: {WORKER_TOKEN}")
    url = _build_worker_url()
    url = url.replace("https://", "wss://").replace("http://", "ws://")
    options = {"cert_reqs": ssl.CERT_REQUIRED}
    return websocket.create_connection(url, header=headers, sslopt=options)


def _reset_ws():
    global ws
    with ws_lock:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
            ws = None


def _ws_receive_loop():
    global ws
    while not _stop_event.is_set():
        try:
            with ws_lock:
                conn = ws
            if conn is None:
                time.sleep(1)
                continue

            message = conn.recv()
            if isinstance(message, bytes):
                if len(message) < 4:
                    continue
                worker_session_id = struct.unpack(">I", message[:4])[0]
                payload = message[4:]
                with _sessions_lock:
                    local_session_id = _worker_session_map.get(worker_session_id)
                    session = _sessions.get(local_session_id)
                if session:
                    try:
                        session["socket"].sendall(payload)
                    except Exception:
                        pass
            else:
                try:
                    obj = json.loads(message)
                except Exception:
                    continue
                msg_type = obj.get("type")
                if msg_type == "assigned":
                    client_id = obj.get("client_id")
                    worker_session_id = obj.get("session_id")
                    buffer = []
                    with _sessions_lock:
                        session = _sessions.get(client_id)
                        if session is not None:
                            session["worker_session_id"] = worker_session_id
                            _worker_session_map[worker_session_id] = client_id
                            buffer = session.get("buffer", [])
                            session["buffer"] = []
                    for chunk in buffer:
                        _send_data(worker_session_id, chunk)
                elif msg_type == "close":
                    worker_session_id = obj.get("session_id")
                    with _sessions_lock:
                        local_session_id = _worker_session_map.pop(worker_session_id, None)
                        session = _sessions.pop(local_session_id, None) if local_session_id is not None else None
                    if session:
                        try:
                            session["socket"].close()
                        except Exception:
                            pass
                elif msg_type == "error":
                    print(f"[cf_back] Worker error: {obj.get('message')}")
        except websocket.WebSocketConnectionClosedException:
            _ws_ready.clear()
            _reset_ws()
            if _stop_event.is_set():
                break
            time.sleep(2)
        except Exception:
            _ws_ready.clear()
            _reset_ws()
            if _stop_event.is_set():
                break
            time.sleep(2)


def _ensure_worker_connection():
    global ws
    if _ws_ready.is_set():
        return
    with ws_lock:
        if ws is None:
            try:
                ws = _connect_worker()
                _ws_ready.set()
                print(f"[cf_back] Connected to Cloudflare worker at {_build_worker_url()}")
            except Exception as exc:
                print(f"[cf_back] Worker connect failed: {exc}")
                ws = None
                _ws_ready.clear()
                return


def _send_open(local_session_id: int, dst_addr: str, dst_port: int):
    body = json.dumps({
        "type": "open",
        "role": WORKER_ROLE,
        "client_id": local_session_id,
        "dst_addr": dst_addr,
        "dst_port": dst_port,
    })
    with ws_lock:
        if ws is not None:
            ws.send(body)


def _send_close(local_session_id: int):
    with _sessions_lock:
        session = _sessions.get(local_session_id)
        if session is None:
            return
        worker_session_id = session.get("worker_session_id")

    body = {"type": "close"}
    if worker_session_id is not None:
        body["session_id"] = worker_session_id
    else:
        body["client_id"] = local_session_id

    with ws_lock:
        if ws is not None:
            ws.send(json.dumps(body))


def _send_data(worker_session_id: int, data: bytes):
    with ws_lock:
        if ws is not None:
            frame = struct.pack(">I", worker_session_id) + data
            ws.send_binary(frame)


def _queue_worker_data(local_session_id: int, data: bytes):
    with _sessions_lock:
        session = _sessions.get(local_session_id)
        if session is None:
            return
        worker_session_id = session.get("worker_session_id")
        if worker_session_id is not None:
            _send_data(worker_session_id, data)
            return
        session.setdefault("buffer", []).append(data)


def _handle_socks5_client(client_sock: socket.socket, addr):
    session_id = None
    try:
        ver_nmethods = client_sock.recv(2)
        if len(ver_nmethods) != 2 or ver_nmethods[0] != 5:
            client_sock.close()
            return
        nmethods = ver_nmethods[1]
        methods = client_sock.recv(nmethods)
        client_sock.sendall(b"\x05\x00")

        hdr = client_sock.recv(4)
        if len(hdr) != 4 or hdr[0] != 5 or hdr[1] != 1:
            client_sock.close()
            return
        atyp = hdr[3]
        if atyp == 1:
            addr_bytes = client_sock.recv(4)
            dst_addr = socket.inet_ntoa(addr_bytes)
        elif atyp == 3:
            dlen = client_sock.recv(1)[0]
            dst_addr = client_sock.recv(dlen).decode("idna")
        elif atyp == 4:
            addr_bytes = client_sock.recv(16)
            dst_addr = socket.inet_ntop(socket.AF_INET6, addr_bytes)
        else:
            client_sock.close()
            return
        port_bytes = client_sock.recv(2)
        dst_port = int.from_bytes(port_bytes, "big")

        client_sock.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")

        session_id = _next_session_id()
        with _sessions_lock:
            _sessions[session_id] = {"socket": client_sock, "worker_session_id": None, "buffer": []}

        _send_open(session_id, dst_addr, dst_port)

        while True:
            chunk = client_sock.recv(MAX_CHUNK_SIZE)
            if not chunk:
                break
            _queue_worker_data(session_id, chunk)
    except Exception as exc:
        print(f"[cf_back] Local client error: {exc}")
    finally:
        if session_id is not None:
            _send_close(session_id)
            with _sessions_lock:
                session = _sessions.pop(session_id, None)
                if session is not None:
                    worker_session_id = session.get("worker_session_id")
                    if worker_session_id is not None:
                        _worker_session_map.pop(worker_session_id, None)
        try:
            client_sock.close()
        except Exception:
            pass


def _start_socks5_listener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((Local_Host, Local_Port))
    server.listen()
    print(f"[cf_back] SOCKS5 listener running on {Local_Host}:{Local_Port}")
    while True:
        client_sock, addr = server.accept()
        threading.Thread(target=_handle_socks5_client, args=(client_sock, addr), daemon=True).start()


def start():
    threading.Thread(target=_ws_receive_loop, daemon=True).start()
    while not _ws_ready.is_set():
        _ensure_worker_connection()
        if not _ws_ready.is_set():
            time.sleep(2)
    _start_socks5_listener()


if __name__ == "__main__":
    start()
