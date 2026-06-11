"""Frontend placeholder for BalancerWithVSCode."""

from protocol import (
    COMMAND_TRANSMIT,
    PayloadHeader,
    RequestIdGenerator,
    pack_chunk,
    make_flags,
)
from config import load_or_create_config
import socket
import threading

request_counter = RequestIdGenerator()

# Load `front.config.json` or create it with defaults
_front_defaults = {
    "Local_Host": "127.0.0.1",
    "Local_Port": 1080,
    "MAX_CHUNK_SIZE": 32768,
    "socks_auth_user": "",
    "socks_auth_password": "",
}
_front_cfg = load_or_create_config("front.config.json", _front_defaults)
Local_Host = _front_cfg.get("Local_Host", _front_defaults["Local_Host"])
Local_Port = _front_cfg.get("Local_Port", _front_defaults["Local_Port"])
MAX_CHUNK_SIZE = _front_cfg.get("MAX_CHUNK_SIZE", _front_defaults["MAX_CHUNK_SIZE"])
socks_auth_user = _front_cfg.get("socks_auth_user", _front_defaults["socks_auth_user"])
socks_auth_password = _front_cfg.get("socks_auth_password", _front_defaults["socks_auth_password"])


def start_frontend():
    # Start SOCKS5 listener using configured host/port
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((Local_Host, Local_Port))
    server.listen()
    print(f"Frontend SOCKS5 listening on {Local_Host}:{Local_Port}")

    def handle_client(conn: socket.socket, addr):
        try:
            # SOCKS5 handshake - method selection
            ver_nmethods = conn.recv(2)
            if len(ver_nmethods) < 2 or ver_nmethods[0] != 5:
                conn.close()
                return
            nmethods = ver_nmethods[1]
            methods = conn.recv(nmethods)
            
            # Determine if auth is required
            auth_required = bool(socks_auth_user or socks_auth_password)
            if auth_required:
                # Reply: version 5, method 0x02 (username/password auth)
                conn.sendall(b"\x05\x02")
                
                # Receive auth request: [version, ulen, uname, plen, passwd]
                auth_hdr = conn.recv(2)
                if len(auth_hdr) < 2:
                    conn.close()
                    return
                auth_version = auth_hdr[0]
                ulen = auth_hdr[1]
                
                username_bytes = conn.recv(ulen)
                plen_byte = conn.recv(1)
                if not plen_byte:
                    conn.close()
                    return
                plen = plen_byte[0]
                password_bytes = conn.recv(plen)
                
                username = username_bytes.decode("utf-8", errors="ignore")
                password = password_bytes.decode("utf-8", errors="ignore")
                
                # Check credentials
                if username == socks_auth_user and password == socks_auth_password:
                    # Auth success: reply [version, status=0]
                    conn.sendall(b"\x01\x00")
                else:
                    # Auth failure: reply [version, status=1]
                    conn.sendall(b"\x01\x01")
                    conn.close()
                    return
            else:
                # Reply: version 5, method 0x00 (no auth)
                conn.sendall(b"\x05\x00")

            # request
            hdr = conn.recv(4)
            if len(hdr) < 4 or hdr[0] != 5:
                conn.close()
                return
            cmd = hdr[1]
            atyp = hdr[3]
            if atyp == 1:  # IPv4
                addr_bytes = conn.recv(4)
                dst_addr = socket.inet_ntoa(addr_bytes)
            elif atyp == 3:  # domain
                dlen_b = conn.recv(1)
                if not dlen_b:
                    conn.close()
                    return
                dlen = dlen_b[0]
                dst_addr = conn.recv(dlen).decode("idna")
            elif atyp == 4:  # IPv6
                addr_bytes = conn.recv(16)
                dst_addr = socket.inet_ntop(socket.AF_INET6, addr_bytes)
            else:
                conn.close()
                return
            port_bytes = conn.recv(2)
            dst_port = int.from_bytes(port_bytes, "big")

            if cmd != 1:  # only CONNECT
                # reply: command not supported
                conn.sendall(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
                conn.close()
                return

            # Capture-only mode: print payload to console for now
            request_id = request_counter.next_id()
            print(f"\n[Request {request_id}] SOCKS5 CONNECT to {dst_addr}:{dst_port}")

            # Reply success to SOCKS5 client so it will start sending payload.
            conn.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")

            # Read client data and print to console
            try:
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    print(f"[Request {request_id}] Received {len(chunk)} bytes")
                    print(f"[Request {request_id}] Data: {chunk!r}")
            except Exception:
                pass

            print(f"[Request {request_id}] Connection closed")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    try:
        while True:
            c, a = server.accept()
            threading.Thread(target=handle_client, args=(c, a), daemon=True).start()
    except KeyboardInterrupt:
        server.close()

    # (Config already loaded at module import time)

if __name__ == "__main__":
    start_frontend()
