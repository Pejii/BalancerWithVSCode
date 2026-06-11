"""Backend stub for BalancerWithVSCode.

For now, backend accepts connections but does not process data.
Will be patched later for specific transport methods and data handling.
"""

import socket
import threading
from config import load_or_create_config

# Load `back.config.json` or create it with defaults
_back_defaults = {"Local_Host": "127.0.0.1", "Local_Port": 1080, "MAX_CHUNK_SIZE": 32768}
_back_cfg = load_or_create_config("back.config.json", _back_defaults)
Local_Host = _back_cfg.get("Local_Host", _back_defaults["Local_Host"])
Local_Port = _back_cfg.get("Local_Port", _back_defaults["Local_Port"])
MAX_CHUNK_SIZE = _back_cfg.get("MAX_CHUNK_SIZE", _back_defaults["MAX_CHUNK_SIZE"])


def handle_client(conn: socket.socket, addr):
    print(f"Backend: client connected from {addr}")
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            print(f"Backend: received {len(data)} bytes from {addr}")
            # TODO: process data according to transport method
    except Exception as e:
        print(f"Backend: error from {addr}: {e}")
    finally:
        conn.close()
        print(f"Backend: client disconnected from {addr}")


def start_backend():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((Local_Host, Local_Port))
    server.listen()
    print(f"Backend listening on {Local_Host}:{Local_Port}")

    try:
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("Backend shutting down")
    finally:
        server.close()


if __name__ == "__main__":
    start_backend()
