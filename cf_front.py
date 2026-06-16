# This version is using cloudflare worker to pipe data between upstream proxies and the server

import asyncio
import socket
import struct
import random
import urllib.parse
import collections
import websockets

# Configuration Variables
CHUNK_SIZE = 16384 # 65536 for fast connection, 16384 for slow connections
CHUNKS_PER_WORKER = 5
PACKET_FAILURE_TIMEOUT = 15.0
LOCAL_LISTENER_HOST = "127.0.0.1"
LOCAL_LISTENER_PORT = 1080

# Configuring relay
CLOUDFLARE_BASE_URL = "wss://websocket-bridge-pipe.kolang-733.workers.dev/"
DIRECT_BASE_URL = ""

UPSTREAM_PROXIES = [
    #("192.168.1.13", 1080, "cl1", "user123", "user123"),
    #("192.168.1.17", 1080, "cl2", "user123", "user123"),
    ("192.168.1.69", 15000, "cl1", "", ""),
    ("192.168.1.70", 15000, "cl2", "", ""),
]

in_flight_chunks = {}
active_connections = {}
worker_semaphores = {}


class Chunk:
    def __init__(self, conn_id, cmd, seq_id, payload):
        self.conn_id = conn_id
        self.cmd = cmd
        self.seq_id = seq_id
        self.payload = payload
        self.penalties = {}


class AsyncChunkQueue:
    def __init__(self):
        self._queue = collections.deque()
        self._event = asyncio.Event()

    async def put(self, chunk, front=False):
        if front:
            self._queue.appendleft(chunk)
        else:
            self._queue.append(chunk)
        self._event.set()

    async def get(self, proxy_id):
        while True:
            if not self._queue:
                self._event.clear()
                await self._event.wait()
                continue

            now = asyncio.get_event_loop().time()
            for chunk in self._queue:
                if proxy_id in chunk.penalties and now < chunk.penalties[proxy_id]:
                    continue
                self._queue.remove(chunk)
                return chunk
            await asyncio.sleep(0.02)


pending_queue = AsyncChunkQueue()


def safe_release_semaphore(client_id):
    sem = worker_semaphores.get(client_id)
    if sem:
        try:
            sem.release()
        except ValueError:
            pass


class ReassemblyBuffer:
    def __init__(self, conn_id, write_callback, close_callback):
        self.conn_id = conn_id
        self.expected_seq = 0
        self.buffer = {}
        self.write_callback = write_callback
        self.close_callback = close_callback

    async def add_chunk(self, cmd, seq_id, payload):
        if seq_id < self.expected_seq:
            return

        self.buffer[seq_id] = (cmd, payload)

        while self.expected_seq in self.buffer:
            curr_cmd, curr_payload = self.buffer.pop(self.expected_seq)
            try:
                if curr_cmd == 2 and curr_payload:
                    await self.write_callback(curr_payload)
                elif curr_cmd == 3:
                    print(f" [Local Client] Closing stream sequence for Conn ID: {self.conn_id}")
                    await self.close_callback()
            except (ConnectionResetError, OSError):
                await self.close_callback()
                self.buffer.clear()
                return
            self.expected_seq += 1


def sync_socks5_handshake(proxy_host, proxy_port, target_url_str, username=None, password=None):
    parsed = urllib.parse.urlparse(target_url_str)
    target_host = parsed.hostname
    target_port = parsed.port if parsed.port else (443 if parsed.scheme == "wss" else 80)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect((proxy_host, proxy_port))

    if username and password:
        sock.sendall(b'\x05\x02\x00\x02')
    else:
        sock.sendall(b'\x05\x01\x00')

    response = sock.recv(2)
    if len(response) < 2 or response[0] != 0x05:
        sock.close()
        raise Exception("Upstream SOCKS5 handshake presentation rejected")

    if response[1] == 0x02:
        user_bytes = username.encode('utf-8')
        pass_bytes = password.encode('utf-8')
        auth_payload = struct.pack('!BB', 0x01, len(user_bytes)) + user_bytes + struct.pack('!B',
                                                                                            len(pass_bytes)) + pass_bytes
        sock.sendall(auth_payload)
        if (sock.recv(2))[1] != 0x00:
            sock.close()
            raise Exception("Upstream SOCKS5 authentication failed")

    host_bytes = target_host.encode('utf-8')
    sock.sendall(
        struct.pack('!BBBBB', 0x05, 0x01, 0x00, 0x03, len(host_bytes)) + host_bytes + struct.pack('!H', target_port))
    reply = sock.recv(4)
    if reply[1] != 0x00:
        sock.close()
        raise Exception(f"SOCKS Gateway rejected connection with code {hex(reply[1])}")

    atyp = reply[3]
    sock.recv(6 if atyp == 1 else (sock.recv(1)[0] + 2 if atyp == 3 else 18))
    sock.settimeout(None)
    return sock


async def monitor_in_flight_assembly():
    while True:
        await asyncio.sleep(0.1)
        now = asyncio.get_event_loop().time()
        timeouts = []

        for key, info in list(in_flight_chunks.items()):
            if now - info['dispatch_time'] > PACKET_FAILURE_TIMEOUT:
                timeouts.append(key)

        for key in timeouts:
            info = in_flight_chunks.pop(key, None)
            if not info: continue
            chunk = info['chunk']
            proxy_id = info['proxy_id']

            chunk.penalties[proxy_id] = now + PACKET_FAILURE_TIMEOUT
            print(f" [!] TIMEOUT: Chunk Seq {chunk.seq_id} on Link [{proxy_id}] expired. Re-queuing...")

            await pending_queue.put(chunk, front=True)
            safe_release_semaphore(proxy_id)


async def handle_local_client(reader, writer):
    connection_id = random.getrandbits(32)
    print(f" [+] Local Client Connected: Assigned Conn ID: {connection_id}")

    async def socket_write(data):
        writer.write(data)
        await writer.drain()

    async def socket_close():
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass
        active_connections.pop(connection_id, None)

    buffer = ReassemblyBuffer(connection_id, socket_write, socket_close)
    active_connections[connection_id] = buffer

    await pending_queue.put(Chunk(connection_id, 1, 0, b""))

    seq_id = 1
    try:
        while True:
            data = await reader.read(CHUNK_SIZE)
            if not data: break
            await pending_queue.put(Chunk(connection_id, 2, seq_id, data))
            seq_id += 1
    except Exception:
        pass
    finally:
        await pending_queue.put(Chunk(connection_id, 3, seq_id, b""))


async def tunnel_worker(proxy_ip, proxy_port, client_id, user, pwd):
    url = f"{CLOUDFLARE_BASE_URL}?id={client_id}"
    loop = asyncio.get_running_loop()

    worker_semaphores[client_id] = asyncio.BoundedSemaphore(CHUNKS_PER_WORKER)

    while True:
        try:
            print(f" [*] Link [{client_id}] dialing SOCKS proxy {proxy_ip}:{proxy_port}...")
            raw_sock = await loop.run_in_executor(None, sync_socks5_handshake, proxy_ip, proxy_port, url, user, pwd)

            async with websockets.connect(url, max_size=None, sock=raw_sock, open_timeout=60) as websocket:
                print(f" [v] PIPELINE ACTIVE: Link [{client_id}] established. Window capacity: {CHUNKS_PER_WORKER}")

                async def send_loop():
                    while True:
                        if worker_semaphores[client_id]._value == 0:
                            print(f" [Window Lock] Link [{client_id}] window exhausted! Awaiting ACKs...")

                        await worker_semaphores[client_id].acquire()
                        chunk = await pending_queue.get(client_id)

                        in_flight_chunks[(chunk.conn_id, chunk.seq_id)] = {
                            'chunk': chunk,
                            'dispatch_time': loop.time(),
                            'proxy_id': client_id
                        }

                        packet = struct.pack("!IBI", chunk.conn_id, chunk.cmd, chunk.seq_id) + chunk.payload
                        try:
                            await websocket.send(packet)
                            # Light logging to trace pipelining without drowning the terminal
                            if chunk.seq_id % 5 == 0 or chunk.cmd != 2:
                                print(
                                    f" [-> Outbound] Link [{client_id}] pushed Conn: {chunk.conn_id}, Seq: {chunk.seq_id} (Cmd: {chunk.cmd})")
                        except Exception:
                            in_flight_chunks.pop((chunk.conn_id, chunk.seq_id), None)
                            await pending_queue.put(chunk, front=True)
                            safe_release_semaphore(client_id)
                            raise

                async def recv_loop():
                    async_loop = asyncio.get_event_loop()
                    async for message in websocket:
                        if len(message) < 9: continue
                        conn_id, cmd, seq_id = struct.unpack("!IBI", message[:9])
                        payload = message[9:]

                        if cmd == 5:  # Delivery verified by backend
                            info = in_flight_chunks.pop((conn_id, seq_id), None)
                            if info and info['proxy_id'] == client_id:
                                safe_release_semaphore(client_id)
                                if seq_id % 5 == 0:
                                    print(
                                        f" [ACK Received] Link [{client_id}] verified delivery for Seq: {seq_id}. Window slot released.")
                        elif cmd in [2, 3]:
                            await websocket.send(struct.pack("!IBI", conn_id, 5, seq_id))
                            buf = active_connections.get(conn_id)
                            if buf:
                                async_loop.create_task(buf.add_chunk(cmd, seq_id, payload))

                await asyncio.gather(send_loop(), recv_loop())
        except Exception as e:
            print(f" [!] Link [{client_id}] error: {e}. Reconnecting in 2s...")
            for _ in range(CHUNKS_PER_WORKER):
                safe_release_semaphore(client_id)
            await asyncio.sleep(2)


async def main():
    print("======================================================================")
    print("  LAUNCHING HIGH-SPEED SLIDING-WINDOW MULTIPLEX FRONTEND")
    print("======================================================================")
    asyncio.create_task(monitor_in_flight_assembly())
    workers = [tunnel_worker(ip, pt, cid, u, p) for ip, pt, cid, u, p in UPSTREAM_PROXIES]
    server = await asyncio.start_server(handle_local_client, LOCAL_LISTENER_HOST, LOCAL_LISTENER_PORT)
    await asyncio.gather(server.serve_forever(), *workers)


if __name__ == "__main__":
    asyncio.run(main())