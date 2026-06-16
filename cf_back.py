import asyncio
import struct
import socket
import collections
import websockets

# Configuration Variables
CHUNK_SIZE = 65536
CHUNKS_PER_WORKER = 15
PACKET_FAILURE_TIMEOUT = 15.0

CLOUDFLARE_BASE_URL = "wss://websocket-bridge-pipe.kolang-733.workers.dev/"
INTERNAL_SOCKS_PORT = 21080
CLIENT_POOLS = ["cl1", "cl2"]

in_flight_chunks = {}
active_connections = {}
live_websockets = {}
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
    def __init__(self, conn_id, loop_env):
        self.conn_id = conn_id
        self.expected_seq = 0
        self.buffer = {}
        self.writer = None
        self.loop = loop_env

    async def add_chunk(self, cmd, seq_id, payload):
        if seq_id < self.expected_seq: return
        self.buffer[seq_id] = (cmd, payload)

        while self.expected_seq in self.buffer:
            curr_cmd, curr_payload = self.buffer.pop(self.expected_seq)

            try:
                if curr_cmd == 1:
                    print(
                        f" [Target Connection] Trigger received: Spawning outbound internet proxy link for Conn: {self.conn_id}")
                    r, w = await asyncio.open_connection('127.0.0.1', INTERNAL_SOCKS_PORT)
                    self.writer = w
                    self.loop.create_task(self.stream_target_to_assembly(r))
                elif curr_cmd == 2 and self.writer:
                    self.writer.write(curr_payload)
                    await self.writer.drain()
                elif curr_cmd == 3:
                    print(f" [Target Connection] Teardown signal received for Conn: {self.conn_id}")
                    if self.writer:
                        self.writer.close()
                        await self.writer.wait_closed()
                    active_connections.pop(self.conn_id, None)
                    return
            except (ConnectionResetError, OSError):
                if self.writer:
                    try:
                        self.writer.close()
                    except:
                        pass
                active_connections.pop(self.conn_id, None)
                self.buffer.clear()
                return

            self.expected_seq += 1

    async def stream_target_to_assembly(self, reader):
        seq_id = 0
        try:
            while True:
                data = await reader.read(CHUNK_SIZE)
                if not data: break
                await pending_queue.put(Chunk(self.conn_id, 2, seq_id, data))
                seq_id += 1
        except Exception:
            pass
        finally:
            await pending_queue.put(Chunk(self.conn_id, 3, seq_id, b""))


async def socks5_handler(reader, writer):
    try:
        header = await reader.readexactly(2)
        await reader.readexactly(header[1])
        writer.write(b"\x05\x00")
        await writer.drain()
        req = await reader.readexactly(4)
        if req[3] == 1:
            addr = socket.inet_ntoa(await reader.readexactly(4))
        elif req[3] == 3:
            addr = (await reader.readexactly((await reader.readexactly(1))[0])).decode()
        else:
            return
        port = int.from_bytes(await reader.readexactly(2), 'big')
        rm_r, rm_w = await asyncio.open_connection(addr, port)
        writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
        await writer.drain()

        async def relay(src, dst):
            try:
                while True:
                    d = await src.read(65536)
                    if not d: break
                    dst.write(d)
                    await dst.drain()
            except:
                pass
            finally:
                try:
                    dst.close()
                except:
                    pass

        await asyncio.gather(relay(reader, rm_w), relay(rm_r, writer))
    except:
        try:
            writer.close()
        except:
            pass


async def monitor_cloud_in_flight():
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
            pid = info['proxy_id']
            print(f" [!] TIMEOUT: Return Chunk Seq {chunk.seq_id} on Hub [{pid}] expired. Re-queuing...")
            chunk.penalties[pid] = now + PACKET_FAILURE_TIMEOUT
            await pending_queue.put(chunk, front=True)
            safe_release_semaphore(pid)


async def listen_to_client_pipe(client_id):
    url = f"{CLOUDFLARE_BASE_URL}?id={client_id}"
    loop = asyncio.get_running_loop()

    worker_semaphores[client_id] = asyncio.BoundedSemaphore(CHUNKS_PER_WORKER)

    while True:
        try:
            async with websockets.connect(url, max_size=None, open_timeout=60) as websocket:
                print(f" [v] BACKBONE ONLINE: Listener [{client_id}] connected. Window capacity: {CHUNKS_PER_WORKER}")
                live_websockets[client_id] = websocket

                async def send_loop():
                    while True:
                        if worker_semaphores[client_id]._value == 0:
                            print(f" [Window Lock] Hub [{client_id}] window exhausted! Awaiting return ACKs...")

                        await worker_semaphores[client_id].acquire()
                        chunk = await pending_queue.get(client_id)

                        in_flight_chunks[(chunk.conn_id, chunk.seq_id)] = {
                            'chunk': chunk, 'dispatch_time': loop.time(), 'proxy_id': client_id
                        }
                        pkt = struct.pack("!IBI", chunk.conn_id, chunk.cmd, chunk.seq_id) + chunk.payload
                        try:
                            await websocket.send(pkt)
                            if chunk.seq_id % 5 == 0 or chunk.cmd != 2:
                                print(
                                    f" [<- Return Path] Hub [{client_id}] sent response data for Conn: {chunk.conn_id}, Seq: {chunk.seq_id}")
                        except Exception:
                            in_flight_chunks.pop((chunk.conn_id, chunk.seq_id), None)
                            await pending_queue.put(chunk, front=True)
                            safe_release_semaphore(client_id)
                            raise

                async def recv_loop():
                    async for message in websocket:
                        if len(message) < 9: continue
                        conn_id, cmd, seq_id = struct.unpack("!IBI", message[:9])
                        payload = message[9:]

                        if cmd == 5:  # Delivery verified by client frontend
                            info = in_flight_chunks.pop((conn_id, seq_id), None)
                            if info and info['proxy_id'] == client_id:
                                safe_release_semaphore(client_id)
                                if seq_id % 5 == 0:
                                    print(
                                        f" [ACK Received] Hub [{client_id}] verified downstream receipt for Response Seq: {seq_id}")
                        elif cmd in [1, 2, 3]:
                            await websocket.send(struct.pack("!IBI", conn_id, 5, seq_id))
                            if conn_id not in active_connections:
                                active_connections[conn_id] = ReassemblyBuffer(conn_id, loop)
                            loop.create_task(active_connections[conn_id].add_chunk(cmd, seq_id, payload))

                await asyncio.gather(send_loop(), recv_loop())
        except Exception as e:
            print(f" [!] Hub [{client_id}] pipeline connection dropped: {e}. Reconnecting in 5s...")
            live_websockets.pop(client_id, None)
            for _ in range(CHUNKS_PER_WORKER):
                safe_release_semaphore(client_id)
            await asyncio.sleep(5)


async def main():
    print("======================================================================")
    print("  LAUNCHING HIGH-SPEED SLIDING-WINDOW CLOUD EXIT HUB")
    print("======================================================================")
    asyncio.create_task(monitor_cloud_in_flight())
    socks_server = await asyncio.start_server(socks5_handler, '127.0.0.1', INTERNAL_SOCKS_PORT)
    workers = [listen_to_client_pipe(cid) for cid in CLIENT_POOLS]
    await asyncio.gather(socks_server.serve_forever(), *workers)


if __name__ == "__main__":
    asyncio.run(main())