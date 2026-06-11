# BalancerWithVSCode

A multi-connection data bonding and chunking system for accelerating TCP requests over multiple connections. This project simulates how to send data across multiple concurrent connections to speed up data transfer by bonding or combining internet connections.

## Project Overview

**BalancerWithVSCode** is an educational project demonstrating:
- **Frontend**: A SOCKS5 proxy server that captures client requests
- **Backend**: A stub server ready to accept and process chunked payloads
- **Protocol**: A compact 9-byte header format with command flags for message framing
- **Multi-connection support**: Infrastructure for splitting data across multiple concurrent connections

## Architecture

### Frontend (`frontend.py`)
- Runs a SOCKS5 listener on `127.0.0.1:1080` (configurable)
- Accepts SOCKS5 CONNECT requests from clients
- Optional username/password authentication
- Prints captured payloads to console
- Configuration: `front.config.json`

### Backend (`backend.py`)
- Stub TCP server listening on `127.0.0.1:1080` (configurable)
- Ready for custom transport and data processing logic
- Configuration: `back.config.json`

### Protocol (`protocol.py`)
- Compact 9-byte `PayloadHeader` with command flags
- Supports 2 commands:
  - `COMMAND_COMMUNICATION` (0): For communication between frontend/backend
  - `COMMAND_TRANSMIT` (1): For data transmission
- 6 reserved bits for future extensions
- `RequestIdGenerator`: 4-byte request counter (wraps at 2^32)

### Configuration (`config.py`)
- Unified config loader that creates JSON files with defaults if missing
- Atomic writes to prevent corruption

## Setup

### Prerequisites
- Python 3.7+
- No external dependencies (uses only stdlib)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/BalancerWithVSCode.git
   cd BalancerWithVSCode
   ```

2. First run will auto-generate config files with defaults:
   ```bash
   python -c "import frontend, backend"
   ```

   This creates:
   - `front.config.json`: Frontend listener settings and auth
   - `back.config.json`: Backend settings

3. Edit config files as needed:
   - `front.config.json`: Set SOCKS5 auth credentials (optional)
   - `back.config.json`: Adjust chunk size or port if needed

**Note**: Config files are in `.gitignore` and not synced with git. Each student has their own local configuration.

## Running the Project

### Start the Frontend SOCKS5 Listener
```bash
python frontend.py
```
Output:
```
Frontend SOCKS5 listening on 127.0.0.1:1080
```

### Start the Backend Server
```bash
python backend.py
```
Output:
```
Backend listening on 127.0.0.1:1080
```

## Configuration Files

### `front.config.json`
```json
{
  "Local_Host": "127.0.0.1",
  "Local_Port": 1080,
  "MAX_CHUNK_SIZE": 32768,
  "socks_auth_user": "",
  "socks_auth_password": ""
}
```

- `socks_auth_user` / `socks_auth_password`: Set both to enable SOCKS5 authentication. Leave empty for no-auth mode.

### `back.config.json`
```json
{
  "Local_Host": "127.0.0.1",
  "Local_Port": 1080,
  "MAX_CHUNK_SIZE": 32768
}
```

## Protocol Details

### PayloadHeader (9 bytes)
| Field | Size | Type | Description |
|-------|------|------|-------------|
| flags | 1 byte | B | Command (bits 6-7) + reserved (bits 0-5) |
| chunk_index | 2 bytes | H | Which chunk (0-65535) |
| total_chunks | 2 bytes | H | Total chunks (0-65535) |
| payload_length | 4 bytes | I | Payload size in bytes |

### Commands
- `0 (COMMAND_COMMUNICATION)`: Control/handshake messages
- `1 (COMMAND_TRANSMIT)`: Data transmission

## Project Structure

```
BalancerWithVSCode/
├── frontend.py          # SOCKS5 listener
├── backend.py           # Backend stub
├── protocol.py          # Message framing and protocol
├── config.py            # Config loader utilities
├── README.md            # This file
├── .gitignore           # Git ignore rules
├── front.config.json    # (auto-generated, not in git)
├── back.config.json     # (auto-generated, not in git)
├── pending/             # (local, not in git)
└── resolved/            # (local, not in git)
```

## Development Notes

- **Config files are local**: Each student's `front.config.json` and `back.config.json` are not synced with git. Customize them locally as needed.
- **Pending/Resolved directories**: Currently unused; will be populated by future transport implementations.
- **Next steps**: Implement multi-connection chunking, TCP bonding, and custom transport methods.

## License

MIT or appropriate educational license.

## Contributing

This is an educational project. Follow along with the course/workshop for updates!
