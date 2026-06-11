"""Shared protocol helpers for BalancerWithVSCode.

This module defines a compact payload header format with command flags.
"""

import struct
from dataclasses import dataclass
from typing import Tuple

HEADER_FORMAT = "!BHHI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

COMMAND_COMMUNICATION = 0
COMMAND_TRANSMIT = 1


def make_flags(command: int, reserved: int = 0) -> int:
    return ((command & 0x03) << 6) | (reserved & 0x3F)


def parse_flags(flags_byte: int) -> Tuple[int, int]:
    return (flags_byte >> 6) & 0x03, flags_byte & 0x3F


@dataclass
class PayloadHeader:
    flags: int
    chunk_index: int
    total_chunks: int
    payload_length: int

    def to_bytes(self) -> bytes:
        return struct.pack(
            HEADER_FORMAT,
            self.flags,
            self.chunk_index,
            self.total_chunks,
            self.payload_length,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "PayloadHeader":
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Payload header must be {HEADER_SIZE} bytes")

        flags, chunk_index, total_chunks, payload_length = struct.unpack(
            HEADER_FORMAT, data[:HEADER_SIZE]
        )
        return cls(
            flags=flags,
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            payload_length=payload_length,
        )


class RequestIdGenerator:
    """Simple request ID counter (4-byte, wraps on overflow)."""

    def __init__(self, start: int = 0):
        self.current = start

    def next_id(self) -> int:
        request_id = self.current & 0xFFFFFFFF  # Keep it 32-bit
        self.current = (self.current + 1) & 0xFFFFFFFF
        return request_id


def pack_chunk(header: PayloadHeader, payload: bytes) -> bytes:
    return header.to_bytes() + payload


def unpack_chunk(data: bytes) -> Tuple[PayloadHeader, bytes]:
    header = PayloadHeader.from_bytes(data[:HEADER_SIZE])
    payload = data[HEADER_SIZE : HEADER_SIZE + header.payload_length]
    if len(payload) != header.payload_length:
        raise ValueError("Payload length does not match header metadata")
    return header, payload
