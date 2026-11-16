"""Kymotrace compact binary protocol v6.2 data frames (KymoCore PROTOCOL.md)."""
import math
import struct

SYNC = b'\xa5\x5a'


def crc8(data: bytes) -> int:
    """CRC-8/ATM: polynomial 0x07, init 0, no reflection, no xor-out."""
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = ((crc << 1) ^ 0x07) & 0xFF if crc & 0x80 else (crc << 1) & 0xFF
    return crc


def frame(id: int, y: float, x: float = None, z: float = None, t: int = None, *,
          us: bool = False, crc: bool = False) -> bytes:
    """One data frame. ``t`` counts ms (or µs with ``us``) since the sender started."""
    if type(id) is not int or not 0 <= id <= 255:
        raise ValueError(f'channel id {id} outside 0..255')
    if y is None:
        raise ValueError('y is required')
    if t is not None and (type(t) is not int or t < 0):
        raise ValueError('timestamp must be a non-negative integer')
    values = [v for v in (x, y, z) if v is not None]
    if not all(math.isfinite(v) for v in values):
        raise ValueError('NaN and infinity cannot be sent')
    if us and t is None:
        raise ValueError('microseconds need a timestamp')
    descriptor = (0x40 | (0x08 if x is not None else 0) | (0x04 if z is not None else 0)
                  | (0x02 if t is not None else 0) | (0x10 if us else 0) | (0 if crc else 0x01))
    try:
        body = bytes([descriptor, id]) + struct.pack(f'<{len(values)}f', *values)
    except (OverflowError, struct.error) as exc:
        raise ValueError('values must fit finite float32') from exc
    if t is not None:
        body += struct.pack('<I', t & 0xFFFFFFFF)  # wraps; KymoStudio continues the axis
    return SYNC + body + bytes([crc8(body) if crc else 0])


def control(payload: bytes) -> bytes:
    """Encode a CRC-protected control message (operation included in payload)."""
    if not 1 <= len(payload) <= 64:
        raise ValueError('control payload must contain 1..64 bytes')
    body = bytes((0x60, len(payload))) + bytes(payload)
    return SYNC + body + bytes((crc8(body),))


class StreamDecoder:
    """Incrementally recover DATA/control frames, retaining at most 68 bytes.

    Invalid descriptors, lengths, CRCs and nonfinite DATA are discarded. A
    partial frame can be explicitly cleared at a connection boundary.
    """

    def __init__(self):
        self._buffer = bytearray()
        self.invalid = 0

    def reset(self):
        self._buffer.clear()

    def feed(self, data: bytes) -> list[bytes]:
        frames = []
        for byte in data:
            self._buffer.append(byte)
            while self._buffer:
                if self._buffer[0] != 0xA5 or (len(self._buffer) > 1 and self._buffer[1] != 0x5A):
                    del self._buffer[0]
                    continue
                if len(self._buffer) < 4:
                    break
                descriptor = self._buffer[2]
                if descriptor == 0x60:
                    size = 5 + self._buffer[3] if 1 <= self._buffer[3] <= 64 else 0
                elif descriptor & 0xE0 == 0x40 and not (descriptor & 0x10 and not descriptor & 2):
                    size = 9 + 4 * sum(bool(descriptor & bit) for bit in (8, 4, 2))
                else:
                    size = 0
                if not size:
                    self.invalid += 1
                    del self._buffer[0]
                    continue
                if len(self._buffer) < size:
                    break
                candidate = bytes(self._buffer[:size])
                valid = (descriptor != 0x60 and descriptor & 1) or crc8(candidate[2:-1]) == candidate[-1]
                if valid and descriptor != 0x60:
                    count = 1 + bool(descriptor & 8) + bool(descriptor & 4)
                    valid = all(math.isfinite(v) for v in struct.unpack_from(f'<{count}f', candidate, 4))
                if valid:
                    frames.append(candidate)
                    del self._buffer[:size]
                else:
                    self.invalid += 1
                    del self._buffer[0]
        return frames
