import struct

import pytest


class Clock:
    def __init__(self):
        self.now = 1_000_000_000

    def __call__(self):
        return self.now

    def advance(self, ms):
        self.now += int(ms * 1_000_000)


class Sink:
    def __init__(self, limit=None):
        self.data = bytearray()
        self.rx = bytearray()
        self.limit = limit
        self.closed = False

    def write(self, data):
        accepted = len(data) if self.limit is None else min(self.limit, len(data))
        self.data.extend(data[:accepted])
        return accepted

    def read(self, size):
        result = bytes(self.rx[:size])
        del self.rx[:size]
        return result

    def close(self):
        self.closed = True


def split_frames(data):
    result = []
    offset = 0
    while offset < len(data):
        assert data[offset:offset + 2] == b'\xa5\x5a'
        descriptor = data[offset + 2]
        if descriptor == 0x80:
            size = 24 + struct.unpack_from('<H', data, offset + 4)[0]
        elif descriptor == 0x60:
            size = 5 + data[offset + 3]
        else:
            size = 9 + 4 * sum(bool(descriptor & bit) for bit in (2, 4, 8))
        packet = bytes(data[offset:offset + size])
        assert len(packet) == size
        result.append(packet)
        offset += size
    return result


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def sink():
    return Sink()
