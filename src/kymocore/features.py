"""Feature definitions for the KymoCore 8 compatible Python endpoint."""
from __future__ import annotations

import math
import hashlib
import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Callable
from pathlib import Path

from .protocol import control


def uint(value, bits, name):
    if type(value) is not int or not 0 <= value < 1 << bits:
        raise ValueError(f'{name} must be uint{bits}')
    return value


def text_bytes(value, maximum, name):
    if not isinstance(value, str) or '\0' in value:
        raise ValueError(f'{name} must be text without NUL')
    result = value.encode('utf-8')
    if len(result) > maximum:
        raise ValueError(f'{name} exceeds {maximum} UTF-8 bytes')
    return result


def float32(value):
    try:
        result = struct.unpack('<f', struct.pack('<f', value))[0]
    except (TypeError, ValueError, struct.error, OverflowError) as exc:
        raise ValueError('value must fit finite float32') from exc
    if not math.isfinite(result):
        raise ValueError('value must fit finite float32')
    return result


@dataclass(frozen=True)
class Sample:
    """One sampled point; X and Z are optional."""
    y: float
    x: float | None = None
    z: float | None = None


@dataclass(frozen=True)
class Plot:
    channel: int
    read: Callable[[], float | Sample]
    period_ms: int = 20

    def __post_init__(self):
        uint(self.channel, 8, 'channel')
        if not callable(self.read) or type(self.period_ms) is not int or not 1 <= self.period_ms <= 0x7FFFFFFF:
            raise ValueError('plot needs a reader and a positive period up to 2^31-1 ms')


class Parameter:
    """Writable float32 parameter, stored here or bound to getter/setter callbacks.

    Callback pairs run in Core.main's caller. The readback always comes from
    the getter, including when an out-of-range host write is rejected.
    """

    def __init__(self, channel, value=0., *, minimum, maximum, name='', unit='', get=None, set=None):
        self.channel = uint(channel, 8, 'channel')
        self.minimum, self.maximum = float32(minimum), float32(maximum)
        if self.minimum > self.maximum:
            raise ValueError('minimum exceeds maximum')
        self.name = text_bytes(name or f'parameter_{channel}', 24, 'name')
        self.unit = text_bytes(unit, 8, 'unit')
        if (get is None) != (set is None) or (get is not None and (not callable(get) or not callable(set))):
            raise ValueError('provide both getter and setter')
        self._get, self._set = get, set
        self._value = float32(value)

    @property
    def value(self):
        return float32(self._get() if self._get else self._value)

    @value.setter
    def value(self, value):
        value = float32(value)
        if not self.minimum <= value <= self.maximum:
            raise ValueError('parameter outside its allowed range')
        if self._set:
            self._set(value)
        else:
            self._value = value

    def description(self, index, count):
        return control(bytes((2, self.channel, index, count)) + struct.pack('<ff', self.minimum, self.maximum)
                       + bytes((len(self.name),)) + self.name + bytes((len(self.unit),)) + self.unit)


class WatchType(IntEnum):
    U8 = 1
    I8 = 2
    U16 = 3
    I16 = 4
    U32 = 5
    I32 = 6
    F32 = 7
    F64 = 8
    BOOL = 9


WATCH_FORMATS = {1: 'B', 2: 'b', 3: 'H', 4: 'h', 5: 'I', 6: 'i', 7: 'f', 8: 'd', 9: 'B'}


@dataclass(frozen=True)
class WatchVariable:
    """A named scalar getter, available in Studio via Watch.export_elf()."""
    name: str
    type: WatchType
    read: Callable[[], object]

    def __post_init__(self):
        text_bytes(self.name, 128, 'variable name')
        WatchType(self.type)
        if not self.name or not callable(self.read):
            raise ValueError('watch variable requires a name and getter')


@dataclass(frozen=True)
class WatchRange:
    """Explicit wire address range; read(address, width) must return exact bytes.

    Addresses are protocol addresses, never implicitly dereferenced in Python.
    A callback may map a byte buffer, a device or registered Python values.
    """
    start: int
    length: int
    read: Callable[[int, int], bytes]

    def __post_init__(self):
        uint(self.start, 32, 'range start')
        uint(self.length, 32, 'range length')
        if not self.length or self.start + self.length > 1 << 32 or not callable(self.read):
            raise ValueError('invalid watch range')

    @classmethod
    def variable(cls, address, type, get):
        """Map a Python getter to a stable, application-selected wire address."""
        fmt = WATCH_FORMATS[int(WatchType(type))]
        width = struct.calcsize(fmt)
        if address % width or not callable(get):
            raise ValueError('variable requires an aligned address and getter')

        def read(start, size):
            value = bool(get()) if type == WatchType.BOOL else get()
            packed = struct.pack('<' + fmt, value)
            return packed[start - address:start - address + size]
        return cls(address, width, read)


class Watch:
    def __init__(self, ranges, *, build_id=b'', slots=8, min_period_ms=5, channels=(200, 215)):
        self.ranges = tuple(ranges)
        self.build_id = bytes(build_id)
        if not 1 <= len(self.ranges) <= 4 or not all(isinstance(r, WatchRange) for r in self.ranges):
            raise ValueError('watch needs 1..4 explicit readable ranges')
        uint(slots, 8, 'slots')
        uint(min_period_ms, 16, 'minimum period')
        if not slots or not min_period_ms or len(self.build_id) > 20:
            raise ValueError('invalid watch limits/build ID')
        first, last = channels
        uint(first, 8, 'first channel')
        uint(last, 8, 'last channel')
        if first > last:
            raise ValueError('invalid watch channel interval')
        self.slots, self.min_period_ms, self.channels = slots, min_period_ms, (first, last)

    @classmethod
    def from_variables(cls, variables, *, base_address=0x1000, build_tag='1', **options):
        """Map named Python getters and prepare a matching Studio ELF symbol map.

        Addresses are stable eight-byte virtual slots in declaration order.
        The generated layout's SHA-256 prefix is the advertised build ID.
        """
        from .symbols import symbol_elf
        variables = tuple(variables)
        uint(base_address, 32, 'base address')
        text_bytes(build_tag, 128, 'build tag')
        if not 1 <= len(variables) <= 256 or not all(isinstance(v, WatchVariable) for v in variables):
            raise ValueError('provide 1..256 named WatchVariable definitions')
        if base_address % 8 or base_address + 8 * len(variables) > 1 << 32:
            raise ValueError('watch mapping must fit aligned uint32 addresses')
        if len({v.name for v in variables}) != len(variables):
            raise ValueError('watch variable names must be unique')
        if 'build_id' in options:
            raise ValueError('named-variable build ID comes from the exported layout')

        def read(address, width):
            index, offset = divmod(address - base_address, 8)
            variable = variables[index]
            value = variable.read()
            if variable.type == WatchType.BOOL:
                value = bool(value)
            raw = struct.pack('<' + WATCH_FORMATS[variable.type], value).ljust(8, b'\0')
            return raw[offset:offset + width]

        mapping = symbol_elf(variables, base_address, build_tag)
        watch = cls([WatchRange(base_address, len(variables) * 8, read)],
                    build_id=hashlib.sha256(mapping).digest()[:20], **options)
        watch._symbol_map = mapping
        return watch

    def export_elf(self, path):
        """Write the data-only symbol map for Studio's existing ELF selection UI."""
        if not hasattr(self, '_symbol_map'):
            raise ValueError('ELF export requires Watch.from_variables()')
        path = Path(path)
        path.write_bytes(self._symbol_map)
        return path

    def info(self):
        payload = struct.pack('<BBHB', 4, self.slots, self.min_period_ms, len(self.build_id)) + self.build_id
        payload += bytes((len(self.ranges),))
        for region in self.ranges:
            payload += struct.pack('<II', region.start, region.length)
        return control(payload)


class MemoryKind(IntEnum):
    STACK_PEAK = 1
    HEAP_FREE = 2
    HEAP_MIN_FREE = 3


@dataclass(frozen=True)
class Memory:
    """Measured byte count; the caller supplies actual stack/heap semantics."""
    channel: int
    kind: MemoryKind
    read: Callable[[], int]
    name: str = ''
    total: int = 0
    period_ms: int = 1000
    info_period_ms: int = 5000

    def __post_init__(self):
        uint(self.channel, 8, 'channel')
        MemoryKind(self.kind)
        uint(self.total, 32, 'total')
        text_bytes(self.name, 16, 'name')
        if not callable(self.read) or type(self.period_ms) is not int or not 1 <= self.period_ms <= 60000:
            raise ValueError('memory sampling period must be 1..60000 ms')
        if type(self.info_period_ms) is not int or not 0 <= self.info_period_ms <= 60000:
            raise ValueError('memory info period must be 0..60000 ms')

    def info(self):
        return control(struct.pack('<BBBI', 7, self.channel, self.kind, self.total) + self.name.encode('utf-8'))
