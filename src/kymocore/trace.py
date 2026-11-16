"""Bounded Python instrumentation emitting the Embedded KymoTrace v2 contract.

Hooks only record events; Core.main performs all transport work. Python threads
and async tasks need distinct context IDs. OS scheduling/ISR completeness is
never inferred from Python execution.
"""
from __future__ import annotations

import binascii
import hashlib
import inspect
import json
import secrets
import struct
import threading
import time
import zlib
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps

from .features import text_bytes, uint


def envelope(kind, session_id, sequence, payload):
    """Encode a wire-v2 envelope, independently of native struct layout."""
    uint(kind, 8, 'kind')
    uint(session_id, 64, 'session ID')
    uint(sequence, 32, 'packet sequence')
    if len(payload) > 216:
        raise ValueError('trace payload exceeds 216 bytes')
    header = struct.pack('<BBHHQI', 0x80, kind, len(payload), 0, session_id, sequence)
    header += struct.pack('<H', binascii.crc_hqx(header, 0xffff))
    body = header + payload
    return b'\xa5\x5a' + body + struct.pack('<H', binascii.crc_hqx(body, 0xffff))


@dataclass
class _Context:
    name: str
    kind: str
    stack: list = field(default_factory=list)
    depth: int = 0
    sequence: int = 0
    invalid: bool = False


class Trace:
    def __init__(self, *, device_id='python', build_id='python', session_id=None,
                 core_count=1, capacity=64, max_depth=32, mtu=240,
                 metadata_period_ms=5000, health_period_ms=1000,
                 clock=time.monotonic_ns, frequency_hz=1_000_000_000):
        if not isinstance(device_id, str) or not device_id or not isinstance(build_id, str) or not build_id:
            raise ValueError('device_id and build_id must be nonempty strings')
        if type(core_count) is not int or not 1 <= core_count <= 2:
            raise ValueError('core_count must be 1 or 2')
        if type(capacity) is not int or not 16 <= capacity <= 4096 or capacity & (capacity - 1):
            raise ValueError('capacity must be a power of two in 16..4096')
        if type(max_depth) is not int or not 1 <= max_depth <= 255:
            raise ValueError('max_depth must be 1..255')
        if type(mtu) is not int or not 64 <= mtu <= 240:
            raise ValueError('trace MTU must be 64..240')
        uint(frequency_hz, 64, 'frequency')
        if not frequency_hz or not callable(clock):
            raise ValueError('trace requires a clock and positive frequency')
        for value in (metadata_period_ms, health_period_ms):
            if type(value) is not int or value < 0:
                raise ValueError('trace periods must be non-negative milliseconds')
        self.session_id = secrets.randbits(64) if session_id is None else uint(session_id, 64, 'session ID')
        self.device_id, self.build_id = device_id, build_id
        self.core_count, self.capacity, self.max_depth, self.mtu = core_count, capacity, max_depth, mtu
        self.metadata_period_ms, self.health_period_ms = metadata_period_ms, health_period_ms
        self._clock, self.frequency_hz = clock, frequency_hz
        self._origin = clock()
        self._last_ticks = 0
        self._lock = threading.RLock()
        self._contexts = {}
        self._symbols = {}
        self._rings = [deque() for _ in range(core_count)]
        self._seq = [0] * core_count
        self._drops = [0] * core_count
        self._errors = [0] * core_count
        self._high = [0] * core_count
        self._packet = 0
        self._transfer = 0
        self._admin = deque()
        self._refresh = False
        self._prefer_events = False
        self._initial_metadata = True
        self._started = self._stopping = self.ended = False
        self._end_queued = False
        self._bound = False
        self._metadata_due = self._health_due = 0

    def symbol(self, id, name, kind='function'):
        with self._lock:
            uint(id, 16, 'symbol ID')
            text_bytes(name, 128, 'symbol name')
            if self._started or id == 0 or not name or id in self._symbols or kind not in ('function', 'scope', 'marker'):
                raise ValueError('invalid, duplicate or late symbol registration')
            self._symbols[id] = {'id': id, 'name': name, 'kind': kind}
        return id

    def context(self, id, name, kind='main'):
        with self._lock:
            uint(id, 32, 'context ID')
            text_bytes(name, 128, 'context name')
            if self._started or not 1 <= id <= 0xfffffffe or not name or id in self._contexts or len(self._contexts) >= 256 or kind not in ('main', 'task', 'isr'):
                raise ValueError('invalid, duplicate or late context registration')
            self._contexts[id] = _Context(name, kind)
        return id

    @staticmethod
    def _json(value):
        encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
        if len(encoded) > 65532:
            raise ValueError('trace administrative record exceeds 64 KiB')
        return encoded

    def _ticks(self):
        ticks = self._clock() - self._origin
        uint(ticks, 64, 'ticks')
        if ticks < self._last_ticks:
            raise ValueError('trace clock moved backwards')
        self._last_ticks = ticks
        return ticks

    def start(self):
        with self._lock:
            if self._started:
                raise RuntimeError('Trace sessions cannot be restarted')
            self._dictionary = self._json({'schema_version': 1, 'generation': 1,
                'symbols': list(self._symbols.values()),
                'contexts': [{'id': id, 'name': c.name, 'kind': c.kind} for id, c in self._contexts.items()]})
            self._metadata = self._json({'schema_version': 1, 'wire_version': 2, 'event_schema_version': 1,
                'device_id': self.device_id, 'build_id': self.build_id, 'core_count': self.core_count,
                'dictionary_hash': hashlib.sha256(self._dictionary).hexdigest(),
                'clock': {'frequency_hz': str(self.frequency_hz), 'clock_domain_id': 0,
                          'resolution_ticks': 1, 'domain': 'python-monotonic', 'smp_synchronized': True},
                'features': {'scopes': True, 'markers': True, 'flow': True, 'scheduler': True, 'isr': True},
                'port_capabilities': {'scheduler_complete': False, 'isr_complete': False,
                                      'core_state_complete': False, 'capabilities_verified': False}})
            self._started = True
            self._queue_metadata()

    def _fragments(self, kind, body):
        self._transfer = (self._transfer + 1) & 0xffffffff
        transfer = self._transfer
        record = body + struct.pack('<I', zlib.crc32(body) & 0xffffffff)
        for offset in range(0, len(record), self.mtu - 42):
            yield kind, struct.pack('<BBIIII', 1, kind, 1, transfer, len(record), offset) + record[offset:offset + self.mtu - 42]

    def _queue_metadata(self):
        self._admin.extend((iter(self._fragments(1, self._metadata)), iter(self._fragments(2, self._dictionary))))
        self._metadata_due = self._ticks() + self.metadata_period_ms * self.frequency_hz // 1000
        self._refresh = False

    def request_metadata(self):
        with self._lock:
            self._refresh = True

    def _emit(self, context, symbol, event, core, flags=0, arg=0):
        if not self._started or self._stopping:
            return False
        uint(core, 8, 'core')
        uint(arg, 32, 'argument')
        if core >= self.core_count:
            raise ValueError('core outside configured range')
        state = self._contexts.get(context)
        if context and state is None:
            raise ValueError('unknown trace context')
        if symbol and symbol not in self._symbols:
            raise ValueError('unknown trace symbol')
        ticks = self._ticks()
        context_seq = state.sequence if state else 0
        depth = min(state.depth, 65535) if state else 0
        if state and state.invalid:
            flags |= 2
        record = struct.pack('<QIIIHHBBHI', ticks, self._seq[core], context, context_seq, symbol, depth, event, core, flags, arg)
        self._seq[core] = (self._seq[core] + 1) & 0xffffffff
        if state:
            state.sequence = (state.sequence + 1) & 0xffffffff
        ring = self._rings[core]
        accepted = len(ring) < self.capacity
        if accepted:
            ring.append((ticks, record))
            self._high[core] = max(self._high[core], len(ring))
        else:
            self._drops[core] = (self._drops[core] + 1) & 0xffffffff
        return accepted

    def _scope_state(self, context, symbol, core):
        if context not in self._contexts or symbol not in self._symbols or type(core) is not int or not 0 <= core < self.core_count:
            raise ValueError('unknown context, symbol or core')
        return self._contexts[context]

    def enter(self, context, symbol, *, core=0, stack_pointer=None):
        with self._lock:
            if not self._started or self._stopping:
                return False
            state = self._scope_state(context, symbol, core)
            if stack_pointer is not None:
                uint(stack_pointer, 32, 'stack pointer')
            root = state.depth == 0
            state.depth += 1
            if state.depth <= self.max_depth:
                state.stack.append(symbol)
            else:
                state.invalid = True
                self._errors[core] += 1
            return self._emit(context, symbol, 1, core, int(root) | (16 if stack_pointer is not None else 0), stack_pointer or 0)

    def exit(self, context, symbol, *, core=0, stack_pointer=None):
        with self._lock:
            if not self._started or self._stopping:
                return False
            state = self._scope_state(context, symbol, core)
            if stack_pointer is not None:
                uint(stack_pointer, 32, 'stack pointer')
            if state.depth == 0 or (state.depth <= self.max_depth and state.stack[-1] != symbol):
                state.invalid = True
                self._errors[core] += 1
            accepted = self._emit(context, symbol, 2, core, 16 if stack_pointer is not None else 0, stack_pointer or 0)
            if 0 < state.depth <= self.max_depth:
                state.stack.pop()
            state.depth = max(0, state.depth - 1)
            return accepted

    def mark(self, context, symbol, value=0, *, core=0):
        with self._lock:
            return self._emit(context, symbol, 3, core, 4, value)

    def reset(self, context, generation=0, *, core=0):
        with self._lock:
            state = self._contexts[context]
            if state.depth:
                raise RuntimeError('unwind scopes before resetting a context')
            state.invalid = False
            return self._emit(context, 0, 6, core, 0, generation)

    def task_create(self, context, generation=0, *, core=0):
        with self._lock:
            return self._emit(context, 0, 0x20, core, 0, generation)

    def task_end(self, context, generation=0, *, core=0):
        with self._lock:
            return self._emit(context, 0, 0x21, core, 0, generation)

    def task_switch(self, outgoing, incoming, *, core=0):
        with self._lock:
            if incoming and incoming not in self._contexts:
                raise ValueError('unknown incoming context')
            return self._emit(outgoing, 0, 0x22, core, 0, incoming)

    def core_state(self, context, state, *, core=0, recovery=False):
        with self._lock:
            if state not in (0, 1, 2):
                raise ValueError('core state must be unknown=0, running=1 or idle=2')
            return self._emit(context, 0, 0x23, core, 8 if recovery else 0, state)

    def flow_send(self, context, correlation, symbol=0, *, core=0):
        with self._lock:
            return self._emit(context, symbol, 0x30, core, 4, correlation)

    def flow_receive(self, context, correlation, symbol=0, *, core=0):
        with self._lock:
            return self._emit(context, symbol, 0x31, core, 4, correlation)

    def isr_enter(self, context, interrupted=0, *, core=0):
        with self._lock:
            return self._emit(context, 0, 4, core, 4, interrupted)

    def isr_exit(self, context, restored=0, *, core=0):
        with self._lock:
            return self._emit(context, 0, 5, core, 4, restored)

    @contextmanager
    def scope(self, context, symbol, *, core=0):
        self.enter(context, symbol, core=core)
        try:
            yield
        finally:
            self.exit(context, symbol, core=core)

    def instrument(self, context, symbol, *, core=0):
        """Decorator for sync/async functions; assign one context per concurrent task."""
        def decorate(function):
            if inspect.iscoroutinefunction(function):
                @wraps(function)
                async def async_call(*args, **kwargs):
                    with self.scope(context, symbol, core=core):
                        return await function(*args, **kwargs)
                return async_call
            @wraps(function)
            def call(*args, **kwargs):
                with self.scope(context, symbol, core=core):
                    return function(*args, **kwargs)
            return call
        return decorate

    @property
    def diagnostics(self):
        with self._lock:
            return {'queued': [len(r) for r in self._rings], 'drops': self._drops[:],
                    'errors': self._errors[:], 'high_water': self._high[:]}

    def stop(self):
        with self._lock:
            if any(c.depth for c in self._contexts.values()):
                raise RuntimeError('finish all trace scopes before stopping')
            self._stopping = True

    def _settlement(self, end=False):
        ticks = self._ticks()
        body = struct.pack('<BBH', 1, self.core_count, 0)
        for core in range(self.core_count):
            if end:
                body += struct.pack('<BBBBIIIQ', core, 0, 0, 0, self._seq[core], self._seq[core], self._drops[core], ticks)
            else:
                body += struct.pack('<BBHHHIIIIQ', core, 1, 0, self._high[core], 0,
                                    self._seq[core], self._seq[core], self._drops[core], self._errors[core], ticks)
        return body

    def wrap(self, kind, payload):
        result = envelope(kind, self.session_id, self._packet, payload)
        self._packet = (self._packet + 1) & 0xffffffff
        return result

    def next_frame(self):
        """One frame per call. Call only after the previous frame was accepted."""
        with self._lock:
            if not self._started or self.ended:
                return None
            now = self._ticks()
            if not self._admin and not self._stopping and (self._refresh or (self.metadata_period_ms and now >= self._metadata_due)):
                self._queue_metadata()
            if not self._admin and not any(self._rings):
                if self._stopping:
                    self._admin.append(iter(self._fragments(6, self._settlement(end=True))))
                    self._end_queued = True
                elif self.health_period_ms and now >= self._health_due:
                    self._admin.append(iter(self._fragments(5, self._settlement())))
                    self._health_due = now + self.health_period_ms * self.frequency_hz // 1000
            # Initial metadata comes first; refresh/health frames share event turns.
            if self._admin and (self._initial_metadata or not self._prefer_events or not any(self._rings) or self._end_queued):
                try:
                    kind, payload = next(self._admin[0])
                    self._prefer_events = True
                    return self.wrap(kind, payload)
                except StopIteration:
                    self._admin.popleft()
                    if not self._admin:
                        self._initial_metadata = False
                    if self._stopping and not self._admin and not any(self._rings):
                        # Only the final end record sets completion (see below).
                        if self._end_queued:
                            self.ended = True
                            return None
                    return self.next_frame()
            records = []
            for _ in range(min(6, (self.mtu - 28) // 32)):
                available = [i for i, ring in enumerate(self._rings) if ring]
                if not available:
                    break
                core = min(available, key=lambda i: self._rings[i][0][0])
                records.append(self._rings[core].popleft()[1])
            if records:
                self._prefer_events = False
                return self.wrap(3, struct.pack('<BBH', 1, len(records), 0) + b''.join(records))
            return None
