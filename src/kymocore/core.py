"""One cyclic service for Plot, Parameters, Watch, Memory and Trace."""
from __future__ import annotations

import struct
import threading
import time
from collections import deque

from . import transport
from .features import Memory, Parameter, Plot, Sample, Watch, WATCH_FORMATS, float32, uint
from .protocol import StreamDecoder, control, frame


class Core:
    """KymoCore 8 feature lifecycle with Python-owned, bounded buffers.

    Construct with feature definitions, then call main() regularly from one
    owner thread. Trace hooks may run in other threads. A sink implements
    write(bytes) -> accepted count (None means all), close(), and optionally
    read(budget), read_packets(budget), service() and pending. Callbacks must
    not block. There is no hidden application thread.
    """

    def __init__(self, sink, *, plots=(), parameters=(), watch=None, memory=(), trace=None,
                 timestamps='us', crc=False, rx_budget=4096, frames_per_main=16,
                 queue_capacity=256, clock=time.monotonic_ns):
        if timestamps not in ('us', 'ms', None):
            raise ValueError('timestamps must be us, ms or None')
        for value in (rx_budget, frames_per_main, queue_capacity):
            if type(value) is not int or value < 1:
                raise ValueError('budgets and queue capacity must be positive integers')
        self.sink = sink
        self.plots, self.parameters, self.memory = tuple(plots), tuple(parameters), tuple(memory)
        self.watch, self.trace = watch, trace
        if len(self.parameters) > 255:
            raise ValueError('parameter discovery count must fit uint8')
        for entries, expected in ((self.plots, Plot), (self.parameters, Parameter), (self.memory, Memory)):
            if not all(isinstance(entry, expected) for entry in entries):
                raise ValueError('invalid feature definition')
        if watch is not None and not isinstance(watch, Watch):
            raise ValueError('watch must be a Watch definition')
        self._reserved = set()
        for entry in (*self.plots, *self.parameters, *self.memory):
            if entry.channel in self._reserved:
                raise ValueError(f'channel {entry.channel} is already reserved')
            self._reserved.add(entry.channel)
        if watch is not None:
            channels = set(range(watch.channels[0], watch.channels[1] + 1))
            if self._reserved & channels:
                raise ValueError('watch interval overlaps another feature')
            self._reserved.update(channels)
        self._parameters = {p.channel: p for p in self.parameters}
        self._clock, self._origin, self._now = clock, clock(), 0
        self._timestamps, self._crc = timestamps, crc
        self._rx_budget, self._frames_per_main = rx_budget, frames_per_main
        self._queue_capacity = queue_capacity
        self._manual = deque()
        self._decoders = {}
        self._descriptions, self._readbacks, self._acks = {}, {}, {}
        self._watch_info = False
        self._slots = [None] * (watch.slots if watch else 0)
        self._plot_due = [0] * len(self.plots)
        self._memory_due = [0] * len(self.memory)
        self._memory_info_due = [0] * len(self.memory)
        self._memory_sample_turn = [False] * len(self.memory)
        self._plot_index = self._memory_index = self._watch_index = self._lane = self._control_lane = 0
        self._pending = b''
        self._offset = 0
        self._main_lock = threading.Lock()
        self._connections = getattr(sink, 'connections', 0)
        self.state = 'configuring'
        self.error = None
        self.diagnostics = {'frames_sent': 0, 'bytes_sent': 0, 'invalid_rx': 0,
                            'writes_accepted': 0, 'writes_rejected': 0, 'sample_errors': 0}
        if trace is not None:
            if trace._bound or trace._started:
                raise ValueError('Trace already belongs to an endpoint/session')
            trace._bound = True

    @classmethod
    def udp(cls, host, port, *, bind=None, **features):
        return cls._with_sink(transport.Udp(host, port, bind=bind), features)

    @classmethod
    def tcp_server(cls, port, host='0.0.0.0', *, max_clients=8, max_pending=65536, **features):
        return cls._with_sink(transport.TcpServer(port, host, max_clients=max_clients, max_pending=max_pending), features)

    @classmethod
    def serial(cls, port, baudrate=115200, **features):
        return cls._with_sink(transport.Serial(port, baudrate, write_timeout=0), features)

    @classmethod
    def mqtt(cls, host, topic, *, rx_topic, port=1883, qos=0, username=None, password=None,
             client_id='', max_pending=65536, **features):
        sink = transport.Mqtt(host, topic, port, rx_topic=rx_topic, qos=qos, username=username,
                              password=password, client_id=client_id, max_pending=max_pending)
        return cls._with_sink(sink, features)

    @classmethod
    def _with_sink(cls, sink, features):
        try:
            return cls(sink, **features)
        except Exception:
            sink.close()
            raise

    def _start(self):
        if self.state == 'configuring':
            if self.trace:
                self.trace.start()
            self.state = 'running'
        if self.state == 'fault':
            raise RuntimeError('Core is faulted; create a new endpoint') from self.error

    def _update_clock(self):
        now = self._clock() - self._origin
        if type(now) is not int or now < self._now:
            raise ValueError('Core requires a monotonic integer nanosecond clock')
        self._now = now

    def _data(self, channel, value):
        point = value if isinstance(value, Sample) else Sample(value)
        timestamp = None if self._timestamps is None else self._now // (1000 if self._timestamps == 'us' else 1_000_000)
        return frame(channel, point.y, point.x, point.z, timestamp, us=self._timestamps == 'us', crc=self._crc)

    def send(self, channel, y, *, x=None, z=None):
        """Queue an immediate point on an unreserved channel; raise when full."""
        self._start()
        if self.state != 'running':
            raise RuntimeError('Core is stopping/stopped')
        if channel in self._reserved:
            raise ValueError('channel belongs to a registered feature')
        if len(self._manual) >= self._queue_capacity:
            raise BufferError('measurement queue is full')
        self._update_clock()
        self._manual.append(self._data(channel, Sample(y, x, z)))

    def feed(self, data, *, peer='external'):
        """Feed a bounded external byte stream; peer=None is one complete packet."""
        self._start()
        if self.state != 'running':
            return
        if len(data) > self._rx_budget:
            raise ValueError('split external input into chunks no larger than rx_budget')
        if peer is None:
            decoder = StreamDecoder()
        else:
            if peer not in self._decoders and len(self._decoders) >= 16:
                raise BufferError('too many independent receive streams')
            decoder = self._decoders.setdefault(peer, StreamDecoder())
        invalid_before = decoder.invalid
        for packet in decoder.feed(data):
            self._request(packet)
        self.diagnostics['invalid_rx'] += decoder.invalid - invalid_before

    def _request(self, packet):
        descriptor = packet[2]
        if descriptor == 0x40:
            parameter = self._parameters.get(packet[3])
            if parameter is None:
                self.diagnostics['writes_rejected'] += 1
                return
            value = struct.unpack_from('<f', packet, 4)[0]
            if parameter.minimum <= value <= parameter.maximum:
                parameter.value = value
                self.diagnostics['writes_accepted'] += 1
            else:
                self.diagnostics['writes_rejected'] += 1
            self._readbacks[parameter.channel] = parameter
        elif descriptor == 0x60:
            payload = packet[4:-1]
            if payload == b'\x01':
                self._descriptions = {p.channel: i for i, p in enumerate(self.parameters)}
                self._readbacks.update(self._parameters)
            elif payload == b'\x03' and self.watch:
                self._slots = [None] * self.watch.slots
                self._acks.clear()
                self._watch_info = True
            elif payload[0] == 5 and len(payload) == 10 and self.watch:
                self._watch_set(payload)
            else:
                self.diagnostics['invalid_rx'] += 1
        else:
            self.diagnostics['writes_rejected'] += 1

    def feed_compact(self, data):
        """CAN-style ID+float32 write; framing/CRC belong to the external link."""
        if len(data) != 5:
            raise ValueError('compact parameter write must contain exactly five bytes')
        value = float32(struct.unpack_from('<f', data, 1)[0])
        self.feed(frame(data[0], value, crc=True))

    def _watch_set(self, payload):
        _, slot, channel, address, type_id, period = struct.unpack('<BBBI BH'.replace(' ', ''), payload)
        status, region = 0, None
        if slot >= len(self._slots):
            status = 1
        else:
            self._slots[slot] = None
            if period:
                fmt = WATCH_FORMATS.get(type_id)
                if not self.watch.channels[0] <= channel <= self.watch.channels[1]:
                    status = 2
                elif fmt is None:
                    status = 4
                elif period < self.watch.min_period_ms:
                    status = 5
                else:
                    width = struct.calcsize(fmt)
                    if address % width:
                        status = 3
                    else:
                        region = next((r for r in self.watch.ranges if r.start <= address and address + width <= r.start + r.length), None)
                        if region is None:
                            status = 2
                if status == 0:
                    self._slots[slot] = [channel, address, type_id, period, 0, region]
        self._acks[slot] = status

    def request_memory_info(self):
        self._memory_info_due = [0] * len(self.memory)

    def _next_control(self):
        for _ in range(4):
            lane = self._control_lane
            self._control_lane = (lane + 1) % 4
            if lane == 0 and self._watch_info:
                self._watch_info = False
                return self.watch.info()
            if lane == 1 and self._acks:
                slot = next(iter(self._acks))
                return control(bytes((6, slot, self._acks.pop(slot))))
            if lane == 2 and self._descriptions:
                channel = next(iter(self._descriptions))
                index = self._descriptions.pop(channel)
                return self._parameters[channel].description(index, len(self.parameters))
            if lane == 3 and self._readbacks:
                channel = next(iter(self._readbacks))
                parameter = self._readbacks.pop(channel)
                return self._data(channel, parameter.value)
        return None

    def _next_plot(self):
        if self.state != 'running':
            return None
        for _ in self.plots:
            index = self._plot_index
            self._plot_index = (index + 1) % len(self.plots)
            if self._now >= self._plot_due[index]:
                entry = self.plots[index]
                self._plot_due[index] = self._now + entry.period_ms * 1_000_000
                return self._data(entry.channel, entry.read())
        return None

    def _next_watch(self):
        if self.state != 'running':
            return None
        for _ in self._slots:
            index = self._watch_index
            self._watch_index = (index + 1) % len(self._slots)
            slot = self._slots[index]
            if slot is not None and self._now >= slot[4]:
                channel, address, type_id, period, _, region = slot
                slot[4] = self._now + period * 1_000_000
                fmt = WATCH_FORMATS[type_id]
                raw = region.read(address, struct.calcsize(fmt))
                if len(raw) != struct.calcsize(fmt):
                    raise ValueError('watch reader returned an incorrect byte count')
                value = struct.unpack('<' + fmt, raw)[0]
                if type_id == 9:
                    value = int(bool(value))
                try:
                    return self._data(channel, float32(value))
                except ValueError:
                    self.diagnostics['sample_errors'] += 1
        return None

    def _next_memory(self):
        for _ in self.memory:
            index = self._memory_index
            self._memory_index = (index + 1) % len(self.memory)
            entry = self.memory[index]
            due = self._memory_info_due[index]
            sample_due = self.state == 'running' and self._now >= self._memory_due[index]
            if due is not None and self._now >= due and (not self._memory_sample_turn[index] or not sample_due):
                self._memory_info_due[index] = self._now + entry.info_period_ms * 1_000_000 if entry.info_period_ms else None
                self._memory_sample_turn[index] = True
                return entry.info()
            if sample_due:
                self._memory_sample_turn[index] = False
                self._memory_due[index] = self._now + entry.period_ms * 1_000_000
                value = entry.read()
                uint(value, 32, 'memory bytes')
                return self._data(entry.channel, value)
        return None

    def _next_frame(self):
        sources = (self._next_control, self._next_plot, self._next_watch, self._next_memory,
                   lambda: self._manual.popleft() if self._manual else None,
                   lambda: self.trace.next_frame() if self.trace else None)
        for _ in sources:
            lane = self._lane
            self._lane = (lane + 1) % len(sources)
            result = sources[lane]()
            if result is not None:
                # DATA and control retain their original framing in a mixed stream.
                return result
        return None

    def _flush_one(self):
        busy = getattr(self.sink, 'busy', lambda: False)
        if busy():
            return False
        if self._pending and self._offset == len(self._pending):
            self.diagnostics['frames_sent'] += 1
            self._pending = b''
            return True
        if not self._pending:
            self._pending = self._next_frame() or b''
            self._offset = 0
        if not self._pending:
            return False
        remaining = self._pending[self._offset:]
        accepted = self.sink.write(remaining)
        if accepted is None:
            accepted = len(remaining)
        if type(accepted) is not int or not 0 <= accepted <= len(remaining):
            raise ValueError('transport violated accepted-byte contract')
        self._offset += accepted
        self.diagnostics['bytes_sent'] += accepted
        if self._offset == len(self._pending):
            if busy():
                return False
            self.diagnostics['frames_sent'] += 1
            self._pending = b''
            return True
        return False

    def main(self):
        """Run bounded RX, fair feature turns and partial-write completion."""
        if not self._main_lock.acquire(blocking=False):
            raise RuntimeError('Core.main must have exactly one non-reentrant owner')
        try:
            self._start()
            if self.state == 'stopped':
                return self.state
            self._update_clock()
            service = getattr(self.sink, 'service', None)
            if service:
                service()
            if self.state == 'running':
                if hasattr(self.sink, 'read_packets'):
                    packets = self.sink.read_packets(self._rx_budget)
                    if hasattr(self.sink, 'peers'):
                        peers = self.sink.peers
                        self._decoders = {p: d for p, d in self._decoders.items() if p in peers or p in ('external', 'transport')}
                    for peer, data in packets:
                        self.feed(data, peer=peer)
                elif hasattr(self.sink, 'read'):
                    self.feed(self.sink.read(self._rx_budget), peer='transport')
                if hasattr(self.sink, 'peers'):
                    peers = self.sink.peers
                    self._decoders = {p: d for p, d in self._decoders.items() if p in peers or p in ('external', 'transport')}
                connections = getattr(self.sink, 'connections', 0)
                if connections != self._connections:
                    self._connections = connections
                    self.request_memory_info()
                    if self.trace:
                        self.trace.request_metadata()
            for _ in range(self._frames_per_main):
                if not self._flush_one():
                    break
            if self.state == 'draining' and not self._pending and not self._manual and not self._readbacks and not self._descriptions and not self._acks and not self._watch_info:
                if (self.trace is None or self.trace.ended) and not getattr(self.sink, 'pending', False):
                    self.state = 'stopped'
            return self.state
        except Exception as exc:
            self.state, self.error = 'fault', exc
            if self.trace:
                with self.trace._lock:
                    self.trace._stopping = True
            raise
        finally:
            self._main_lock.release()

    def request_stop(self):
        self._start()
        if self.state == 'running':
            if self.trace:
                self.trace.stop()
            self._memory_info_due = [None] * len(self.memory)
            self.state = 'draining'

    def close(self, timeout=2.0):
        """Drain accepted work and SESSION_END, or raise TimeoutError; always close."""
        try:
            self.request_stop()
            deadline = time.monotonic() + timeout
            while self.state != 'stopped':
                self.main()
                if self.state != 'stopped' and time.monotonic() >= deadline:
                    self.state = 'fault'
                    self.error = TimeoutError('Core could not drain the transport before close')
                    raise self.error
                if self.state != 'stopped':
                    time.sleep(.001)
        finally:
            self.sink.close()

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.close()
        else:
            # Preserve the application exception; do not claim a complete capture.
            if self.trace:
                with self.trace._lock:
                    self.trace._stopping = True
            self.state = 'fault'
            self.sink.close()
