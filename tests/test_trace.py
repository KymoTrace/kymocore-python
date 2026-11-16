import asyncio
import binascii
import json
import struct
import threading
import zlib
from pathlib import Path

import pytest

from kymocore import Core, Plot, Trace
from kymocore.trace import envelope
from conftest import Sink, split_frames


VECTORS = json.loads((Path(__file__).parent / 'fixtures/trace_protocol_vectors.json').read_text(encoding='utf-8'))['vectors']


@pytest.mark.parametrize('vector', VECTORS)
def test_all_embedded_8_golden_envelopes(vector):
    assert envelope(vector['kind'], int(vector['session_id'], 16), vector['packet_seq'],
                    bytes.fromhex(vector['payload_hex'])).hex() == vector['frame_hex']


def decode_trace(data):
    """Independent assertions on the public wire contract, including CRCs."""
    events, records, transfers, packets = [], [], {}, []
    for raw in split_frames(data):
        if raw[2] != 0x80:
            continue
        assert binascii.crc_hqx(raw[2:20], 0xffff) == int.from_bytes(raw[20:22], 'little')
        assert binascii.crc_hqx(raw[2:-2], 0xffff) == int.from_bytes(raw[-2:], 'little')
        packets.append(struct.unpack_from('<I', raw, 16)[0])
        kind, payload = raw[3], raw[22:-2]
        if kind == 3:
            schema, count, domain = struct.unpack_from('<BBH', payload)
            assert (schema, domain) == (1, 0)
            assert len(payload) == 4 + 32 * count
            events.extend(struct.iter_unpack('<QIIIHHBBHI', payload[4:]))
        elif kind in (1, 2, 5, 6):
            schema, repeated_kind, generation, transfer, total, offset = struct.unpack_from('<BBIIII', payload)
            assert (schema, repeated_kind, generation) == (1, kind, 1)
            record = transfers.setdefault((kind, transfer), bytearray())
            assert len(record) == offset
            record.extend(payload[18:])
            if len(record) == total:
                assert zlib.crc32(record[:-4]) & 0xffffffff == int.from_bytes(record[-4:], 'little')
                records.append((kind, bytes(record[:-4])))
                del transfers[kind, transfer]
    assert not transfers
    assert packets == list(range(len(packets)))
    return events, records


def configured_trace(clock, **kwargs):
    trace = Trace(clock=clock, session_id=123, **kwargs)
    trace.context(1, 'main')
    trace.symbol(1, 'work')
    trace.symbol(2, 'sample', 'marker')
    return trace


@pytest.mark.parametrize('mtu', [64, 240])
def test_metadata_events_and_complete_settlement_with_partial_writes(clock, mtu):
    sink = Sink(limit=7)
    trace = configured_trace(clock, mtu=mtu)
    core = Core(sink, trace=trace, clock=clock)
    with core:
        with trace.scope(1, 1):
            clock.advance(1)
            trace.mark(1, 2, 7)
        for _ in range(20):
            core.main()
    events, records = decode_trace(sink.data)
    assert [e[6] for e in events] == [1, 3, 2]
    assert [e[5] for e in events] == [1, 1, 1]
    assert [e[3] for e in events] == [0, 1, 2]
    assert [kind for kind, _ in records] == [1, 2, 6]
    meta = json.loads(records[0][1])
    assert meta['port_capabilities']['scheduler_complete'] is False
    assert meta['wire_version'] == 2
    end = records[-1][1]
    assert struct.unpack_from('<II', end, 8) == (3, 3)
    assert core.state == 'stopped'
    assert trace.enter(1, 1) is False
    assert all(len(raw) <= mtu for raw in split_frames(sink.data))


def test_overflow_counts_attempts_and_preserves_scope_state(sink, clock):
    trace = configured_trace(clock, capacity=16)
    core = Core(sink, trace=trace, clock=clock)
    with core:
        for _ in range(20):
            with trace.scope(1, 1):
                clock.advance(1)
        assert trace.diagnostics['drops'] == [24]
    events, records = decode_trace(sink.data)
    assert len(events) == 16
    assert struct.unpack_from('<III', records[-1][1], 8) == (40, 40, 24)


def test_depth_overflow_and_mismatch_require_unwind_then_reset(sink, clock):
    trace = configured_trace(clock, max_depth=1)
    core = Core(sink, trace=trace, clock=clock)
    with core:
        trace.enter(1, 1)
        trace.enter(1, 1)
        with pytest.raises(RuntimeError):
            trace.reset(1)
        trace.exit(1, 1)
        trace.exit(1, 2)  # mismatched symbol
        trace.reset(1, 1)
        with trace.scope(1, 1):
            pass
    events, _ = decode_trace(sink.data)
    assert events[1][8] & 2
    assert events[3][8] & 2
    assert not events[-1][8] & 2
    assert trace.diagnostics['errors'] == [2]


def test_refresh_and_health_do_not_lose_events(sink, clock):
    trace = configured_trace(clock, metadata_period_ms=20, health_period_ms=10)
    core = Core(sink, trace=trace, clock=clock)
    with core:
        core.main()
        clock.advance(30)
        with trace.scope(1, 1):
            pass
        for _ in range(5):
            core.main()
    events, records = decode_trace(sink.data)
    assert len(events) == 2
    assert sum(kind == 1 for kind, _ in records) >= 2
    health = [body for kind, body in records if kind == 5]
    assert health
    assert struct.unpack_from('<II', health[-1], 12) == (2, 2)


def test_threads_have_bounded_independent_contexts_and_sequences(sink, clock):
    trace = configured_trace(clock, capacity=64, core_count=2)
    trace.context(2, 'worker', 'task')
    core = Core(sink, trace=trace, clock=clock)
    def work(context, core_id):
        for _ in range(10):
            with trace.scope(context, 1, core=core_id):
                pass
    with core:
        threads = [threading.Thread(target=work, args=(1, 0)), threading.Thread(target=work, args=(2, 1))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    events, records = decode_trace(sink.data)
    assert len(events) == 40
    for core_id in range(2):
        assert [e[1] for e in events if e[7] == core_id] == list(range(20))
    assert records[-1][1][1] == 2


def test_async_decorator_unwinds_exceptions(sink, clock):
    trace = configured_trace(clock)
    core = Core(sink, trace=trace, clock=clock)
    @trace.instrument(1, 1)
    async def work():
        await asyncio.sleep(0)
        raise LookupError('application failure')
    with core:
        with pytest.raises(LookupError):
            asyncio.run(work())
    events, _ = decode_trace(sink.data)
    assert [e[6] for e in events] == [1, 2]


def test_scheduler_flow_isr_stack_and_explicit_stop(sink, clock):
    trace = configured_trace(clock)
    trace.context(2, 'worker', 'task')
    trace.context(3, 'interrupt', 'isr')
    core = Core(sink, trace=trace, clock=clock)
    with core:
        trace.task_create(2)
        trace.task_switch(1, 2)
        trace.core_state(2, 1, recovery=True)
        trace.enter(2, 1, stack_pointer=0x1000)
        with pytest.raises(RuntimeError):
            core.request_stop()
        trace.flow_send(2, 42)
        trace.isr_enter(3, 2)
        trace.isr_exit(3, 2)
        trace.flow_receive(1, 42)
        trace.exit(2, 1, stack_pointer=0x1000)
        trace.task_end(2)
    events, _ = decode_trace(sink.data)
    assert [e[6] for e in events] == [0x20, 0x22, 0x23, 1, 0x30, 4, 5, 0x31, 2, 0x21]
    assert events[3][8] & 16


def test_failed_application_does_not_claim_complete_session(sink, clock):
    trace = configured_trace(clock)
    with pytest.raises(ValueError):
        with Core(sink, trace=trace, clock=clock) as core:
            core.main()
            raise ValueError('failure')
    _, records = decode_trace(sink.data)
    assert not any(kind == 6 for kind, _ in records)
    assert sink.closed


def test_trace_binding_registration_and_disabled_hooks(sink, clock):
    trace = configured_trace(clock)
    assert trace.enter(1, 1) is False
    core = Core(sink, trace=trace, clock=clock)
    with pytest.raises(ValueError):
        Core(sink, trace=trace)
    with core:
        with pytest.raises(ValueError):
            trace.symbol(3, 'too late')
        with pytest.raises(ValueError):
            trace.enter(1, 55)
        with pytest.raises(ValueError):
            trace.enter(1, 1, core=2)


def test_trace_events_progress_during_continuous_fragmented_refresh(sink, clock):
    trace = configured_trace(clock, mtu=64, metadata_period_ms=1, capacity=256)
    core = Core(sink, trace=trace, clock=clock, frames_per_main=1)
    with core:
        for _ in range(100):
            clock.advance(10)
            trace.mark(1, 2)
            core.main()
        # The metadata refresh never catches up to its period; events must still
        # have reached the wire before stopping, rather than waiting behind it.
        assert any(p[2] == 0x80 and p[3] == 3 for p in split_frames(sink.data))
    events, _ = decode_trace(sink.data)
    assert len(events) == 100
