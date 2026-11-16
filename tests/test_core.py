import struct

import pytest

from kymocore import Core, Memory, MemoryKind, Parameter, Plot, Sample, Watch, WatchRange, WatchType, frame
from kymocore.protocol import StreamDecoder, control
from conftest import Sink, split_frames


def watch_set(slot=0, channel=200, address=0x3ffb1234, type=7, period=20):
    return control(struct.pack('<BBBI BH'.replace(' ', ''), 5, slot, channel, address, type, period))


def watch_config(**kwargs):
    return Watch([WatchRange(0x3ffb0000, 0x2c200, lambda address, size: struct.pack('<f', 3.5)[:size])],
                 build_id=bytes.fromhex('deadbeef'), **kwargs)


def test_control_golden_vectors():
    assert control(b'\x01').hex() == 'a55a600101d7'
    assert control(b'\x03').hex() == 'a55a600103d9'
    assert watch_config().info().hex() == 'a55a60120408050004deadbeef010000fb3f00c2020075'
    assert watch_set().hex() == 'a55a600a0500c83412fb3f07140055'
    assert control(bytes((6, 0, 0))).hex() == 'a55a6003060000eb'
    assert Memory(220, MemoryKind.STACK_PEAK, lambda: 0, name='parser', total=4096).info().hex() == 'a55a600d07dc01001000007061727365727e'


def test_incremental_decoder_all_boundaries_and_corruption():
    messages = [control(b'\x01'), watch_set(), frame(7, 1, crc=True), frame(3, 2, x=3, z=4, t=5, us=True)]
    for message in messages:
        for split in range(len(message) + 1):
            decoder = StreamDecoder()
            assert decoder.feed(message[:split]) + decoder.feed(message[split:]) == [message]
    bad = bytearray(messages[2])
    bad[-1] ^= 1
    decoder = StreamDecoder()
    assert decoder.feed(b'junk\xa5' + bad + b''.join(messages)) == messages
    assert decoder.invalid
    assert len(decoder._buffer) <= 68
    assert decoder.feed(b'x' * 100000) == []
    assert len(decoder._buffer) <= 68


def test_parameters_discovery_write_range_and_actual_readback(sink, clock):
    target = {'value': 2.}
    parameter = Parameter(7, minimum=0, maximum=10, name='gain', unit='V',
                          get=lambda: target['value'], set=lambda v: target.update(value=min(v, 8)))
    core = Core(sink, parameters=[parameter], timestamps=None, clock=clock)
    sink.rx.extend(control(b'\x01'))
    core.main()
    packets = split_frames(sink.data)
    assert parameter.description(0, 1) in packets
    assert frame(7, 2) in packets
    for value, expected in [(9., 8.), (20., 8.)]:
        sink.data.clear()
        sink.rx.extend(frame(7, value, crc=True))
        core.main()
        assert target['value'] == expected
        assert split_frames(sink.data) == [frame(7, expected)]
    assert core.diagnostics['writes_accepted'] == 1
    assert core.diagnostics['writes_rejected'] == 1
    core.close()


@pytest.mark.parametrize('packet', [frame(7, 3), frame(8, 3, crc=True), frame(7, 3, x=1, crc=True),
                                  frame(7, 3, t=1, crc=True), frame(7, 3, crc=True)[:-1] + b'\x00'])
def test_invalid_parameter_writes_never_apply_or_confirm(sink, packet):
    parameter = Parameter(7, 1, minimum=0, maximum=5, name='gain')
    core = Core(sink, parameters=[parameter])
    sink.rx.extend(packet)
    core.main()
    assert parameter.value == 1
    assert sink.data == b''
    core.close()


def test_compact_parameter_write(sink):
    parameter = Parameter(7, minimum=0, maximum=10, name='gain')
    core = Core(sink, parameters=[parameter], timestamps=None)
    core.feed_compact(bytes.fromhex('070000803f'))
    core.main()
    assert parameter.value == 1
    assert sink.data == frame(7, 1)
    core.close()


@pytest.mark.parametrize('options,status', [({'slot': 9}, 1), ({'channel': 20}, 2),
    ({'address': 0x3ffb1235}, 3), ({'type': 99}, 4), ({'period': 1}, 5), ({'address': 0}, 2)])
def test_watch_rejections_have_exact_ack(sink, options, status):
    core = Core(sink, watch=watch_config(), timestamps=None)
    core.feed(watch_set(**options))
    core.main()
    assert split_frames(sink.data) == [control(bytes((6, options.get('slot', 0), status)))]
    core.close()


def test_watch_sampling_stop_and_rejected_replacement(sink, clock):
    core = Core(sink, watch=watch_config(), timestamps=None, clock=clock)
    core.feed(watch_set())
    core.main()
    assert frame(200, 3.5) in split_frames(sink.data)
    sink.data.clear()
    clock.advance(19)
    core.main()
    assert not sink.data
    clock.advance(1)
    core.main()
    assert split_frames(sink.data) == [frame(200, 3.5)]
    core.feed(watch_set(type=99))
    sink.data.clear()
    clock.advance(30)
    core.main()
    assert split_frames(sink.data) == [control(bytes((6, 0, 4)))]
    core.feed(watch_set())
    core.main()
    core.feed(control(b'\x03'))
    sink.data.clear()
    clock.advance(30)
    core.main()
    assert split_frames(sink.data) == [core.watch.info()]
    core.feed(watch_set(address=1, type=99, period=0))
    sink.data.clear()
    core.main()
    assert split_frames(sink.data) == [control(bytes((6, 0, 0)))]
    core.close()


@pytest.mark.parametrize('type_id,value,expected', [(1, 255, 255), (2, -12, -12), (3, 65000, 65000),
    (4, -30000, -30000), (5, 16777217, 16777216), (6, -90000, -90000), (7, 1.5, 1.5),
    (8, 2.25, 2.25), (9, 9, 1)])
def test_all_watch_types_map_explicit_python_readers(sink, type_id, value, expected):
    region = WatchRange.variable(0x1000, WatchType(type_id), lambda: value)
    core = Core(sink, watch=Watch([region]), timestamps=None)
    core.feed(watch_set(address=0x1000, type=type_id))
    core.main()
    assert frame(200, expected) in split_frames(sink.data)
    core.close()


def test_plot_memory_clock_and_info_refresh(sink, clock):
    core = Core(sink, plots=[Plot(0, lambda: Sample(2, x=1, z=3), 20)],
                memory=[Memory(220, MemoryKind.HEAP_FREE, lambda: 123, name='heap', info_period_ms=50)],
                clock=clock, timestamps='ms')
    core.main()
    packets = split_frames(sink.data)
    assert frame(0, 2, x=1, z=3, t=0) in packets
    assert frame(220, 123, t=0) in packets
    assert core.memory[0].info() in packets
    sink.data.clear()
    clock.advance(50)
    core.main()
    assert core.memory[0].info() in split_frames(sink.data)
    assert frame(0, 2, x=1, z=3, t=50) in split_frames(sink.data)
    core.close()


def test_partial_writes_preserve_original_sample_timestamp(clock):
    sink = Sink(limit=1)
    core = Core(sink, plots=[Plot(0, lambda: 4)], clock=clock, timestamps='ms')
    core.main()
    clock.advance(100)
    core.request_stop()
    for _ in range(20):
        core.main()
    assert core.state == 'stopped'
    assert bytes(sink.data) == frame(0, 4, t=0)
    core.close()


def test_no_unbounded_sampling_catchup_and_all_lanes_progress(sink, clock):
    core = Core(sink, plots=[Plot(i, lambda: 1, 1) for i in range(20)], frames_per_main=2,
                parameters=[Parameter(50, minimum=0, maximum=5, name='gain')], clock=clock)
    for _ in range(15):
        clock.advance(1000)
        core.feed(control(b'\x01'))
        core.main()
    points = [p[3] for p in split_frames(sink.data) if p[2] != 0x60]
    assert set(range(15)) <= set(points)
    assert 50 in points
    core.close()


def test_collisions_bounded_manual_queue_and_transport_fault(sink):
    with pytest.raises(ValueError):
        Core(sink, plots=[Plot(7, lambda: 1)], parameters=[Parameter(7, minimum=0, maximum=1)])
    with pytest.raises(ValueError):
        Core(sink, plots=[Plot(200, lambda: 1)], watch=watch_config())
    core = Core(sink, queue_capacity=1)
    core.send(1, 1)
    with pytest.raises(BufferError):
        core.send(2, 2)
    sink.write = lambda data: len(data) + 1
    with pytest.raises(ValueError):
        core.main()
    assert core.state == 'fault'
    with pytest.raises(RuntimeError):
        core.main()


def test_close_times_out_and_always_releases_transport():
    sink = Sink(limit=0)
    core = Core(sink)
    core.send(0, 1)
    with pytest.raises(TimeoutError):
        core.close(timeout=0)
    assert sink.closed


@pytest.mark.parametrize('kwargs', [dict(id=0, y=None), dict(id=1.5, y=0), dict(id=0, y=1e100), dict(id=0, y=1, t=-1)])
def test_frame_rejects_malformed_values(kwargs):
    with pytest.raises(ValueError):
        frame(**kwargs)


def test_named_watch_layout_is_stable_and_identity_tracks_schema(tmp_path):
    from kymocore import WatchVariable
    import hashlib
    a = Watch.from_variables([WatchVariable('gain', WatchType.F32, lambda: 1)])
    b = Watch.from_variables([WatchVariable('gain', WatchType.F32, lambda: 999)])
    c = Watch.from_variables([WatchVariable('gain', WatchType.F64, lambda: 1)])
    path = a.export_elf(tmp_path / 'symbols.elf')
    assert path.read_bytes().startswith(b'\x7fELF')
    assert a.build_id == b.build_id == hashlib.sha256(path.read_bytes()).digest()[:20]
    assert a.build_id != c.build_id
    with pytest.raises(ValueError):
        Watch.from_variables([WatchVariable('gain', WatchType.F32, lambda: 1)] * 2)
    with pytest.raises(ValueError):
        watch_config().export_elf(tmp_path / 'invalid.elf')


def test_exported_symbol_map_has_real_scalar_dwarf_types(tmp_path):
    from kymocore import WatchVariable
    from elftools.elf.elffile import ELFFile
    variables = [WatchVariable(kind.name, kind, lambda: 1) for kind in WatchType]
    watch = Watch.from_variables(variables)
    path = watch.export_elf(tmp_path / 'types.elf')
    with path.open('rb') as stream:
        elf = ELFFile(stream)
        assert elf.num_segments() == 0  # symbol metadata, no loadable executable
        symbols = list(elf.get_section_by_name('.symtab').iter_symbols())[1:]
        assert [symbol.name for symbol in symbols] == [kind.name for kind in WatchType]
        assert [symbol['st_value'] for symbol in symbols] == list(range(0x1000, 0x1048, 8))
        unit = next(elf.get_dwarf_info().iter_CUs())
        entries = [die for die in unit.get_top_DIE().iter_children() if die.tag == 'DW_TAG_variable']
        types = [entry.get_DIE_from_attribute('DW_AT_type') for entry in entries]
        assert [type.attributes['DW_AT_encoding'].value for type in types] == [7, 5, 7, 5, 7, 5, 4, 4, 2]
        assert [type.attributes['DW_AT_byte_size'].value for type in types] == [1, 1, 2, 2, 4, 4, 4, 8, 1]


def test_memory_samples_progress_even_when_info_is_due_on_every_main(sink, clock):
    core = Core(sink, memory=[Memory(220, MemoryKind.HEAP_FREE, lambda: 10,
                info_period_ms=1, period_ms=1)], frames_per_main=1, clock=clock)
    with core:
        for _ in range(6):
            clock.advance(100)
            core.main()
    packets = split_frames(sink.data)
    assert sum(p[2] == 0x60 for p in packets) == 3
    assert sum(p[2] != 0x60 for p in packets) == 3
