"""Optional tests against the actual, separately licensed Studio receiver.

Studio sources are imported in place, never copied into the Python package.
"""
import os
import socket
import struct
from pathlib import Path

import pytest

from kymocore import Core, Memory, MemoryKind, Parameter, Plot, Trace, Watch, WatchRange, WatchType
from conftest import split_frames


STUDIO = Path(os.environ.get('KYMOSTUDIO', Path(__file__).parents[2] / 'KymoStudio')) / 'python'
pytestmark = pytest.mark.skipif(not (STUDIO / 'Receiver/trace_protocol.py').is_file(), reason='Studio checkout unavailable')


def test_named_python_variables_work_in_existing_studio_elf_browser(sink, clock, monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(STUDIO))
    from kymocore import WatchVariable
    from Core.elf_variables import read_elf_variables
    from Backend.live_variables import LiveVariableRegistry
    from Receiver.binary_protocol import decode_parameter_frame, decode_data_frame
    from dataclasses import asdict

    values = {'gain': 2.5}
    variables = [WatchVariable('gain', WatchType.F32, lambda: values['gain'])]
    for kind in WatchType:
        variables.append(WatchVariable('value_' + kind.name.lower(), kind, lambda: 1))
    watch = Watch.from_variables(variables)
    exported = watch.export_elf(tmp_path / 'python-symbols.elf')
    symbols = read_elf_variables(exported)
    assert symbols.has_dwarf
    assert symbols.matches(watch.build_id.hex())
    assert len(symbols.variables) == 10
    assert not any(row['guessed'] for row in symbols.variables)
    assert {row['type'] for row in symbols.variables} == {'u8', 'i8', 'u16', 'i16', 'u32', 'i32', 'f32', 'f64', 'bool'}
    with Core(sink, watch=watch, clock=clock, timestamps=None) as core:
        registry = LiveVariableRegistry(lambda connection, data: sink.rx.extend(data), lambda connection: set())
        registry.load('python', symbols)
        registry.request_info('python')
        core.main()
        info = decode_parameter_frame(split_frames(sink.data)[0])
        registry.info('python', asdict(info))
        assert registry.build_status('python') == 'match'
        error, channel = registry.add('python', 'gain', 20)
        assert error == ''
        sink.data.clear()
        core.main()
        packets = split_frames(sink.data)
        ack = decode_parameter_frame(next(p for p in packets if p[2] == 0x60))
        registry.ack('python', asdict(ack))
        assert registry._connections['python'].watches['gain'].status == 'active'
        point = decode_data_frame(next(p for p in packets if p[2] != 0x60))
        assert (point.id, point.value) == (channel, 2.5)
        values['gain'] = 4.5
        sink.data.clear()
        clock.advance(20)
        core.main()
        assert decode_data_frame(split_frames(sink.data)[0]).value == 4.5


def test_all_five_features_through_actual_studio_decoder_and_trace_model(sink, clock, monkeypatch):
    monkeypatch.syspath_prepend(str(STUDIO))
    from Receiver.binary_protocol import (ProtocolStreamDecoder, decode_data_frame, decode_parameter_frame,
        encode_parameter_list_request, encode_parameter_write, encode_watch_info_request, encode_watch_set)
    from Receiver.trace_protocol import decode_frame
    from Core.trace_model import TraceSessionStore

    parameter = Parameter(50, 1, minimum=0, maximum=10, name='gain', unit='V')
    trace = Trace(clock=clock, session_id=99, mtu=64)
    trace.context(1, 'main')
    trace.symbol(1, 'sample')
    core = Core(sink, plots=[Plot(0, lambda: parameter.value)], parameters=[parameter],
        watch=Watch([WatchRange.variable(0x1000, WatchType.F32, lambda: parameter.value)], build_id=b'abcd'),
        memory=[Memory(220, MemoryKind.HEAP_FREE, lambda: 1234, name='heap')], trace=trace, clock=clock)
    with core:
        sink.rx.extend(encode_parameter_list_request() + encode_parameter_write(50, 4)
                       + encode_watch_info_request() + encode_watch_set(0, 200, 0x1000, 7, 20))
        with trace.scope(1, 1):
            clock.advance(1)
            core.main()
    decoder = ProtocolStreamDecoder()
    packets = []
    for byte in sink.data:
        packets += decoder.feed(bytes((byte,)))
    assert packets == split_frames(sink.data)
    data = [decode_data_frame(p) for p in packets if p[2] not in (0x60, 0x80)]
    assert {p.id for p in data} >= {0, 50, 200, 220}
    assert [p.value for p in data if p.id == 50] == [4.]
    descriptions = [decode_parameter_frame(p) for p in packets if p[2] == 0x60]
    assert {type(d).__name__ for d in descriptions} == {'ParameterDescription', 'WatchInfo', 'WatchAck', 'MemoryInfo'}
    store = TraceSessionStore()
    for packet in packets:
        if packet[2] == 0x80:
            store.ingest(decode_frame(packet))
    session = store.sessions[99]
    assert session.complete
    assert not session.gaps
    assert len(session.spans) == 1
    assert session.spans[0].end_ticks is not None
    assert session.spans[0].elapsed_ticks == 1_000_000
    assert not session.cpu_coverage


@pytest.mark.parametrize('mode', ['tcp', 'udp'])
def test_real_socket_parameter_discovery_and_write_using_studio_messages(monkeypatch, mode):
    monkeypatch.syspath_prepend(str(STUDIO))
    from Receiver.binary_protocol import (ProtocolStreamDecoder, decode_data_frame, decode_parameter_frame,
        encode_parameter_list_request, encode_parameter_write)
    parameter = Parameter(10, 1, minimum=0, maximum=5, name='gain')
    if mode == 'tcp':
        core = Core.tcp_server(0, '127.0.0.1', parameters=[parameter], timestamps=None)
        client = socket.create_connection(core.sink.address)
    else:
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.bind(('127.0.0.1', 0))
        core = Core.udp('127.0.0.1', client.getsockname()[1], bind=('127.0.0.1', 0), parameters=[parameter], timestamps=None)
        client.connect(core.sink.address)
    client.settimeout(1)
    try:
        with core:
            request = encode_parameter_list_request() + encode_parameter_write(10, 3)
            if mode == 'tcp':
                client.sendall(request[:3])
                core.main()
                client.sendall(request[3:])
            else:
                client.send(request)
            core.main()
            parser = ProtocolStreamDecoder()
            packets = []
            while len(packets) < 2:
                packets += parser.feed(client.recv(4096))
            assert parameter.value == 3
            assert any(p[2] == 0x60 and decode_parameter_frame(p).name == 'gain' for p in packets)
            assert any(p[2] != 0x60 and decode_data_frame(p).value == 3 for p in packets)
    finally:
        client.close()
