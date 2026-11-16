"""All five features on one connection; values and memory pool are synthetic.

Run: python examples/all_features.py --transport tcp --port 5025
Then connect KymoStudio's TCP source to localhost:5025. For UDP use a Studio
UDP listener on that port. Parameters are discovered through Studio's panel.
"""
import argparse
import math
import time

from kymocore import Core, Memory, MemoryKind, Parameter, Plot, Trace, Watch, WatchType, WatchVariable


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--transport', choices=('tcp', 'udp'), default='tcp')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=5025)
    parser.add_argument('--duration', type=float, default=30)
    parser.add_argument('--symbols', default='python-watch.elf', help='output symbol map for Studio live variables')
    args = parser.parse_args()

    gain = Parameter(50, 1, minimum=0, maximum=5, name='gain', unit='V')
    values = {'signal': 0., 'free': 4096}
    trace = Trace(device_id='python-demo', build_id='all-features-1')
    trace.context(1, 'main')
    trace.symbol(1, 'update')
    trace.symbol(2, 'cycle', kind='marker')
    watch = Watch.from_variables([
        WatchVariable('signal', WatchType.F32, lambda: values['signal']),
        WatchVariable('gain', WatchType.F32, lambda: gain.value),
    ], build_tag='all-features-1')
    symbol_file = watch.export_elf(args.symbols)
    features = dict(
        plots=[Plot(0, lambda: values['signal'], period_ms=20)],
        parameters=[gain],
        watch=watch,
        memory=[Memory(220, MemoryKind.HEAP_FREE, lambda: values['free'],
                       name='demo_pool', total=4096, period_ms=100)],
        trace=trace,
    )
    endpoint = (Core.tcp_server(args.port, args.host, **features) if args.transport == 'tcp'
                else Core.udp(args.host, args.port, **features))
    print(f'{args.transport.upper()} demo: {args.host}:{endpoint.sink.address[1] if args.transport == "tcp" else args.port}')
    print('Plot: 0; parameter: 50; watch: address 0x1000, f32, channel 200; memory: 220.')
    print(f'Load {symbol_file} in Studio live variables; choose signal or gain.')
    with endpoint:
        started = time.monotonic()
        cycle = 0
        while time.monotonic() - started < args.duration:
            with trace.scope(1, 1):
                values['signal'] = gain.value * math.sin(time.monotonic() - started)
                values['free'] = 4096 - cycle % 4096
                trace.mark(1, 2, cycle)
                endpoint.main()
            cycle += 1
            time.sleep(.01)
    print('Stopped after draining the transport and trace session.')


if __name__ == '__main__':
    main()
