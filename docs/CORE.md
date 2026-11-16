# Python Core and Embedded 8 compatibility

Version 0.2 adds Plot, Parameters, Watch, Memory and Trace behind `Core.main()`.
The compatibility reference is **KymoCore v8.0.0**, not an uncommitted embedded
checkout. DATA/control retain the v6.2 framing; Trace uses wire v2. The original
`Sender`, `frame()` and transport constructors remain available.

## Start with parameters and periodic measurements

```python
import time
from kymocore import Core, Parameter, Plot

gain = Parameter(50, 1., minimum=0., maximum=5., name='gain', unit='V')
with Core.tcp_server(5025, '127.0.0.1',
                     plots=[Plot(0, lambda: gain.value * 2, period_ms=20)],
                     parameters=[gain]) as core:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        core.main()
        time.sleep(.001)
```

Connect a KymoStudio TCP source to port 5025, select channel 0, and discover the
parameters in its parameter panel. Writing `gain` changes the next measurement.
`Parameter` can own its value or bind a pair of `get=`/`set=` callbacks. Names
are at most 24 UTF-8 bytes, units at most 8. An omitted name becomes
`parameter_<channel>`. Existing Studio receivers expect at least two combined
name/unit bytes; use descriptive names for interoperability.

LIST returns every description and current value. Only a CRC-protected scalar
DATA write can change a registered parameter. Out-of-range writes return the
unchanged actual value; unknown IDs, malformed writes, extra fields and NO_CRC
writes do not confirm anything. Values and limits are float32. `feed_compact()`
also accepts the five-byte ID/float form for an externally framed CAN adapter;
this package does not contain a CAN driver.

## Feature definitions

| Definition | Behavior |
|---|---|
| `Plot(channel, read, period_ms=20)` | Periodically calls `read()`, returning a number or `Sample(y, x=None, z=None)`. Late cycles do not generate a catch-up burst. |
| `Parameter(channel, value, minimum=..., maximum=..., name=..., unit=..., get=..., set=...)` | Discovery, range-checked remote write, actual-value readback. Getter/setter must be provided together. |
| `Watch(ranges, build_id=b'', slots=8, min_period_ms=5, channels=(200, 215))` | Implements WATCH_INFO, WATCH_SET and every ACK status. INFO stops all slots; a rejected replacement also stops its slot. |
| `Memory(channel, kind, read, name='', total=0, period_ms=1000, info_period_ms=5000)` | Describes and samples byte counts. Metadata repeats on startup, interval, TCP/MQTT connection and `request_memory_info()`. Zero info period means startup/explicit request only. |
| `Trace(...)` | Explicit instrumentation, fragmented metadata/dictionary, bounded event rings, loss/error reports, periodic HEALTH and final SESSION_END. |

All Plot/Parameter/Memory channels and the complete Watch interval must be
disjoint. Constructor validation rejects collisions regardless of feature order.
`Core.send(channel, y, x=..., z=...)` additionally queues immediate measurements
on unreserved channels, raising `BufferError` if its bounded queue is full.

All measurement features use the same Core clock origin. `timestamps='us'`
(default), `'ms'` and `None` match the existing Sender behavior. Timestamps are
captured with acquisition, including when physical transmission is delayed.
`crc=True` protects outgoing DATA; control and trace checksums are mandatory.

## Live variables in Python

```python
from kymocore import Watch, WatchVariable, WatchType

watch = Watch.from_variables([
    WatchVariable('gain', WatchType.F32, lambda: gain.value),
], build_tag='my-application-1')
watch.export_elf('python-watch.elf')
```

Pass `watch=watch` to Core. In Studio's **Live variables** panel, load the exported
`python-watch.elf` and select `gain`. The existing browser reads its exact scalar
type from DWARF and matches its SHA-256 prefix against the advertised build ID;
no Studio change or mismatch override is needed.

This is a data-only **symbol map**, not firmware or a Python process-memory dump.
Names/types map to virtual eight-byte slots starting at `base_address=0x1000`.
They remain stable for the same declaration order, names, types and build tag.
Up to 256 names fit one range; active sampling is limited by the configured
slot count (eight by default). No Python object is implicitly dereferenced.

For manually assigned addresses, `WatchRange.variable(address, WatchType.F32,
getter)` still provides a simple scalar mapping; supply such ranges to `Watch`
directly. All nine Embedded types are supported, including bool normalization
and float32 conversion of 64-bit values. Nonfinite or unrepresentable sampled
values are counted and skipped.

For an existing binary layout use `WatchRange(start, length, read)`;
`read(address, width)` must return exactly the requested little-endian bytes.
The range and alignment are checked before any callback. At most four explicit
ranges and a 20-byte build ID fit the unchanged Embedded control protocol.
The callback owns synchronization with external writers.

The symbol-map writer uses only the standard library; no compiler, linker or
ELF parsing dependency is needed at runtime. Arbitrary manually configured
ranges have no automatic symbol export: use `Watch.from_variables()` when
you want the existing Studio browser to display named Python variables.

## Memory measurements

`MemoryKind.STACK_PEAK`, `HEAP_FREE`, and `HEAP_MIN_FREE` carry exactly the
Embedded meanings. The reader returns a uint32 count **in bytes**, with an
optional total size. Supply actual measurements from your application allocator
or native extension. Python allocation totals from `tracemalloc`, available OS
memory and a microcontroller's free heap are different measurements; the library
does not relabel one as another. The all-features example explicitly uses a
synthetic pool for demonstration.

## Tracing

```python
from kymocore import Trace

trace = Trace(device_id='test-bench', build_id='experiment-1')
trace.context(1, 'main')
trace.symbol(1, 'read-sensors', kind='scope')
trace.symbol(2, 'sample-ready', kind='marker')
# Pass trace=trace to Core, then inside its with block:
with trace.scope(1, 1):
    trace.mark(1, 2, 42)
```

Register symbols and contexts before the endpoint starts. `scope()` pairs
ENTER/EXIT even on exceptions. `@trace.instrument(context, symbol)` supports
ordinary and async functions. Give concurrent threads/tasks different context
IDs. Explicit `enter()`/`exit()` can carry an actual uint32 `stack_pointer` from
a suitable native adapter. A Python call depth is not a physical stack address.

Other hooks are `mark`, `reset`, `task_create`, `task_end`, `task_switch`,
`core_state`, `flow_send`, `flow_receive`, `isr_enter` and `isr_exit`. These emit
application-provided events; Python does not automatically observe the operating
system scheduler or hardware interrupts. Metadata explicitly reports scheduler,
ISR and core-state completeness as false. Consequently Studio can show spans,
wall-clock durations and flow correlations without claiming complete CPU usage.

One or two logical cores are supported. Hooks are serialized by a lock and do
no I/O. Each core has a bounded ring (default 64, power of two from 16 to 4096).
Overflow drops the new event while advancing event/context sequences and logical
scope state. Depth overflow and symbol mismatch mark a context invalid; unwind
its scopes and call `reset()` explicitly to recover. Inspect `trace.diagnostics`
for queued events, high-water marks, drops and errors.

Metadata and dictionaries repeat by default every five seconds and after a
new TCP/MQTT connection. `metadata_period_ms=0` disables periodic refresh.
`health_period_ms=1000` controls settlement reports, and `mtu=64..240` controls
fragmentation. Trace defaults to a monotonic nanosecond clock and a fresh random
session ID. A supplied clock must match `frequency_hz`; core IDs are logical
adapter identities, not an inferred physical CPU assignment.

Stop all producers and finish their scopes before `request_stop()` or `close()`.
SESSION_END follows all queued trace events and accounts for dropped attempts.
An exception escaping the Core context closes the transport without claiming
a complete session. A new run needs a new Core and Trace instance.

## Transports and lifecycle

| Constructor | Studio connection / return channel |
|---|---|
| `Core.udp(host, port, bind=None, **features)` | Studio UDP listener. `bind=(host, port)` sets a known device port; replies use the last received sender. |
| `Core.tcp_server(port, host='0.0.0.0', **features)` | Studio TCP client. Bounded queue per client, max 8 clients by default; slow clients exceeding it disconnect. |
| `Core.serial(port, baudrate=115200, **features)` | Same serial/USB link in both directions; requires the serial extra. |
| `Core.mqtt(host, topic, rx_topic=..., **features)` | `topic` carries telemetry; Studio publishes commands to the separate `rx_topic`. Reconnect resubscribes; retained commands are ignored. |

`Core` accepts the same feature arguments for each transport. Network and serial
processing is nonblocking. Paho owns its MQTT network loop; application callbacks
still execute in the thread calling `main()`. TCP disconnect statistics and MQTT
receive drops are available on `core.sink`.

The lifecycle is configuring → running → draining → stopped, or fault on a
callback/transport contract error. The first `main()`, `feed()`, `send()` or
context entry starts the configured features. Do not mutate feature definitions
after construction. Call `main()` from one owner, without reentrancy. There is
no hard-real-time guarantee for Python or callbacks that block.

`main()` processes at most `rx_budget=4096` input bytes (normal packets), and
`frames_per_main=16` complete outgoing frames. Sources share turns. An outgoing
frame remains pending through backpressure or short writes, preventing interleaving.
`queue_capacity=256` bounds manually queued measurements. Control readbacks and
ACKs coalesce by parameter/slot, matching Embedded pending-state behavior.

Custom sinks implement `write(bytes)` returning the accepted prefix length,
zero for backpressure, or `None` for full synchronous acceptance. Counts outside
the offered length latch a fault. An optional `busy()` holds borrowed bytes
until released. `read(budget)` or `read_packets(budget)` supplies incoming bytes;
packet keys distinguish byte streams, with `None` for self-contained datagrams.
Alternatively call `feed(data, peer=...)` yourself in bounded chunks. Optional
`service()` and `pending` support transport progress and final draining.

`request_stop()` begins finite draining. Keep calling `main()` until stopped,
or use `close(timeout=2)` / a `with` block. Close waits for TCP/MQTT pending work
and raises on timeout, always releasing the transport. UDP and TCP without an
attached receiver cannot prove remote delivery; protocol completion means the
transport accepted the bytes. The legacy `Sender` remains a simple immediate
sender; use Core when you need reliable handling of transport backpressure.

## Verification

Install the test extras with `pip install -e '.[test]'`, then run
`python -m pytest -q`. The suite includes unchanged Sender tests, native
Embedded-8 envelope fixtures, malformed/fragmented input, all Watch types and
statuses, parameter readbacks, timestamp retention, bounded queues, borrowed
buffers, tracing overflow, multithreaded contexts and async exception unwinding.

With a sibling `KymoStudio` checkout (or `KYMOSTUDIO` pointing to its root), tests
also use the actual Studio encoder/decoder and trace model: all five features,
valid reconstructed spans and session completion, live loopback TCP/UDP, and
named Python variable selection through Studio's actual ELF reader and registry.
Studio code remains separate and is never packaged. Serial is tested through
pyserial loopback; MQTT callback/reconnect/queue behavior uses a simulated client.
Physical hardware and an external MQTT broker need their own environment test.

Reference contracts: [DATA/control](https://github.com/KymoTrace/kymocore/blob/v8.0.0/docs/PROTOCOL.md),
[Trace v2](https://github.com/KymoTrace/kymocore/blob/v8.0.0/docs/TRACE_PROTOCOL.md),
[Embedded Core](https://github.com/KymoTrace/kymocore/blob/v8.0.0/docs/CORE_ENTRY.md).
