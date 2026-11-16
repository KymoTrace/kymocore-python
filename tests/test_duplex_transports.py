import socket
from types import SimpleNamespace

import pytest

from kymocore import Core, Parameter, frame, transport
from conftest import Sink


def test_tcp_clients_cannot_complete_each_others_control_frame():
    parameter = Parameter(1, 0, minimum=0, maximum=10)
    with Core.tcp_server(0, '127.0.0.1', parameters=[parameter]) as core:
        a = socket.create_connection(core.sink.address)
        b = socket.create_connection(core.sink.address)
        try:
            request = frame(1, 7, crc=True)
            a.sendall(request[:4])
            b.sendall(request[4:])
            core.main()
            assert parameter.value == 0
            a.sendall(request[4:])
            core.main()
            assert parameter.value == 7
        finally:
            a.close()
            b.close()


def test_tcp_slow_client_is_bounded_and_does_not_block_healthy_client():
    class Client:
        def __init__(self, slow):
            self.slow, self.data, self.closed = slow, bytearray(), False
        def send(self, data):
            if self.slow:
                raise BlockingIOError()
            self.data.extend(data)
            return len(data)
        def close(self):
            self.closed = True

    sink = transport.TcpServer(0, '127.0.0.1', max_pending=240)
    slow, healthy = Client(True), Client(False)
    sink._clients = [slow, healthy]
    sink._pending = {slow: bytearray(), healthy: bytearray()}
    try:
        for _ in range(3):
            assert sink.write(b'a' * 100) == 100
        assert slow.closed
        assert healthy.data == b'a' * 300
        assert sink.dropped_clients == 1
        assert not sink.pending
    finally:
        sink.close()


def test_busy_borrowed_transport_keeps_frame_until_released():
    class Borrowing(Sink):
        borrowed = None
        def write(self, data):
            assert self.borrowed is None
            self.borrowed = data
            return len(data)
        def busy(self):
            return self.borrowed is not None
        def release(self):
            self.data.extend(self.borrowed)
            self.borrowed = None
    sink = Borrowing()
    core = Core(sink, timestamps=None)
    core.send(0, 1)
    core.send(1, 2)
    core.main()
    first = sink.borrowed
    for _ in range(3):
        core.main()
        assert sink.borrowed is first
        assert core._pending == first
    sink.release()
    core.main()
    assert sink.borrowed == frame(1, 2)
    sink.release()
    core.close()
    assert sink.data == frame(0, 1) + frame(1, 2)


def test_serial_transport_duplex_with_real_pyserial_loopback():
    pytest.importorskip('serial')
    sink = transport.Serial('loop://')
    try:
        packet = frame(3, 2, crc=True)
        assert sink.write(packet) == len(packet)
        assert sink.read(4096) == packet
    finally:
        sink.close()


def test_mqtt_duplex_reconnect_limits_and_delivery_completion(monkeypatch):
    mqtt = pytest.importorskip('paho.mqtt.client')
    class Info:
        rc = 0
        mid = 1
    class Client:
        def __init__(self, *args, **kwargs):
            self.connected = True
            self.subscriptions, self.published = [], []
        def max_queued_messages_set(self, limit):
            self.limit = limit
        def connect(self, *args):
            pass
        def loop_start(self):
            pass
        def loop_stop(self):
            pass
        def disconnect(self):
            self.connected = False
        def is_connected(self):
            return self.connected
        def subscribe(self, topic, qos):
            self.subscriptions.append((topic, qos))
        def publish(self, topic, data, qos):
            info = Info()
            self.published.append((topic, data, qos, info))
            return info
    monkeypatch.setattr(mqtt, 'Client', Client)
    sink = transport.Mqtt('unused', 'data', rx_topic='commands', qos=1, max_pending=240)
    try:
        client = sink._client
        client.on_connect(client, None, None, 0, None)
        assert client.subscriptions == [('commands', 1)]
        for retained in (True, False):
            client.on_message(client, None, SimpleNamespace(payload=frame(3, 2, crc=True), retain=retained))
        assert sink.read_packets() == [(None, frame(3, 2, crc=True))]
        client.on_message(client, None, SimpleNamespace(payload=b'x' * 241, retain=False))
        assert sink.rx_drops == 1
        assert sink.write(b'data') == 4
        assert sink.pending
        assert sink.write(b'next') == 0
        client.on_publish(client, None, 1, 0, None)
        assert not sink.pending
        client.connected = False
        assert sink.write(b'next') == 0
        client.connected = True
        client.on_connect(client, None, None, 0, None)
        assert sink.connections == 2
        assert len(client.subscriptions) == 2
        # A socket-close race can still queue QoS 1 in paho. Delivery completion
        # must use on_publish; MQTTMessageInfo.is_published raises for rc=4.
        def disconnected_publish(*args, **kwargs):
            result = mqtt.MQTTMessageInfo(2)
            result.rc = mqtt.MQTT_ERR_NO_CONN
            return result
        client.publish = disconnected_publish
        assert sink.write(b'queued') == 6
        assert sink.pending
        client.on_publish(client, None, 2, 0, None)
        assert not sink.pending
        def immediate_publish(*args, **kwargs):
            client.on_publish(client, None, 1, 0, None)
            return Info()
        client.publish = immediate_publish
        assert sink.write(b'quick') == 5
        assert not sink.pending
    finally:
        sink.close()
