"""Standard transports. Core uses accepted-byte writes and bounded reads."""
import socket
from collections import deque
from threading import Lock, RLock


class Udp:
    """Send to Studio; optional bind provides a stable return-channel port."""

    def __init__(self, host: str, port: int, *, bind=None):
        self._address = (host, port)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if bind is not None:
            self._sock.bind(bind)
        self._sock.setblocking(False)

    def write(self, data: bytes):
        try:
            return self._sock.sendto(data, self._address)
        except BlockingIOError:
            return 0

    def read_packets(self, budget=4096):
        packets = []
        remaining = budget
        while remaining > 0:
            try:
                data, address = self._sock.recvfrom(65536)
            except BlockingIOError:
                break
            remaining -= max(1, len(data))
            if len(data) <= budget:
                self._address = address
                packets.append((None, data))
        return packets

    @property
    def address(self):
        return self._sock.getsockname()

    def close(self):
        self._sock.close()


class TcpServer:
    """Bounded nonblocking broadcast with independent client RX streams.

    Slow clients exceeding their TX queue are disconnected. Values sent without
    a client are dropped, matching the original Sender API.
    """

    def __init__(self, port: int, host: str = '0.0.0.0', *, max_clients=8, max_pending=65536):
        if type(max_clients) is not int or not 1 <= max_clients <= 16 or type(max_pending) is not int or max_pending < 240:
            raise ValueError('invalid TCP client/queue limit')
        self._server = socket.create_server((host, port))
        self._server.setblocking(False)
        self._clients = []
        self._pending = {}
        self._max_clients, self._max_pending = max_clients, max_pending
        self.connections = 0
        self.dropped_clients = 0

    @property
    def address(self):
        return self._server.getsockname()

    @property
    def pending(self):
        return any(self._pending.values())

    @property
    def peers(self):
        return tuple(self._clients)

    def _drop(self, client):
        client.close()
        self._clients.remove(client)
        self._pending.pop(client, None)

    def service(self):
        for _ in range(self._max_clients):
            try:
                client, _ = self._server.accept()
            except BlockingIOError:
                break
            if len(self._clients) >= self._max_clients:
                client.close()
                continue
            client.setblocking(False)
            self._clients.append(client)
            self._pending[client] = bytearray()
            self.connections += 1
        for client in self._clients[:]:
            pending = self._pending[client]
            if pending:
                try:
                    accepted = client.send(pending)
                    if accepted == 0:
                        self._drop(client)
                    else:
                        del pending[:accepted]
                except BlockingIOError:
                    pass
                except OSError:
                    self._drop(client)

    def write(self, data: bytes):
        self.service()
        for client in self._clients[:]:
            pending = self._pending[client]
            if len(pending) + len(data) > self._max_pending:
                self.dropped_clients += 1
                self._drop(client)
            else:
                pending.extend(data)
        self.service()
        return len(data)

    def read_packets(self, budget=4096):
        self.service()
        packets = []
        quota = max(1, budget // max(1, len(self._clients)))
        for client in self._clients[:]:
            try:
                data = client.recv(quota)
                if data:
                    packets.append((client, data))
                else:
                    self._drop(client)
            except BlockingIOError:
                pass
            except OSError:
                self._drop(client)
        return packets

    def close(self):
        for client in self._clients[:]:
            self._drop(client)
        self._server.close()


class Serial:
    """Serial/USB byte stream; Core selects nonblocking writes."""

    def __init__(self, port: str, baudrate: int = 115200, *, write_timeout=None):
        import serial
        self._port = serial.serial_for_url(port, baudrate, timeout=0, write_timeout=write_timeout)

    def write(self, data: bytes):
        return self._port.write(data)

    def read(self, budget=4096):
        return self._port.read(budget)

    def close(self):
        self._port.close()


class Mqtt:
    """Publish DATA; rx_topic enables bounded duplex control reception.

    Retained commands are ignored. Supply distinct publish/receive topics.
    """

    def __init__(self, host: str, topic: str, port: int = 1883, *, qos: int = 0,
                 username: str = None, password: str = None, client_id: str = '',
                 rx_topic=None, max_pending=65536):
        import paho.mqtt.client as mqtt
        if max_pending < 240 or rx_topic == topic:
            raise ValueError('invalid MQTT queue size or identical TX/RX topics')
        self._topic, self._qos, self._rx_topic = topic, qos, rx_topic
        self._messages = deque()
        self._inflight = set()
        self._early_published = set()
        self._publish_lock = RLock()
        self._rx_bytes = 0
        self._lock = Lock()
        self._max_pending = max_pending
        self.rx_drops = 0
        self.connections = 0
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self._client.on_publish = self._on_publish
        if rx_topic is not None:
            self._client.max_queued_messages_set(max_pending // 240)
            self._client.on_connect = self._on_connect
            self._client.on_message = self._on_message
        if username:
            self._client.username_pw_set(username, password)
        self._client.connect(host, port)
        self._client.loop_start()

    def write(self, data: bytes):
        with self._publish_lock:
            if len(self._inflight) >= self._max_pending // 240:
                return 0
            if self._rx_topic is not None and not self._client.is_connected():
                return 0
            result = self._client.publish(self._topic, data, qos=self._qos)
            if result is not None:
                # QoS 1/2 are queued by paho even when the socket just went away.
                if result.rc != 0 and not (result.rc == 4 and self._qos > 0):
                    return 0
                if result.mid in self._early_published:
                    self._early_published.remove(result.mid)
                else:
                    self._inflight.add(result.mid)
            return len(data)

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        with self._publish_lock:
            if mid in self._inflight:
                self._inflight.remove(mid)
            else:
                # QoS 0 may complete synchronously inside publish().
                self._early_published.add(mid)

    @property
    def pending(self):
        with self._publish_lock:
            return bool(self._inflight)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.connections += 1
            client.subscribe(self._rx_topic, qos=self._qos)

    def _on_message(self, client, userdata, message):
        data = bytes(message.payload)
        if message.retain or not data:
            return
        with self._lock:
            if self._rx_bytes + len(data) <= self._max_pending:
                self._messages.append(data)
                self._rx_bytes += len(data)
            else:
                self.rx_drops += 1

    def read_packets(self, budget=4096):
        result = []
        with self._lock:
            while self._messages and budget > 0:
                data = self._messages.popleft()
                self._rx_bytes -= len(data)
                if len(data) <= budget:
                    result.append((None, data))
                else:
                    self.rx_drops += 1
                budget -= len(data)
        return result

    def close(self):
        self._client.loop_stop()
        self._client.disconnect()
