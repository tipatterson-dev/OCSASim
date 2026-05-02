import asyncio
import logging
import threading

import paho.mqtt.client as mqtt

from oshconnect import Node
from oshconnect.csapi4py.mqtt import MQTTCommClient

# These class-level patches affect every paho.Client in the process,
# including the one inside oshconnect's MQTTCommClient — so they still apply
# after this bridge stopped owning its own paho.Client.
#
# paho-mqtt 2.x's Client.__del__ reaches self._sock without a hasattr guard,
# raising AttributeError on Python 3.14 during interpreter shutdown after the
# loop thread is torn down. Wrap the finalizer to swallow that spurious error.
_paho_original_del = mqtt.Client.__del__


def _paho_safe_del(self):
    try:
        _paho_original_del(self)
    except AttributeError:
        pass


mqtt.Client.__del__ = _paho_safe_del

# A paho Client wraps a live socket, which `socket.__getstate__` refuses to
# pickle. Treat the Client as a singleton resource that aliases on copy/deepcopy
# so any deep-copy walk that lands on a Client stops at the boundary.
mqtt.Client.__deepcopy__ = lambda self, memo=None: self
mqtt.Client.__copy__ = lambda self: self

# oshconnect's EventBuilder.build() calls `event.model_copy(deep=True)` on
# events whose data/producer fields hold the live Node + OSHConnect graph —
# Node → MQTTCommClient → paho.Client → socket, and OSHConnect → DataStore →
# sqlite3.Connection. Both contain unpicklable system resources, so deepcopy
# fails. Subscribers receive in-process objects and expect identity to be
# preserved across publish, so a shallow copy is the correct semantic.


logger = logging.getLogger(__name__)


class MQTTBridge:
    def __init__(self):
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue | None = None
        self._clients: set = set()
        self._monitors: dict[str, MQTTCommClient] = {}    # node_id -> oshconnect MQTT client
        self._subscriptions: dict[str, set[str]] = {}     # node_id -> {topics}
        self._lock = threading.Lock()
        self._hooks: dict[str, callable] = {}             # node_id -> callback(node_id, topic, payload)

    def attach_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._queue = asyncio.Queue()

    # ── Monitor clients (one per connected node) ───────────────────────────

    def attach_node(self, node_id: str, node: Node) -> None:
        """Bind *node*'s oshconnect MQTT client to *node_id* and start monitoring all topics."""
        mqtt_client = node.get_mqtt_client()
        if mqtt_client is None:
            raise RuntimeError(
                f"Node {node_id} has no MQTT client — Node must be created with enable_mqtt=True"
            )

        with self._lock:
            self._monitors[node_id] = mqtt_client
            if node_id not in self._subscriptions:
                self._subscriptions[node_id] = set()

        # Subscribe to everything as a per-topic ("#") filtered callback. paho 2.x
        # calls every matching filtered callback, so this fires *alongside* the
        # per-topic callbacks oshconnect's Datastream/ControlStream register when
        # they start in BIDIRECTIONAL mode. Using set_on_message() instead would
        # be shadowed by those per-topic registrations and miss observation/command
        # messages on the shared client.
        # Bind node_id via closure since paho's callback signature has no per-call userdata.
        mqtt_client.subscribe(
            "#", qos=0,
            msg_callback=lambda c, u, msg: self._on_global_message(node_id, msg),
        )
        with self._lock:
            self._subscriptions[node_id].add("#")
        logger.info("MQTT monitor [%s] attached to oshconnect client; subscribed to #", node_id)

    def detach_node(self, node_id: str) -> None:
        """Stop tracking *node_id*. Does NOT disconnect the underlying client — the Node owns it."""
        with self._lock:
            client = self._monitors.pop(node_id, None)
            topics = self._subscriptions.pop(node_id, set())
        self._hooks.pop(node_id, None)
        if client is not None:
            for topic in topics:
                try:
                    client.unsubscribe(topic)
                except Exception as e:
                    logger.warning("Error unsubscribing [%s] %s: %s", node_id, topic, e)
            logger.info("MQTT monitor [%s] detached", node_id)

    def _on_global_message(self, node_id: str, msg) -> None:
        """Called from paho's I/O thread (via oshconnect's MQTTCommClient) — push onto the asyncio queue."""
        payload_str = msg.payload.decode("utf-8", errors="replace")
        logger.info("MQTT in [%s] %s %d bytes", node_id, msg.topic, len(msg.payload))
        payload = {
            "node_id": node_id,
            "topic": msg.topic,
            "payload": payload_str,
        }
        if self._loop:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, payload)
        # Notify registered hook (e.g. for external device auto-discovery)
        hook = self._hooks.get(node_id)
        if hook is not None:
            try:
                hook(node_id, msg.topic, payload_str)
            except Exception as exc:
                logger.warning("MQTT hook [%s] raised: %s", node_id, exc)

    def register_message_hook(self, node_id: str, callback) -> None:
        """Register a callback called on every MQTT message for *node_id*.

        Signature: callback(node_id: str, topic: str, payload: str) -> None
        Called from paho's I/O thread — keep it fast and non-blocking.
        """
        self._hooks[node_id] = callback

    def unregister_message_hook(self, node_id: str) -> None:
        self._hooks.pop(node_id, None)

    def broadcast_event(self, event: dict) -> None:
        """Push a non-MQTT event (e.g. device_discovered) to all WebSocket clients."""
        if self._loop:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, event)

    # ── Subscription management ────────────────────────────────────────────

    def subscribe_topic(self, node_id: str, topic: str):
        with self._lock:
            if node_id not in self._subscriptions:
                self._subscriptions[node_id] = set()
            if topic in self._subscriptions[node_id]:
                return
            self._subscriptions[node_id].add(topic)
            client = self._monitors.get(node_id)
        if client is not None:
            client.subscribe(topic, qos=0)
            logger.debug("MQTT [%s] subscribed to %s", node_id, topic)

    def publish_topic(self, node_id: str, topic: str, payload: str):
        """Publish *payload* to *topic* using the monitor client for *node_id*."""
        with self._lock:
            client = self._monitors.get(node_id)
        if client is not None:
            client.publish(topic, payload, qos=0)
            logger.debug("MQTT [%s] published to %s", node_id, topic)
        else:
            logger.warning("MQTT [%s] no monitor client — cannot publish to %s", node_id, topic)

    def unsubscribe_topic(self, node_id: str, topic: str):
        with self._lock:
            if node_id in self._subscriptions:
                self._subscriptions[node_id].discard(topic)
            client = self._monitors.get(node_id)
        if client is not None:
            client.unsubscribe(topic)
            logger.debug("MQTT [%s] unsubscribed from %s", node_id, topic)

    def get_subscriptions(self, node_id: str) -> list[str]:
        with self._lock:
            return sorted(self._subscriptions.get(node_id, set()))

    def get_all_subscriptions(self) -> dict[str, list[str]]:
        with self._lock:
            return {nid: sorted(topics) for nid, topics in self._subscriptions.items()}

    # ── Broadcast loop ─────────────────────────────────────────────────────

    async def broadcast_loop(self):
        """Forward queued MQTT messages to all connected WebSocket clients."""
        while True:
            msg = await self._queue.get()
            dead = set()
            for ws in self._clients:
                try:
                    await ws.send_json(msg)
                except Exception:
                    dead.add(ws)
            self._clients -= dead

    def add_client(self, ws):
        self._clients.add(ws)

    def remove_client(self, ws):
        self._clients.discard(ws)


bridge = MQTTBridge()