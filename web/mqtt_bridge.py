import asyncio
import logging
import threading

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MQTTBridge:
    def __init__(self):
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue | None = None
        self._clients: set = set()
        self._monitors: dict[str, mqtt.Client] = {}     # node_id -> paho.Client
        self._subscriptions: dict[str, set[str]] = {}   # node_id -> {topics}
        self._lock = threading.Lock()

    def attach_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._queue = asyncio.Queue()

    # ── Monitor clients (one per connected node) ───────────────────────────

    def start_monitor(self, node_id: str, host: str, port: int = 1883,
                      username: str = None, password: str = None):
        """Start (or restart) the MQTT monitor client for *node_id*."""
        with self._lock:
            old = self._monitors.get(node_id)
            if old is not None:
                try:
                    old.loop_stop()
                    old.disconnect()
                except Exception:
                    pass

            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"ocsa-bridge-{node_id}",
            )
            # Pass node_id via userdata so callbacks can identify the source
            client.user_data_set(node_id)
            if username and password:
                client.username_pw_set(username, password)

            client.on_connect = self._on_monitor_connect
            client.on_message = self._on_monitor_message

            if node_id not in self._subscriptions:
                self._subscriptions[node_id] = set()

            self._monitors[node_id] = client

        try:
            client.connect(host, port)
            client.loop_start()
            logger.info("MQTT monitor [%s] started → %s:%s", node_id, host, port)
        except Exception as exc:
            with self._lock:
                self._monitors.pop(node_id, None)
            logger.error("MQTT monitor [%s] connect failed: %s", node_id, exc)
            raise

    def stop_monitor(self, node_id: str):
        """Stop and remove the MQTT monitor client for *node_id*."""
        with self._lock:
            client = self._monitors.pop(node_id, None)
            self._subscriptions.pop(node_id, None)
        if client is not None:
            try:
                client.loop_stop()
                client.disconnect()
                logger.info("MQTT monitor [%s] stopped", node_id)
            except Exception as e:
                logger.warning("Error stopping MQTT monitor [%s]: %s", node_id, e)

    def _on_monitor_connect(self, client, userdata, flags, rc, properties):
        node_id = userdata
        if rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info("MQTT monitor [%s] connected", node_id)
            with self._lock:
                topics = set(self._subscriptions.get(node_id, []))
            for topic in topics:
                client.subscribe(topic, qos=0)
                logger.debug("MQTT monitor [%s] re-subscribed to %s", node_id, topic)
        else:
            logger.error("MQTT monitor [%s] connection failed rc=%s", node_id, rc)

    def _on_monitor_message(self, client, userdata, msg):
        """Called from paho's I/O thread — push onto the asyncio queue."""
        node_id = userdata
        payload = {
            "node_id": node_id,
            "topic": msg.topic,
            "payload": msg.payload.decode("utf-8", errors="replace"),
        }
        if self._loop:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, payload)

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