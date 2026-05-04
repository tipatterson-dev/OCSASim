"""
ExternalDeviceSim
=================
Wraps an externally-registered OSH system (e.g. a real Arduino or
MicroPython device) so that OCSASim can display its live data and
send commands to it through the same web UI.

Unlike the Sim subclasses, this class does NOT create any resources in
OSH — the device registers itself via setup_osh.py (or equivalent).
"""

import contextlib
import json
import logging
import time
from datetime import UTC, datetime

from oshconnect.streamableresource import Node

logger = logging.getLogger(__name__)


class ExternalDeviceSim:
    """
    Lightweight wrapper around an externally-managed OSH system.

    Attributes
    ----------
    name        : str  — unique key used in the sim registry and REST paths
    system_id   : str  — OSH system resource ID
    system_label: str  — human-readable label from OSH
    node        : Node — OSHConnect Node used to publish MQTT commands
    obs_topics  : list[str] — all datastream observation MQTT topics
    command_topic: str — controlstream command MQTT topic
    status_topic : str — controlstream status MQTT topic
    last_seen    : float — unix timestamp of last received MQTT message (0 = never)
    last_temp    : float | None
    last_count   : int | None
    """

    ONLINE_TIMEOUT_S = 60  # seconds without a message → considered offline

    def __init__(
        self,
        name: str,
        node: Node,
        system_id: str,
        system_label: str,
        obs_topics: list[str],
        command_topic: str,
        status_topic: str,
    ):
        self.name = name
        self.node = node
        self.system_id = system_id
        self.system_label = system_label
        self.obs_topics = obs_topics  # one per datastream
        self.command_topic = command_topic
        self.status_topic = status_topic

        self.last_seen: float = 0.0
        self.last_temp: float | None = None
        self.last_count: int | None = None

    # ── Incoming data ─────────────────────────────────────────────────────────

    def on_message(self, topic: str, payload_str: str) -> None:
        """Called by node_manager when an MQTT message arrives for this device."""
        self.last_seen = time.time()
        try:
            data = json.loads(payload_str)
            result = data.get("result", {})
        except (json.JSONDecodeError, AttributeError):
            return

        # Identify stream by field presence
        if "temperature" in result:
            with contextlib.suppress(TypeError, ValueError):
                self.last_temp = float(result["temperature"])
        if "count" in result:
            with contextlib.suppress(TypeError, ValueError):
                self.last_count = int(result["count"])

    # ── Outgoing command ──────────────────────────────────────────────────────

    def parse_command(self, cmd_json: str) -> None:
        """
        Send a direction/step command to the device's control stream.

        Expected cmd_json format (matches OCSASim web-API convention):
            {"id": "web-api", "parameters": {"direction_up": bool, "step": int}}
        """
        try:
            cmd = json.loads(cmd_json)
            params = cmd.get("parameters", {})
        except (json.JSONDecodeError, AttributeError):
            logger.warning("[ExternalDeviceSim:%s] Bad command JSON: %s", self.name, cmd_json)
            return

        payload = json.dumps(
            {
                "issueTime": datetime.now(UTC).isoformat(),
                "params": {
                    "direction_up": bool(params.get("direction_up", True)),
                    "step": int(params.get("step", 1)),
                },
            }
        )

        try:
            self.node.get_mqtt_client().publish(self.command_topic, payload)
            logger.debug(
                "[ExternalDeviceSim:%s] Command published to %s", self.name, self.command_topic
            )
        except Exception as exc:
            logger.error("[ExternalDeviceSim:%s] Publish failed: %s", self.name, exc)

    # ── Status helpers ────────────────────────────────────────────────────────

    def is_online(self) -> bool:
        if self.last_seen == 0.0:
            return False
        return (time.time() - self.last_seen) < self.ONLINE_TIMEOUT_S

    # ── Serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "system_id": self.system_id,
            "system_label": self.system_label,
            "online": self.is_online(),
            "last_temp": self.last_temp,
            "last_count": self.last_count,
            "topics": {
                "observations": self.obs_topics,
                "commands": self.command_topic,
                "status": self.status_topic,
            },
        }
