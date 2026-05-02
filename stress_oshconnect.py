"""
Stress test for oshconnect against the live OSH node on localhost:8282 / mqtt 1883.
Exercises the API surface OCSASim depends on:

  - Node(enable_mqtt=True) construction + auth forwarding to MQTTCommClient
  - System / Datastream / ControlStream insert and discovery
  - Concurrent MQTT publish under load
  - Wildcard subscribe ("#") via per-topic message_callback_add — survives
    BIDIRECTIONAL streams adding their own per-topic callbacks (paho dispatches
    every matching filter, so both fire)
  - Inbound deque vs. global on_message dispatch
  - Repeated connect/disconnect (Node lifecycle)
  - Many sims publishing at once

Run: uv run python stress_oshconnect.py
"""
import json
import logging
import os
import sys
import threading
import time
import uuid
from collections import Counter

from oshconnect import OSHConnect, Node, System
from oshconnect.api_utils import URI
from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.streamableresource import StreamableModes
from oshconnect.swe_components import (
    DataRecordSchema, TimeSchema, QuantitySchema,
)
from oshconnect.schema_datamodels import SWEDatastreamRecordSchema
from oshconnect.encoding import JSONEncoding


HOST = os.environ.get("OSH_HOST", "localhost")
HTTP_PORT = int(os.environ.get("OSH_PORT", "8282"))
MQTT_PORT = int(os.environ.get("OSH_MQTT_PORT", "1883"))
USER = os.environ.get("OSH_USER", "admin")
PASS = os.environ.get("OSH_PASS", "admin")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("stress")

results = {"errors": [], "warnings": []}


def fail(label, exc):
    msg = f"{label}: {type(exc).__name__}: {exc}"
    log.error(msg)
    results["errors"].append(msg)


def warn(label, msg):
    log.warning("%s: %s", label, msg)
    results["warnings"].append(f"{label}: {msg}")


def make_obs_schema(field_name="value"):
    return DataRecordSchema(
        label="Stress Record",
        definition="http://stress.example/record",
        fields=[
            TimeSchema(
                name="time", label="Time",
                definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
                uom=URI(href="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"),
            ),
            QuantitySchema(
                name=field_name, label=field_name.title(),
                definition=f"http://stress.example/{field_name}",
                uom=URI(href="http://www.opengis.net/def/uom/OGC/0/unity"),
            ),
        ],
    )


def make_system(node, suffix):
    sys_obj = System(
        name=f"stressSystem{suffix}",
        label=f"Stress System {suffix}",
        urn=f"urn:OCSASim:Stress:{suffix}",
        parent_node=node,
    )
    return sys_obj


# ── Tests ───────────────────────────────────────────────────────────────────

def test_node_lifecycle_and_auth(n=5):
    """Repeatedly construct and tear down Nodes; verify MQTT client connects."""
    log.info("[t1] node_lifecycle: %d construct/get_mqtt_client cycles", n)
    for i in range(n):
        try:
            node = Node(
                "http", HOST, HTTP_PORT, USER, PASS,
                api_root="api", mqtt_topic_root="api",
                enable_mqtt=True, mqtt_port=MQTT_PORT,
            )
            client = node.get_mqtt_client()
            if client is None:
                fail("t1", RuntimeError(f"iter {i}: get_mqtt_client returned None"))
                continue
            # Wait briefly for paho's CONNACK
            for _ in range(20):
                if client.is_connected():
                    break
                time.sleep(0.05)
            if not client.is_connected():
                warn("t1", f"iter {i}: not connected after 1s")
            else:
                log.info("[t1] iter %d: connected", i)
            client.stop()
            client.disconnect()
        except Exception as e:
            fail("t1", e)
    log.info("[t1] done")


def test_wildcard_alongside_per_topic_callback():
    """
    Critical for OCSASim's bridge: when oshconnect's BIDIRECTIONAL stream adds
    a per-topic callback on the same paho client, the bridge's '#' filter
    callback must STILL fire for that topic (paho 2.x dispatches all matching
    filters). Without this, observations from sims wouldn't reach the WS feed.
    """
    log.info("[t2] wildcard_alongside_per_topic: insert datastream, subscribe '#' AND per-topic, publish, verify both fire")
    osh = OSHConnect("StressTest")
    node = Node(
        "http", HOST, HTTP_PORT, USER, PASS,
        api_root="api", mqtt_topic_root="api",
        enable_mqtt=True, mqtt_port=MQTT_PORT,
    )
    osh.add_node(node)
    time.sleep(0.5)  # let MQTT connect

    suffix = uuid.uuid4().hex[:6]
    sys_obj = make_system(node, suffix)
    node.add_system(sys_obj, insert_resource=True)
    try:
        ds = sys_obj.add_insert_datastream(make_obs_schema())
    except Exception as e:
        fail("t2", e)
        return
    ds.set_connection_mode(StreamableModes.BIDIRECTIONAL)
    ds.initialize()

    mqtt_client = node.get_mqtt_client()

    # Counters
    wildcard_hits = Counter()
    per_topic_hits = Counter()

    def wildcard_cb(c, u, msg):
        wildcard_hits[msg.topic] += 1

    def per_topic_cb(c, u, msg):
        per_topic_hits[msg.topic] += 1

    # Bridge-style: '#' wildcard via subscribe (uses message_callback_add internally)
    mqtt_client.subscribe("#", qos=0, msg_callback=wildcard_cb)
    time.sleep(0.3)

    # ds.start() in BIDIRECTIONAL mode adds a per-topic callback for the obs topic
    ds.start()
    time.sleep(0.3)

    # Replace the per-topic with our own counter so we can observe it independently
    obs_topic = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)
    mqtt_client.set_on_message_callback(obs_topic, per_topic_cb)
    time.sleep(0.2)

    # Publish 10 messages directly
    for i in range(10):
        ds.insert({"time": time.time(), "value": float(i)})
        time.sleep(0.1)

    time.sleep(1.0)  # let messages flow back

    log.info("[t2] obs_topic=%s", obs_topic)
    log.info("[t2] wildcard_hits[obs]=%d  per_topic_hits[obs]=%d",
             wildcard_hits[obs_topic], per_topic_hits[obs_topic])
    log.info("[t2] wildcard_hits.total=%d (sees other broker traffic too)", sum(wildcard_hits.values()))

    if wildcard_hits[obs_topic] < 10:
        warn("t2", f"wildcard saw {wildcard_hits[obs_topic]}/10 — per-topic shadowing likely")
    if per_topic_hits[obs_topic] < 10:
        warn("t2", f"per-topic saw {per_topic_hits[obs_topic]}/10")

    mqtt_client.stop()
    mqtt_client.disconnect()


def test_concurrent_publishers(n_sims=5, n_msgs=20):
    """N concurrent threads, each owning a Datastream, publishing simultaneously."""
    log.info("[t3] concurrent_publishers: %d sims × %d msgs", n_sims, n_msgs)
    osh = OSHConnect("StressTestConcurrent")
    node = Node(
        "http", HOST, HTTP_PORT, USER, PASS,
        api_root="api", mqtt_topic_root="api",
        enable_mqtt=True, mqtt_port=MQTT_PORT,
    )
    osh.add_node(node)
    time.sleep(0.5)

    mqtt_client = node.get_mqtt_client()
    seen = Counter()
    seen_lock = threading.Lock()

    def cb(c, u, msg):
        with seen_lock:
            seen[msg.topic] += 1

    mqtt_client.subscribe("#", qos=0, msg_callback=cb)
    time.sleep(0.3)

    datastreams = []
    for i in range(n_sims):
        suffix = f"{uuid.uuid4().hex[:4]}{i}"
        sys_obj = make_system(node, suffix)
        try:
            node.add_system(sys_obj, insert_resource=True)
            ds = sys_obj.add_insert_datastream(make_obs_schema())
            ds.initialize()
            datastreams.append(ds)
        except Exception as e:
            fail("t3.setup", e)

    log.info("[t3] %d datastreams ready", len(datastreams))

    def publisher(ds, idx):
        for j in range(n_msgs):
            try:
                ds.insert({"time": time.time(), "value": float(idx * 1000 + j)})
            except Exception as e:
                fail(f"t3.pub.{idx}", e)
            time.sleep(0.01)

    threads = [threading.Thread(target=publisher, args=(ds, i)) for i, ds in enumerate(datastreams)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    pub_elapsed = max(time.time() - t0, 1e-6)
    if not datastreams:
        warn("t3", "no datastreams created — skipping publish phase")
        return
    log.info("[t3] publishing done in %.2fs (%.0f msg/s)",
             pub_elapsed, (n_sims * n_msgs) / pub_elapsed)

    time.sleep(2.0)  # drain
    expected_per_ds = n_msgs
    total = sum(seen[ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)]
                for ds in datastreams)
    log.info("[t3] received %d/%d observations on '#' subscription",
             total, n_sims * n_msgs)
    for i, ds in enumerate(datastreams):
        topic = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)
        c = seen[topic]
        if c < expected_per_ds:
            warn("t3", f"ds[{i}] {topic} received {c}/{expected_per_ds}")

    mqtt_client.stop()
    mqtt_client.disconnect()


def test_publish_burst(n_msgs=500):
    """Hammer one datastream as fast as possible. Reveals queue/throughput issues."""
    log.info("[t4] publish_burst: 1 ds × %d msgs", n_msgs)
    osh = OSHConnect("StressBurst")
    node = Node(
        "http", HOST, HTTP_PORT, USER, PASS,
        api_root="api", mqtt_topic_root="api",
        enable_mqtt=True, mqtt_port=MQTT_PORT,
    )
    osh.add_node(node)
    time.sleep(0.5)

    sys_obj = make_system(node, f"burst{uuid.uuid4().hex[:6]}")
    try:
        node.add_system(sys_obj, insert_resource=True)
        ds = sys_obj.add_insert_datastream(make_obs_schema())
        ds.initialize()
    except Exception as e:
        fail("t4.setup", e)
        return

    mqtt_client = node.get_mqtt_client()
    obs_topic = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)
    received = [0]

    def cb(c, u, msg):
        received[0] += 1

    mqtt_client.subscribe(obs_topic, qos=0, msg_callback=cb)
    time.sleep(0.3)

    t0 = time.time()
    for i in range(n_msgs):
        ds.insert({"time": time.time(), "value": float(i)})
    pub_elapsed = time.time() - t0

    # Give the broker time to fan back to us
    for _ in range(50):
        if received[0] >= n_msgs:
            break
        time.sleep(0.1)
    rx_elapsed = time.time() - t0

    log.info("[t4] published %d in %.2fs (%.0f msg/s); received %d in %.2fs",
             n_msgs, pub_elapsed, n_msgs / pub_elapsed, received[0], rx_elapsed)
    if received[0] < n_msgs:
        warn("t4", f"lost {n_msgs - received[0]}/{n_msgs} msgs (qos=0; some loss possible)")

    mqtt_client.stop()
    mqtt_client.disconnect()


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    log.info("oshconnect stress test against %s:%s (mqtt %s) as %s",
             HOST, HTTP_PORT, MQTT_PORT, USER)
    try:
        import oshconnect
        log.info("oshconnect installed at %s", oshconnect.__file__)
    except Exception:
        pass

    test_node_lifecycle_and_auth(n=3)
    test_wildcard_alongside_per_topic_callback()
    test_concurrent_publishers(n_sims=4, n_msgs=15)
    test_publish_burst(n_msgs=200)

    print("\n" + "=" * 60)
    print(f"ERRORS:   {len(results['errors'])}")
    for e in results["errors"]:
        print(f"  - {e}")
    print(f"WARNINGS: {len(results['warnings'])}")
    for w in results["warnings"]:
        print(f"  - {w}")
    print("=" * 60)
    return 1 if results["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
