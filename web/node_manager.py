import logging
import re
import uuid

from oshconnect import OSHConnect, Node, SQLiteDataStore, System as OSHSystem
from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.streamableresource import StreamableModes

import web.sim_registry as sim_registry
from sims.controllable_counter_sim import ControllableCounterSim
from sims.external_device_sim import ExternalDeviceSim
from sims.lineofbearing import LoBSim
from web.mqtt_bridge import bridge

logger = logging.getLogger(__name__)

# node_id (8-char hex) -> {osh, node, state}
_nodes: dict[str, dict] = {}

# node_id -> {system_id: ExternalDeviceSim}
_external_devices: dict[str, dict[str, ExternalDeviceSim]] = {}

# node_id -> set of datastream topic prefixes already owned by known sims
# (populated lazily when a sim starts)
_known_obs_topics: dict[str, set[str]] = {}

# Shared SQLite store — survives connect() calls and app restarts
_store = SQLiteDataStore("OCSASim_data.db")


def list_nodes() -> dict:
    return {nid: entry["state"] for nid, entry in _nodes.items()}


def get_node(node_id: str) -> dict | None:
    entry = _nodes.get(node_id)
    return entry["state"] if entry else None


def _apply_connection_modes(sim) -> None:
    """Set BIDIRECTIONAL mode on a sim's datastream and controlstream so start() subscribes correctly."""
    if sim.datastream is not None:
        sim.datastream.set_connection_mode(StreamableModes.BIDIRECTIONAL)
    if sim.controlstream is not None:
        sim.controlstream.set_connection_mode(StreamableModes.BIDIRECTIONAL)


def _ensure_controlstream(sim) -> None:
    """If *sim* declares a controlstream_schema but restore left .controlstream as None,
    insert the controlstream now so cmd_listener has something to subscribe to.

    Old datastore rows may predate controlstream persistence, and the server may not have
    a controlstream for a system that was inserted before this sim's schema required one.
    """
    schema = getattr(sim, 'controlstream_schema', None)
    if schema is None or sim.controlstream is not None or sim.system is None:
        return
    try:
        sim.controlstream = sim.system.add_and_insert_control_stream(schema)
        sim.controlstream.set_connection_mode(StreamableModes.BIDIRECTIONAL)
        logger.info("Inserted missing controlstream for '%s'", sim.name)
    except Exception as e:
        logger.warning("Could not insert missing controlstream for '%s': %s", sim.name, e)


def _restore_sim_from_store(osh, node, sim, target_urn: str) -> bool:
    """
    Try to restore sim.system / .datastream / .controlstream from the SQLite datastore.
    Matches a stored node by address + port, then finds the first System whose URN equals
    *target_urn*. If found, the System is re-deserialized bound to the current *node* so it
    uses the live MQTT client and API helper.
    Returns True if successfully restored, False otherwise (caller should fall back to insert()).
    """
    try:
        old_nodes = _store.load_all_nodes(session_manager=osh._session_manager)
        for old_node in old_nodes:
            if old_node.address != node.address or old_node.port != node.port:
                continue
            for old_sys in old_node.systems():
                if old_sys.urn == target_urn:
                    new_sys = OSHSystem.deserialize(old_sys.serialize(), node)
                    sim.system = new_sys
                    sim.datastream = new_sys.datastreams[0] if new_sys.datastreams else None
                    # Stored serialization may omit control channels — query the server.
                    if not new_sys.control_channels:
                        try:
                            new_sys.discover_controlstreams()
                        except Exception as e:
                            logger.warning("discover_controlstreams for '%s' failed: %s", sim.name, e)
                    sim.controlstream = new_sys.control_channels[0] if new_sys.control_channels else None
                    node._systems.append(new_sys)
                    _ensure_controlstream(sim)
                    _apply_connection_modes(sim)
                    logger.info("Restored '%s' from datastore (system_id=%s)", sim.name, new_sys._resource_id)
                    return True
    except Exception as e:
        logger.warning("Could not restore '%s' from datastore: %s", sim.name, e)
    return False


def _restore_sim_from_node(node, sim, target_urn: str) -> bool:
    """
    Fallback: match a system already discovered on *node* by URN and restore the sim from it.

    This handles the case where the server already has the system (e.g. from a previous
    session) but the local datastore was cleared, so _restore_sim_from_store() returned False
    and a fresh insert() would fail with a 409 conflict.

    After discover_systems() + discover_datastreams() the discovered System objects already
    have their datastreams populated; control channels are discovered here if needed.
    Returns True on success, False if no matching system is found.
    """
    for sys in node._systems:
        if getattr(sys, 'urn', None) != target_urn:
            continue
        if sys._resource_id is None:
            # Discovered placeholder without a server ID — not usable
            continue
        sim.system = sys
        sim.datastream = sys.datastreams[0] if sys.datastreams else None
        # Control channels are not populated by the standard discover flow — query now
        if not sys.control_channels:
            try:
                sys.discover_controlstreams()
            except Exception as e:
                logger.warning("discover_controlstreams for '%s' failed: %s", sim.name, e)
        sim.controlstream = sys.control_channels[0] if sys.control_channels else None
        _ensure_controlstream(sim)
        _apply_connection_modes(sim)
        logger.info("Restored '%s' from discovered node systems (system_id=%s)", sim.name, sys._resource_id)
        return True
    return False


def clear_store() -> None:
    """Delete all persisted resources from the local datastore."""
    _store.clear()
    logger.info("Datastore cleared")


def connect(protocol: str, host: str, port: int, username: str, password: str,
            api_root: str = "api", mqtt_topic_root: str = "oshex",
            use_datastore: bool = False) -> tuple[str | None, str | None]:
    """
    Connect to an OSH node, optionally restoring sims from the local datastore.
    Returns (node_id, None) on success or (None, error_message) on failure.
    """
    try:
        node_id = uuid.uuid4().hex[:8]
        osh = OSHConnect("OCSASim", datastore=_store if use_datastore else None)
        node = Node(protocol, host, port, username, password,
                    api_root=api_root, mqtt_topic_root=mqtt_topic_root,
                    enable_mqtt=True)
        osh.add_node(node)
        osh.discover_systems()
        osh.discover_datastreams()
        osh.save_config()

        # Bind the bridge to this node's oshconnect MQTT client.
        # attach_node also subscribes to "#" so external-device discovery sees everything.
        bridge.attach_node(node_id, node)

        # Register hook for external device auto-discovery
        _external_devices[node_id] = {}
        _known_obs_topics[node_id] = set()
        bridge.register_message_hook(node_id, _on_mqtt_message)

        counter = ControllableCounterSim("ControllableCounter", osh, node)
        lob = LoBSim("LoBSim", osh, node)

        counter_urn = f"urn:OCSASim:ControllableCounter:{counter.name}"
        lob_urn     = f"urn:OCSASim:SimLoB:{lob.name}"

        if use_datastore:
            if not _restore_sim_from_store(osh, node, counter, counter_urn):
                if not _restore_sim_from_node(node, counter, counter_urn):
                    logger.info("[%s] Inserting ControllableCounter (not in datastore or node)", node_id)
                    counter.insert()

            if not _restore_sim_from_store(osh, node, lob, lob_urn):
                if not _restore_sim_from_node(node, lob, lob_urn):
                    logger.info("[%s] Inserting LoBSim (not in datastore or node)", node_id)
                    lob.insert()

            # Persist the full resource graph (upsert — safe to call on every connect)
            osh.save_to_store()
        else:
            if not _restore_sim_from_node(node, counter, counter_urn):
                counter.insert()
            if not _restore_sim_from_node(node, lob, lob_urn):
                lob.insert()

        state = {
            "connected": True,
            "node_id": node_id,
            "protocol": protocol,
            "host": host,
            "port": port,
            "username": username,
            "api_root": api_root,
            "mqtt_topic_root": mqtt_topic_root,
            "use_datastore": use_datastore,
            "error": None,
        }

        _nodes[node_id] = {"osh": osh, "node": node, "state": state}
        sim_registry.register(node_id, "counter", counter)
        sim_registry.register(node_id, "lob", lob)

        return node_id, None

    except Exception as e:
        logger.exception("connect() failed for %s:%s", host, port)
        return None, str(e)


def _spec_to_sim_name(spec: dict, node_id: str) -> str:
    """Derive a unique, URL-safe sim name from the spec label."""
    label = spec.get("label", "custom")
    base  = re.sub(r'[^a-z0-9_]', '', label.lower().replace(' ', '_').replace('-', '_'))
    base  = base.strip('_') or "custom"
    name, n = base, 1
    while sim_registry.get(node_id, name) is not None:
        name, n = f"{base}_{n}", n + 1
    return name


def create_custom_sim(node_id: str, spec: dict) -> tuple[str | None, str | None]:
    """Instantiate a CustomSim from *spec* and register it for *node_id*."""
    entry = _nodes.get(node_id)
    if not entry:
        return None, "Node not found"
    try:
        from sims.custom_sim import CustomSim
        sim_name = _spec_to_sim_name(spec, node_id)
        sim = CustomSim(sim_name, entry["osh"], entry["node"], spec)
        sim.insert()
        sim_registry.register(node_id, sim_name, sim)
        return sim_name, None
    except Exception as e:
        logger.exception("Error creating custom sim for node [%s]", node_id)
        return None, str(e)


# ── External device discovery ──────────────────────────────────────────────────

# Regex to extract the datastream ID from an observation MQTT topic.
# Matches both "/api/datastreams/{id}/observations" and
# "oshex/api/datastreams/{id}/observations" (with optional prefix).
_DS_OBS_RE = re.compile(r'(?:^|/)datastreams/([^/]+)/observations$')

# Regex to detect a command data topic for any control stream.
_CS_CMD_RE = re.compile(r'(?:^|/)controlstreams/([^/]+)/commands')


def _on_mqtt_message(node_id: str, topic: str, payload: str) -> None:
    """
    Hook called on every MQTT message for *node_id* (from paho I/O thread).
    Detects observation messages from unknown systems and triggers discovery.
    Routes command messages to the matching sim's parse_command().
    Routes messages for already-known external devices to their on_message().
    """
    # Route command messages into the owning sim's controlstream inbound deque
    # so cmd_listener picks them up naturally.
    if _CS_CMD_RE.search(topic):
        for sim in sim_registry.get_all_for_node(node_id):
            cs = getattr(sim, "controlstream", None)
            if cs is None:
                continue
            try:
                cmd_topic = cs.get_mqtt_topic(
                    subresource=APIResourceTypes.COMMAND, data_topic=True)
            except Exception:
                continue
            if cmd_topic and topic == cmd_topic:
                cs.get_inbound_deque().append(payload)
                return
        return

    # Check if this is a datastream observation topic
    m = _DS_OBS_RE.search(topic)
    if not m:
        return
    ds_id = m.group(1)

    # Update any known external device that owns this topic
    devices = _external_devices.get(node_id, {})
    for dev in devices.values():
        if topic in dev.obs_topics:
            dev.on_message(topic, payload)
            return  # handled

    # Topic not yet claimed — check if it belongs to an OCSASim sim
    known = _known_obs_topics.get(node_id, set())
    if topic in known:
        return  # belongs to a known sim, ignore

    # Cache known-sim topics lazily
    for sim in sim_registry.get_all_for_node(node_id):
        ds = getattr(sim, "datastream", None)
        if ds is not None:
            try:
                t = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION)
                if t:
                    known.add(t)
            except Exception:
                pass
    _known_obs_topics[node_id] = known

    if topic in known:
        return

    # Unknown observation topic — try to discover the external device
    logger.info("[%s] Unknown observation topic detected: %s — attempting discovery", node_id, topic)
    _try_discover_external_device(node_id, ds_id, topic)


def _try_discover_external_device(node_id: str, ds_id: str, trigger_topic: str) -> None:
    """
    Query the OSH REST API to find the system that owns *ds_id*,
    then build an ExternalDeviceSim and register it.
    """
    entry = _nodes.get(node_id)
    if not entry:
        return

    api  = entry["node"].get_api_helper()
    node = entry["node"]

    try:
        # 1. Fetch the triggering datastream to find its parent system
        resp = api.get_resource(APIResourceTypes.DATASTREAM, resource_id=ds_id)
        if not resp.ok:
            logger.warning("[%s] Could not fetch datastream %s: %d", node_id, ds_id, resp.status_code)
            return
        ds_data = resp.json()
        system_id = ds_data.get("systemId") or ds_data.get("system@id")
        if not system_id:
            logger.warning("[%s] Datastream %s has no systemId field", node_id, ds_id)
            return

        # Skip if we already have a device for this system
        if system_id in _external_devices.get(node_id, {}):
            return

        # 2. Fetch system info for the label
        sys_resp = api.get_resource(APIResourceTypes.SYSTEM, resource_id=system_id)
        system_label = system_id
        if sys_resp.ok:
            sys_data = sys_resp.json()
            system_label = (sys_data.get("properties", {}).get("name")
                            or sys_data.get("label")
                            or system_id)

        # 3. Fetch all datastreams for this system
        all_ds_resp = api.get_resource(
            APIResourceTypes.SYSTEM, resource_id=system_id,
            subresource_type=APIResourceTypes.DATASTREAM)
        obs_topics: list[str] = [trigger_topic]
        if all_ds_resp.ok:
            items = all_ds_resp.json()
            if isinstance(items, dict):
                items = items.get("items", [])
            for item in items:
                item_id = item.get("id")
                if item_id and item_id != ds_id:
                    t = api.get_mqtt_topic(
                        resource_type=APIResourceTypes.DATASTREAM,
                        subresource_type=APIResourceTypes.OBSERVATION,
                        resource_id=item_id)
                    if t and t not in obs_topics:
                        obs_topics.append(t)

        # 4. Fetch control streams for this system
        cs_resp = api.get_resource(
            APIResourceTypes.SYSTEM, resource_id=system_id,
            subresource_type=APIResourceTypes.CONTROL_CHANNEL)
        command_topic = ""
        status_topic  = ""
        if cs_resp.ok:
            items = cs_resp.json()
            if isinstance(items, dict):
                items = items.get("items", [])
            if items:
                cs_id = items[0].get("id")
                if cs_id:
                    command_topic = api.get_mqtt_topic(
                        resource_type=APIResourceTypes.CONTROL_CHANNEL,
                        subresource_type=APIResourceTypes.COMMAND,
                        resource_id=cs_id)
                    status_topic = api.get_mqtt_topic(
                        resource_type=APIResourceTypes.CONTROL_CHANNEL,
                        subresource_type=APIResourceTypes.STATUS,
                        resource_id=cs_id)

        # 5. Build and register the ExternalDeviceSim
        device_name = re.sub(r'[^a-z0-9_]', '', system_label.lower().replace(' ', '_')) or system_id[:8]
        device = ExternalDeviceSim(
            name=device_name,
            node=node,
            system_id=system_id,
            system_label=system_label,
            obs_topics=obs_topics,
            command_topic=command_topic,
            status_topic=status_topic,
        )
        _external_devices[node_id][system_id] = device

        # Subscribe the bridge to all this device's topics so messages flow
        for t in obs_topics:
            bridge.subscribe_topic(node_id, t)
        if status_topic:
            bridge.subscribe_topic(node_id, status_topic)

        logger.info("[%s] External device discovered: %s (system=%s)", node_id, device_name, system_id)

        # Broadcast discovery event to WebSocket clients
        bridge.broadcast_event({
            "type":    "device_discovered",
            "node_id": node_id,
            "device":  device.to_dict(),
        })

    except Exception as exc:
        logger.exception("[%s] Error during external device discovery: %s", node_id, exc)


def get_external_devices(node_id: str) -> list[dict]:
    """Return serialised state of all discovered external devices for *node_id*."""
    return [dev.to_dict() for dev in _external_devices.get(node_id, {}).values()]


def send_device_command(node_id: str, name: str, cmd_json: str) -> str | None:
    """
    Forward a command to the named external device.
    Returns an error string on failure, None on success.
    """
    devices = _external_devices.get(node_id, {})
    device  = next((d for d in devices.values() if d.name == name), None)
    if device is None:
        return f"External device '{name}' not found for node '{node_id}'"
    try:
        device.parse_command(cmd_json)
    except Exception as exc:
        return str(exc)
    return None


def disconnect(node_id: str) -> tuple[bool, str | None]:
    """Stop all sims for a node, shut down its MQTT monitor, and remove it."""
    entry = _nodes.pop(node_id, None)
    if not entry:
        return False, "Node not found"
    try:
        for sim in sim_registry.get_all_for_node(node_id):
            if getattr(sim, "should_simulate", False):
                try:
                    sim.stop()
                except Exception:
                    pass
        sim_registry.remove_for_node(node_id)
        bridge.detach_node(node_id)   # also clears the message hook
        _external_devices.pop(node_id, None)
        _known_obs_topics.pop(node_id, None)
    except Exception as e:
        logger.warning("Error during disconnect of [%s]: %s", node_id, e)
    return True, None