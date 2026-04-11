import logging
import re
import uuid

from oshconnect import OSHConnect, Node, SQLiteDataStore, System as OSHSystem

import web.sim_registry as sim_registry
from sims.controllable_counter_sim import ControllableCounterSim
from sims.lineofbearing import LoBSim
from web.mqtt_bridge import bridge

logger = logging.getLogger(__name__)

# node_id (8-char hex) -> {osh, node, state}
_nodes: dict[str, dict] = {}

# Shared SQLite store — survives connect() calls and app restarts
_store = SQLiteDataStore("OCSASim_data.db")


def list_nodes() -> dict:
    return {nid: entry["state"] for nid, entry in _nodes.items()}


def get_node(node_id: str) -> dict | None:
    entry = _nodes.get(node_id)
    return entry["state"] if entry else None


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
                    sim.controlstream = new_sys.control_channels[0] if new_sys.control_channels else None
                    node._systems.append(new_sys)
                    logger.info("Restored '%s' from datastore (system_id=%s)", sim.name, new_sys._resource_id)
                    return True
    except Exception as e:
        logger.warning("Could not restore '%s' from datastore: %s", sim.name, e)
    return False


def clear_store() -> None:
    """Delete all persisted resources from the local datastore."""
    _store.clear()
    logger.info("Datastore cleared")


def connect(protocol: str, host: str, port: int, username: str, password: str,
            api_root: str = "api", mqtt_topic_root: str = "oshex",
            use_datastore: bool = True) -> tuple[str | None, str | None]:
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

        # Start a dedicated monitor MQTT client for this node
        bridge.start_monitor(node_id, host, username=username, password=password)

        counter = ControllableCounterSim("ControllableCounter", osh, node)
        lob = LoBSim("LoBSim", osh, node)

        if use_datastore:
            counter_urn = f"urn:OCSASim:ControllableCounter:{counter.name}"
            if not _restore_sim_from_store(osh, node, counter, counter_urn):
                logger.info("[%s] Inserting ControllableCounter (not in datastore)", node_id)
                counter.insert()

            lob_urn = f"urn:OCSASim:SimLoB:{lob.name}"
            if not _restore_sim_from_store(osh, node, lob, lob_urn):
                logger.info("[%s] Inserting LoBSim (not in datastore)", node_id)
                lob.insert()

            # Persist the full resource graph (upsert — safe to call on every connect)
            osh.save_to_store()
        else:
            counter.insert()
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
        bridge.stop_monitor(node_id)
    except Exception as e:
        logger.warning("Error during disconnect of [%s]: %s", node_id, e)
    return True, None