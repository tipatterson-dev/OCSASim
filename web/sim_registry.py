from oshconnect.csapi4py.constants import APIResourceTypes

# Flat dict keyed by "{node_id}:{sim_name}"
_sims: dict[str, object] = {}


def register(node_id: str, name: str, sim):
    _sims[f"{node_id}:{name}"] = sim


def get(node_id: str, name: str):
    return _sims.get(f"{node_id}:{name}")


def list_for_node(node_id: str) -> dict:
    """Return {sim_name: sim_dict} for all sims belonging to *node_id*."""
    prefix = f"{node_id}:"
    return {
        k[len(prefix):]: _sim_to_dict(v)
        for k, v in _sims.items()
        if k.startswith(prefix)
    }


def remove_for_node(node_id: str):
    """Unregister all sims for *node_id* (called by disconnect)."""
    prefix = f"{node_id}:"
    for k in [k for k in list(_sims) if k.startswith(prefix)]:
        del _sims[k]


def get_all_for_node(node_id: str) -> list:
    """Return all sim objects (not dicts) for *node_id*."""
    prefix = f"{node_id}:"
    return [v for k, v in _sims.items() if k.startswith(prefix)]


def get_topics(node_id: str, name: str) -> dict:
    sim = get(node_id, name)
    if sim is None:
        return {}
    return _sim_topics(sim)


def _sim_topics(sim) -> dict:
    """Return a {label: mqtt_topic} map for all streams that have been inserted."""
    topics = {}
    ds = getattr(sim, "datastream", None)
    if ds is not None:
        try:
            topics["observations"] = ds.get_mqtt_topic(
                subresource=APIResourceTypes.OBSERVATION, data_topic=True)
        except Exception:
            pass
        try:
            topics["datastream_events"] = ds.get_event_topic()
        except Exception:
            pass

    cs = getattr(sim, "controlstream", None)
    if cs is not None:
        try:
            topics["commands"] = cs.get_mqtt_topic(
                subresource=APIResourceTypes.COMMAND, data_topic=True)
        except Exception:
            pass
        try:
            topics["control_status"] = cs.get_mqtt_topic(
                subresource=APIResourceTypes.STATUS, data_topic=True)
        except Exception:
            pass
        try:
            topics["controlstream_events"] = cs.get_event_topic()
        except Exception:
            pass
    return topics


def _sim_to_dict(sim) -> dict:
    d = {
        "name": sim.name,
        "running": getattr(sim, "should_simulate", False),
        "type": type(sim).__name__,
        "topics": _sim_topics(sim),
        "spec": getattr(sim, "spec", None),
    }
    if hasattr(sim, "spec"):
        d["params"] = {"interval": sim.spec.get("interval", 5)}
    else:
        d["params"] = {k: getattr(sim, k) for k in
                       ("count", "step", "lower_bound", "upper_bound", "count_down")
                       if hasattr(sim, k)}
    return d