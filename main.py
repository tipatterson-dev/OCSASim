"""
OCSASim entry point.

Two modes:
  - web (default): start the FastAPI web UI on :4747
  - sims: connect to an OSH node and spawn one or more sims headlessly

Examples:
  uv run python main.py                                         # web UI
  uv run python main.py --mode web --port 4747
  uv run python main.py --mode sims --sims counter,lob          # 1 of each
  uv run python main.py --mode sims --sims counter --count 5    # 5 counters
  uv run python main.py --mode sims --config examples/sim_config.json

Sims run until SIGINT (Ctrl-C) or SIGTERM.
"""
import argparse
import asyncio
import json
import logging
import signal
import sys
import time

import uvicorn

from oshconnect import OSHConnect, Node


SIM_FACTORIES = {}  # populated lazily — avoids importing sims when running web

# Per-kind allowlist of params the loader is willing to setattr onto a sim
# instance. Keep these in sync with the class-level attrs the sims read at
# runtime (sims/controllable_counter_sim.py, sims/lineofbearing.py). Note:
# `count_down` and `step_sign` are independently settable here even though
# the sim's command path keeps them in sync — config-time wiring is loose.
PARAM_ALLOWLIST = {
    "counter": {"count", "lower_bound", "upper_bound", "step", "step_sign", "count_down"},
    "lob": {"interval", "raw_lob", "angle_step"},
}

NODE_DEFAULTS = {
    "port": 8282,
    "mqtt_port": 1883,
    "user": "admin",
    "password": "admin",
    "protocol": "http",
    "api_root": "api",
    "mqtt_topic_root": "api",
}
NODE_REQUIRED = {"host"}
NODE_ALLOWED = NODE_REQUIRED | set(NODE_DEFAULTS) | {"sims"}


def _load_sim_factories():
    if SIM_FACTORIES:
        return
    from sims.controllable_counter_sim import ControllableCounterSim
    from sims.lineofbearing import LoBSim
    SIM_FACTORIES["counter"] = ControllableCounterSim
    SIM_FACTORIES["lob"] = LoBSim


# ── Config loader ─────────────────────────────────────────────────────────

def _validate_config(cfg: dict) -> tuple[dict[str, dict], list[dict]]:
    """Validate and normalize a sim launch config.

    Returns (nodes, sim_specs). Each entry in sim_specs carries a `node`
    field naming which key of `nodes` it belongs to. Raises ValueError on
    bad input; callers should let it propagate to the CLI's error path.
    """
    if not isinstance(cfg, dict):
        raise ValueError(f"config must be a JSON object; got {type(cfg).__name__}")

    unknown = set(cfg) - {"nodes"}
    if unknown:
        raise ValueError(f"unknown top-level keys: {sorted(unknown)}")

    raw_nodes = cfg.get("nodes")
    if not isinstance(raw_nodes, dict) or not raw_nodes:
        raise ValueError("config must have a non-empty 'nodes' object")

    nodes: dict[str, dict] = {}
    sim_specs: list[dict] = []
    for nid, nspec in raw_nodes.items():
        if not isinstance(nspec, dict):
            raise ValueError(f"node {nid!r}: must be an object, got {type(nspec).__name__}")
        unknown_n = set(nspec) - NODE_ALLOWED
        if unknown_n:
            raise ValueError(f"node {nid!r}: unknown keys {sorted(unknown_n)}; allowed: {sorted(NODE_ALLOWED)}")
        missing = NODE_REQUIRED - set(nspec)
        if missing:
            raise ValueError(f"node {nid!r}: missing required keys {sorted(missing)}")
        # Connection block stripped of sims so it can be passed straight to _make_node
        node_conn = {k: v for k, v in nspec.items() if k != "sims"}
        nodes[nid] = {**NODE_DEFAULTS, **node_conn}

        node_sims = nspec.get("sims", []) or []
        if not isinstance(node_sims, list):
            raise ValueError(f"node {nid!r}: 'sims' must be an array, got {type(node_sims).__name__}")
        for i, sspec in enumerate(node_sims):
            if not isinstance(sspec, dict):
                raise ValueError(f"node {nid!r} sims[{i}]: must be an object, got {type(sspec).__name__}")
            unknown_s = set(sspec) - {"kind", "name", "params"}
            if unknown_s:
                raise ValueError(f"node {nid!r} sims[{i}]: unknown keys {sorted(unknown_s)}")
            kind = sspec.get("kind")
            if kind not in PARAM_ALLOWLIST:
                raise ValueError(
                    f"node {nid!r} sims[{i}]: 'kind' must be one of {sorted(PARAM_ALLOWLIST)}, got {kind!r}"
                )
            name = sspec.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(f"node {nid!r} sims[{i}]: 'name' must be a non-empty string")
            params = sspec.get("params", {}) or {}
            if not isinstance(params, dict):
                raise ValueError(f"node {nid!r} sims[{i}] ({name!r}): 'params' must be an object")
            bad = set(params) - PARAM_ALLOWLIST[kind]
            if bad:
                raise ValueError(
                    f"node {nid!r} sims[{i}] ({name!r}, {kind}): unknown params {sorted(bad)}; "
                    f"allowed: {sorted(PARAM_ALLOWLIST[kind])}"
                )
            sim_specs.append({"kind": kind, "name": name, "node": nid, "params": dict(params)})

    return nodes, sim_specs


def load_config(path: str) -> tuple[dict[str, dict], list[dict]]:
    with open(path) as fh:
        cfg = json.load(fh)
    return _validate_config(cfg)


def _flags_to_config(args) -> dict:
    """Build a config dict from CLI flags so flag-mode reuses the loader path."""
    requested = [s.strip() for s in args.sims.split(",") if s.strip()]
    sims = []
    for kind in requested:
        for i in range(args.count):
            name = kind if args.count == 1 else f"{kind}_{i}"
            sims.append({"kind": kind, "name": name})
    return {
        "nodes": {
            "default": {
                "host": args.host, "port": args.port, "mqtt_port": args.mqtt_port,
                "user": args.user, "password": args.password,
                "protocol": args.protocol, "api_root": args.api_root,
                "mqtt_topic_root": args.mqtt_topic_root,
                "sims": sims,
            }
        },
    }


def _make_node(spec: dict) -> Node:
    return Node(spec["protocol"], spec["host"], spec["port"], spec["user"], spec["password"],
                api_root=spec["api_root"], mqtt_topic_root=spec["mqtt_topic_root"],
                enable_mqtt=True, mqtt_port=spec["mqtt_port"])


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="OCSASim",
        description="Run OCSASim either as a web UI or as headless sims against an OSH node.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--mode", choices=["web", "sims"], default="web",
                   help="web = FastAPI UI; sims = headless sim runner")

    g_web = p.add_argument_group("web")
    g_web.add_argument("--web-host", default="0.0.0.0")
    g_web.add_argument("--web-port", type=int, default=4747)
    g_web.add_argument("--log-level", default="info")

    g_node = p.add_argument_group("OSH node (sims mode)")
    g_node.add_argument("--protocol", default="http")
    g_node.add_argument("--host", default="localhost")
    g_node.add_argument("--port", type=int, default=8282)
    g_node.add_argument("--mqtt-port", type=int, default=1883)
    g_node.add_argument("--user", default="admin")
    g_node.add_argument("--password", default="admin")
    g_node.add_argument("--api-root", default="api")
    g_node.add_argument("--mqtt-topic-root", default="api")

    g_sims = p.add_argument_group("sims (sims mode)")
    g_sims.add_argument("--config", default=None,
                        help="Path to a JSON sim launch config. When set, --sims/--count "
                             "and --host/--port/... are ignored.")
    g_sims.add_argument("--sims", default="counter",
                        help=f"Comma-separated sim kinds. Available: {','.join(['counter','lob'])}")
    g_sims.add_argument("--count", type=int, default=1,
                        help="Instances of each sim kind")
    return p.parse_args(argv)


# ── Web mode ──────────────────────────────────────────────────────────────

async def run_web(args):
    from web.app import app
    from web.mqtt_bridge import bridge

    bridge.attach_loop(asyncio.get_event_loop())
    asyncio.create_task(bridge.broadcast_loop())

    print(f"Web UI available at http://{args.web_host}:{args.web_port}")
    config = uvicorn.Config(app=app, host=args.web_host, port=args.web_port,
                            loop="none", log_level=args.log_level)
    server = uvicorn.Server(config)
    await server.serve()


# ── Sims mode ─────────────────────────────────────────────────────────────

def run_sims(args):
    _load_sim_factories()
    log = logging.getLogger("ocsasim.cli")
    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    try:
        if args.config:
            nodes_dict, sim_specs = load_config(args.config)
            log.info("loaded config from %s: %d node(s), %d sim(s)",
                     args.config, len(nodes_dict), len(sim_specs))
        else:
            nodes_dict, sim_specs = _validate_config(_flags_to_config(args))
    except (OSError, ValueError) as e:
        log.error("config error: %s", e)
        return 2

    if not sim_specs:
        log.error("no sims defined — exiting")
        return 1

    osh = OSHConnect("OCSASim-CLI")
    nodes_by_id: dict[str, Node] = {}
    for nid, nspec in nodes_dict.items():
        log.info("connecting node '%s' to %s://%s:%s (mqtt %s) as %s",
                 nid, nspec["protocol"], nspec["host"], nspec["port"],
                 nspec["mqtt_port"], nspec["user"])
        node = _make_node(nspec)
        osh.add_node(node)
        nodes_by_id[nid] = node
    # Brief settle so MQTT CONNACK lands before we publish
    time.sleep(0.5)

    sims = []
    for spec in sim_specs:
        factory = SIM_FACTORIES[spec["kind"]]
        try:
            sim = factory(spec["name"], osh, nodes_by_id[spec["node"]])
            for k, v in spec["params"].items():
                setattr(sim, k, v)
            sim.insert()
            sim.start()
            sims.append(sim)
            log.info("started sim '%s' (%s) on node '%s'%s",
                     spec["name"], spec["kind"], spec["node"],
                     f" with params {spec['params']}" if spec["params"] else "")
        except Exception as e:
            log.exception("failed to start sim '%s': %s", spec["name"], e)

    if not sims:
        log.error("no sims started — exiting")
        return 1

    log.info("running %d sim(s); Ctrl-C to exit", len(sims))

    stop = {"flag": False}
    def _handle_sig(sig, frame):
        log.info("signal %s — stopping", sig)
        stop["flag"] = True
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    try:
        while not stop["flag"]:
            time.sleep(0.5)
    finally:
        log.info("stopping %d sim(s)", len(sims))
        for sim in sims:
            try:
                sim.stop()
            except Exception as e:
                log.warning("error stopping %s: %s", getattr(sim, "name", "?"), e)
    return 0


# ── Entry ─────────────────────────────────────────────────────────────────

def main(argv=None):
    args = parse_args(argv)
    if args.mode == "web":
        asyncio.run(run_web(args))
        return 0
    elif args.mode == "sims":
        return run_sims(args)


if __name__ == "__main__":
    sys.exit(main())
