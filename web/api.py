import asyncio
import json

from fastapi import APIRouter, Body, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from web.mqtt_bridge import bridge
from web.sim_registry import get, list_for_node, get_topics
import web.node_manager as node_manager

router = APIRouter()


class NodeConfig(BaseModel):
    protocol: str = "http"
    host: str
    port: int = 8282
    username: str = "admin"
    password: str = "admin"
    api_root: str = "api"
    mqtt_topic_root: str = "oshex"
    use_datastore: bool = True


# ── Node Management ────────────────────────────────────────────────────────

@router.get("/api/nodes")
def get_nodes():
    return node_manager.list_nodes()


@router.get("/api/nodes/{node_id}")
def get_node(node_id: str):
    state = node_manager.get_node(node_id)
    if not state:
        raise HTTPException(status_code=404, detail="Node not found")
    return state


@router.post("/api/nodes/connect")
async def connect_node(cfg: NodeConfig):
    loop = asyncio.get_event_loop()
    node_id, err = await loop.run_in_executor(
        None,
        lambda: node_manager.connect(
            cfg.protocol, cfg.host, cfg.port, cfg.username, cfg.password,
            cfg.api_root, cfg.mqtt_topic_root, cfg.use_datastore,
        ),
    )
    if err:
        raise HTTPException(status_code=500, detail=err)
    return {"status": "connected", "node_id": node_id}


@router.delete("/api/nodes/{node_id}")
async def disconnect_node(node_id: str):
    loop = asyncio.get_event_loop()
    ok, err = await loop.run_in_executor(None, lambda: node_manager.disconnect(node_id))
    if not ok:
        raise HTTPException(status_code=404, detail=err)
    return {"status": "disconnected"}


# ── Datastore ──────────────────────────────────────────────────────────────

@router.delete("/api/datastore")
def clear_datastore():
    node_manager.clear_store()
    return {"status": "cleared"}


# ── Sims (node-scoped) ─────────────────────────────────────────────────────

@router.get("/api/nodes/{node_id}/sims")
def get_sims_for_node(node_id: str):
    return list_for_node(node_id)


@router.post("/api/nodes/{node_id}/sims")
async def create_custom_sim(node_id: str, spec: dict = Body(...)):
    loop = asyncio.get_event_loop()
    sim_name, err = await loop.run_in_executor(
        None, lambda: node_manager.create_custom_sim(node_id, spec)
    )
    if err:
        raise HTTPException(status_code=400, detail=err)
    return {"status": "created", "sim_name": sim_name}


@router.post("/api/nodes/{node_id}/sims/{name}/start")
async def start_sim(node_id: str, name: str):
    sim = get(node_id, name)
    if not sim:
        raise HTTPException(status_code=404, detail=f"Sim '{name}' not found for node '{node_id}'")
    if not sim.should_simulate:
        loop = asyncio.get_event_loop()
        # Only call insert() if the datastore did not restore resources (sim.system is None)
        if sim.system is None:
            await loop.run_in_executor(None, sim.insert)
        # start() schedules asyncio tasks internally — must run on the event loop thread
        sim.start()
        for topic in get_topics(node_id, name).values():
            bridge.subscribe_topic(node_id, topic)
    return {"status": "started"}


@router.post("/api/nodes/{node_id}/sims/{name}/stop")
async def stop_sim(node_id: str, name: str):
    sim = get(node_id, name)
    if not sim:
        raise HTTPException(status_code=404, detail=f"Sim '{name}' not found for node '{node_id}'")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, sim.stop)
    return {"status": "stopped"}


@router.post("/api/nodes/{node_id}/sims/{name}/command")
async def send_command(node_id: str, name: str, body: dict):
    sim = get(node_id, name)
    if not sim:
        raise HTTPException(status_code=404, detail=f"Sim '{name}' not found for node '{node_id}'")
    if not hasattr(sim, "parse_command"):
        raise HTTPException(status_code=400, detail=f"Sim '{name}' does not support commands")
    sim.parse_command(json.dumps({"id": "web-api", "parameters": body}))
    return {"status": "ok"}


# ── MQTT Subscriptions (node-scoped) ───────────────────────────────────────

@router.get("/api/mqtt/subscriptions")
def get_all_mqtt_subscriptions():
    """Return subscriptions grouped by node_id."""
    return bridge.get_all_subscriptions()


@router.get("/api/mqtt/{node_id}/subscriptions")
def get_mqtt_subscriptions(node_id: str):
    return {"subscriptions": bridge.get_subscriptions(node_id)}


@router.post("/api/mqtt/{node_id}/subscribe")
def mqtt_subscribe(node_id: str, body: dict):
    topic = body.get("topic")
    if not topic:
        raise HTTPException(status_code=400, detail="topic required")
    bridge.subscribe_topic(node_id, topic)
    return {"status": "subscribed", "topic": topic}


@router.post("/api/mqtt/{node_id}/unsubscribe")
def mqtt_unsubscribe(node_id: str, body: dict):
    topic = body.get("topic")
    if not topic:
        raise HTTPException(status_code=400, detail="topic required")
    bridge.unsubscribe_topic(node_id, topic)
    return {"status": "unsubscribed", "topic": topic}


# ── WebSocket feed ─────────────────────────────────────────────────────────

@router.websocket("/ws/mqtt")
async def mqtt_ws(ws: WebSocket):
    await ws.accept()
    bridge.add_client(ws)
    try:
        while True:
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        bridge.remove_client(ws)