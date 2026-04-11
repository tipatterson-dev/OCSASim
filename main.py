import asyncio

import uvicorn
import websockets

from web.app import app
from web.mqtt_bridge import bridge


async def main():
    bridge.attach_loop(asyncio.get_event_loop())
    asyncio.create_task(bridge.broadcast_loop())

    print("Web UI available at http://localhost:8080")
    config = uvicorn.Config(app=app, host="0.0.0.0", port=8080, loop="none", log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
