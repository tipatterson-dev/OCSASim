from oshconnect import OSHConnect, Node
import asyncio
import websockets
from multiprocessing import Process

from sims.gps import GPSSim
from sims.controllable_counter_sim import ControllableCounterSim


async def main():
    osh = OSHConnect("TESTConnect")
    local_node = Node("http", "localhost", 8282, "admin", "admin", enable_mqtt=True)
    osh.add_node(local_node)

    # gps_sim = GPSSim("Sim GPS", osh, local_node)
    # gps_sim.insert()
    #
    # gps_sim.start()

    counter_sim = ControllableCounterSim("ControllableCounter", osh, local_node)
    counter_sim.insert()
    counter_sim.start()

    while True:
        await asyncio.sleep(1)


def run_websocket_test():
    import asyncio

    asyncio.run(test_websocket())


async def test_websocket():
    uri = "ws://127.0.0.1:8282/sensorhub/api/datastreams/038q16egp1t0/observations?resultTime=latest/2026-01-01T12:00:00Z"
    print(f"Connecting to {uri} ...")
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected! Waiting for a message...")
            message = await websocket.recv()
            print(f"Received message: {message}")
    except Exception as e:
        print(f"WebSocket connection failed: {e}")


if __name__ == "__main__":
    # main(
    asyncio.run(main())