from oshconnect import OSHConnect, Node
import asyncio
import websockets
from multiprocessing import Process

from sims.gps import GPSSim


def main():
    osh = OSHConnect("GPSDriverConnect")
    local_node = Node("http", "localhost", 8282, "admin", "admin")
    osh.add_node(local_node)

    gps_sim = GPSSim("Sim GPS", osh, local_node)
    gps_sim.insert()

    process_1 = gps_sim.start()

    # Start websocket test in a separate process
    ws_process = Process(target=run_websocket_test)

    process_1.start()
    ws_process.start()
    # Optionally, join if you want to wait for it to finish
    # ws_process.join()


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
    main()