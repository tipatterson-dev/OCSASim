# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Install local oshconnect wheel (update version as needed)
uv pip install /Users/cr31/PycharmProjects/OSHConnect-Python/dist/oshconnect-0.4.0a0-py3-none-any.whl

# Run the simulator
uv run python main.py
```

There are no tests in this project currently.

## Architecture

OCSASim is a sensor simulation tool that connects to an OpenSensorHub (OSH) node via the `oshconnect` library and streams synthetic sensor observations.

**Entry point:** `main.py` — creates an `OSHConnect` instance, registers a `Node` (the OSH server), then instantiates, inserts, and starts one or more sims. The `asyncio` event loop is kept alive in an infinite sleep to let the simulation threads run.

**Sim base class:** `sims/sim.py` — abstract `Sim` class that owns a `System`, `Datastream`, and optional `ControlStream`. `insert()` registers the system and streams with the node; `start()` initializes the streams and launches the simulation in a background `Thread`. Subclasses implement `simulation()`.

**Concrete sims:**
- `sims/controllable_counter_sim.py` — `ControllableCounterSim(Sim)`: a counter with configurable bounds, step, and direction. Spawns two threads: `count_func` publishes observations every 5 seconds; `cmd_listener` subscribes to the control stream and updates counter parameters in response to inbound commands, then ACKs with a status message.
- `sims/gps.py` — `GPSSim` (does not extend `Sim`): publishes circular GPS track around Huntsville, AL every 0.5 s using `asyncio` tasks instead of threads.
- `sims/lineofbearing.py` — `LoBSim(Sim)`: publishes a rotating line-of-bearing value.

**OSHConnect concepts used:**
- `Node` — represents an OSH server endpoint (HTTP/MQTT).
- `System` — a logical sensor/platform registered on the node.
- `Datastream` / `ControlStream` — typed SWE-encoded streams; publishing uses `datastream.publish(json_str)` or `datastream.insert(dict)`.
- `DataRecordSchema` with field schemas (`TimeSchema`, `QuantitySchema`, `CountSchema`, `BooleanSchema`, `VectorSchema`) defines the SWE encoding sent to the server.

**To activate a sim:** uncomment the relevant block in `main.py`. Only one sim is active at a time in the current setup. `GPSSim` uses a different (async) lifecycle from `Sim` subclasses.