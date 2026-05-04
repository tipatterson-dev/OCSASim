# OCSASim

A small synthetic-sensor simulator that publishes observations into an
[OpenSensorHub (OSH)](https://github.com/opensensorhub) node via the
[`oshconnect`](https://test.pypi.org/project/oshconnect/) Python library.
Comes with a couple of built-in sims (a controllable counter and a rotating
line-of-bearing) and a JSON config so you can stand up many of them at
different settings.

## Two ways to run

### 1. Locally (CLI)

Requires Python ≥ 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
# Sims against an OSH you're running yourself on the default ports:
uv run python main.py --mode sims --config examples/sim_config.json
```

`oshconnect` is pulled from `test.pypi.org` per `pyproject.toml` —
`uv sync` handles it.

### 2. Dockerized single-node + sim (recommended for day-to-day)

A two-container stack: one OSH node with the MQTT/CSAPI Part 3 add-ons
branch (`osh-addons@mqtt-csapi-pt3-update`, everything else `master`),
and one container running the sim.

```bash
# First-time setup (and any time the OSH source/branches change). Slow —
# this runs the full Java/Gradle build of OSH:
docker compose --profile build build osh-builder

# Day-to-day: bring up the node + sim
docker compose up -d --build sim osh-node-1
docker compose logs -f sim                 # watch the sim publish
docker compose down                        # stop, keep the H2 db
docker compose down -v                     # also drop the db volume
```

The dockerized OSH lives at:

| Service       | Container port | Host port       |
|---------------|---------------:|----------------:|
| HTTP API      | 8282           | **18282**       |
| MQTT broker   | 1883           | **11883**       |
| Admin UI      | 8181           | **18181**       |

Host ports are deliberately offset from OSH's defaults so a locally-installed
OSH won't conflict with the container. Inside the docker network the sim
still talks to `osh-node-1:8282` / `osh-node-1:1883` — the offsets only
affect how *you* reach the node from your laptop. So:

- Browse the admin UI: <http://localhost:18181>
- Hit the CSAPI: `curl -u admin:admin http://localhost:18282/sensorhub/api/systems`

#### Why the build/runtime split?

The OSH gradle build takes minutes and rarely changes. It lives in a
dedicated `osh-builder` image (`docker/osh-builder/Dockerfile`) that's only
rebuilt when you ask. The runtime image (`docker/osh-node/Dockerfile`) just
`COPY`s the unpacked distribution out of the builder, which means everyday
config tweaks rebuild in seconds. The `osh-builder` service is gated behind
`profiles: [build]` so `docker compose up` ignores it.

## Sim config schema

Sims are described in a JSON file passed via `--config`. One file can drive
several sims with per-instance parameters; each sim is nested under the node
it's attached to.

```json
{
  "nodes": {
    "osh": {
      "host": "osh-node-1",
      "port": 8282,
      "mqtt_port": 1883,
      "user": "admin",
      "password": "admin",
      "sims": [
        {"kind": "counter", "name": "fast",  "params": {"step": 1, "upper_bound": 50}},
        {"kind": "counter", "name": "slow",  "params": {"step": 20}},
        {"kind": "lob",     "name": "lob_a", "params": {"interval": 0.5, "angle_step": 5}}
      ]
    }
  }
}
```

Available sim kinds and their per-instance params:

- `counter` — `count`, `lower_bound`, `upper_bound`, `step`, `step_sign`, `count_down`
- `lob` — `interval`, `raw_lob`, `angle_step`

Two example configs ship in `examples/`:

- `sim_config.json` — points at `localhost:8282` (CLI use against a host OSH).
- `sim_config.docker.json` — points at the `osh-node-1` service name (used by
  the dockerized sim service automatically).

To run the sim against a different node or with different sims, edit the
mounted config file (compose mounts it read-only at `/config/sim_config.json`)
or override the mount with your own:

```yaml
# docker-compose.override.yml
services:
  sim:
    volumes:
      - ./my_config.json:/config/sim_config.json:ro
```

Sims run until the container is stopped (SIGINT/SIGTERM); there is no
duration timer.
