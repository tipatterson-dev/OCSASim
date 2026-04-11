import json
import logging
import math
import random
import time

logger = logging.getLogger(__name__)

from oshconnect import OSHConnect, Node, System
from oshconnect.api_utils import URI, UCUMCode
from oshconnect.swe_components import (
    DataRecordSchema, TimeSchema, QuantitySchema, CountSchema, BooleanSchema,
)
from oshconnect.timemanagement import TimeInstant

from sims.sim import Sim


def _to_camel(s: str) -> str:
    parts = s.replace('-', '_').replace(' ', '_').split('_')
    return parts[0].lower() + ''.join(p.title() for p in parts[1:])


# Behaviors available per field type (used by the builder UI and validated here)
BEHAVIORS = {
    "Quantity": ["static", "random", "sine", "ramp", "random_walk"],
    "Count": ["static", "random", "counter"],
    "Boolean": ["static", "random", "toggle"],
}


class CustomSim(Sim):
    """
    A dynamically-configured sim built from a JSON spec.

    Spec shape:
    {
      "label":       "My Sensor",          # human name
      "description": "optional",
      "interval":    5,                    # publish interval in seconds
      "fields": [
        {
          "name":     "temperature",       # camelCase key in result payload
          "label":    "Temperature",
          "type":     "Quantity",          # Quantity | Count | Boolean
          "unit":     "Cel",              # Quantity only
          "behavior": "sine",             # see BEHAVIORS map above
          "min": 18, "max": 30, "period": 60   # behavior-specific params
        },
        ...
      ]
    }
    """

    def __init__(self, name: str, app: OSHConnect, node: Node, spec: dict):
        super().__init__(name, app, node)
        self.spec = spec
        self.interval = float(spec.get("interval", 5))
        self._state: dict = {}
        self._tick: int = 0
        self._init_state()

    # ── State initialisation ───────────────────────────────────────────────

    def _init_state(self):
        for f in self.spec.get("fields", []):
            fn = f["name"]
            beh = f["behavior"]
            ft = f["type"]
            lo = float(f.get("min", 0))
            hi = float(f.get("max", 100))
            if beh == "static":
                if ft == "Boolean":
                    self._state[fn] = bool(f.get("value", False))
                elif ft == "Count":
                    self._state[fn] = int(f.get("value", 0))
                else:
                    self._state[fn] = float(f.get("value", 0))
            elif beh == "toggle":
                self._state[fn] = bool(f.get("value", False))
            elif beh in ("random", "sine", "ramp"):
                self._state[fn] = lo
            elif beh == "random_walk":
                # Start at midpoint so the walk has room in both directions immediately
                self._state[fn] = (lo + hi) / 2.0
            elif beh == "counter":
                self._state[fn] = int(f.get("start", lo))
            else:
                self._state[fn] = 0

    # ── Value generation ───────────────────────────────────────────────────

    def _next_value(self, f: dict):
        fn = f["name"]
        beh = f["behavior"]
        ft = f["type"]
        lo = float(f.get("min", 0))
        hi = float(f.get("max", 100))

        if beh == "static":
            return self._state[fn]

        if beh == "random":
            if ft == "Count":
                return random.randint(int(lo), int(hi))
            return round(random.uniform(lo, hi), 4)

        if beh == "sine":
            period_s = max(0.001, float(f.get("period", 60)))
            period_ticks = period_s / self.interval
            phase = (self._tick % period_ticks) / period_ticks
            return round(lo + (hi - lo) * (0.5 + 0.5 * math.sin(2 * math.pi * phase)), 4)

        if beh == "ramp":
            step = float(f.get("step", 1))
            val = self._state[fn] + step
            if val > hi:
                val = lo
            self._state[fn] = val
            return int(val) if ft == "Count" else round(val, 4)

        if beh == "random_walk":
            step = float(f.get("step", 1))
            val = self._state[fn] + random.uniform(-step, step)
            val = max(lo, min(hi, val))
            self._state[fn] = val
            return round(val, 4)

        if beh == "counter":
            step = int(f.get("step", 1))
            count_down = bool(f.get("count_down", False))
            val = self._state[fn] + (-step if count_down else step)
            if val > int(hi):
                val = int(lo)
            elif val < int(lo):
                val = int(hi)
            self._state[fn] = int(val)
            return int(val)

        if beh == "toggle":
            val = not self._state[fn]
            self._state[fn] = val
            return val

        return 0

    # ── Schema builder ─────────────────────────────────────────────────────

    def _build_schema(self) -> DataRecordSchema:
        sim_label = self.spec.get("label", self.name)
        schema = DataRecordSchema(
            label=sim_label,
            description=self.spec.get("description", f"Custom sim: {sim_label}"),
            definition="http://bottsinc.com/def/CustomSim",
            fields=[],
        )
        schema.fields.append(TimeSchema(
            label="Timestamp", name="timestamp",
            definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
            uom=URI(href="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"),
        ))
        for f in self.spec.get("fields", []):
            ft = f["type"]
            fn = f["name"]
            fl = f.get("label", fn)
            defn = f.get("definition", f"http://bottsinc.com/def/custom/{fn}")
            if ft == "Quantity":
                unit = f.get("unit", "1")
                schema.fields.append(QuantitySchema(
                    label=fl, name=fn, definition=defn,
                    uom=UCUMCode(code=unit, label=unit),
                ))
            elif ft == "Count":
                schema.fields.append(CountSchema(label=fl, name=fn, definition=defn))
            elif ft == "Boolean":
                schema.fields.append(BooleanSchema(label=fl, name=fn, definition=defn))
        return schema

    # ── Sim lifecycle ──────────────────────────────────────────────────────

    def insert(self, system=None, datastream_schema=None, controlstream=None):
        sim_label = self.spec.get("label", self.name)
        self.system = System(
            name=_to_camel(self.name),
            label=sim_label,
            urn=f"urn:OCSASim:Custom:{self.name}",
            parent_node=self.node,
        )
        self.node.add_system(self.system, True)
        self.datastream = self.system.add_insert_datastream(self._build_schema())

    def simulation(self):
        logger.info("CustomSim '%s' simulation loop started", self.name)
        while self.should_simulate:
            try:
                ts = TimeInstant.now_as_time_instant().get_iso_time()
                result = {f["name"]: self._next_value(f) for f in self.spec.get("fields", [])}
                obs = {"resultTime": ts, "phenomenonTime": ts, "result": result}
                self.datastream.publish(json.dumps(obs))
                self._tick += 1
            except Exception:
                logger.exception("CustomSim '%s' error on tick %d", self.name, self._tick)
            time.sleep(self.interval)
        logger.info("CustomSim '%s' simulation loop stopped", self.name)
