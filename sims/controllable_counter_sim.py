import copy
import json
import pickle
import time
from multiprocessing import Process, Queue
from threading import Thread
from typing import Literal

from oshconnect import OSHConnect, Node, System
from oshconnect.api_utils import URI
from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.swe_components import DataRecordSchema, TimeSchema, CountSchema, BooleanSchema
from oshconnect.timemanagement import TimeInstant
from pydantic.v1.utils import to_lower_camel

from sims.sim import Sim


class ControllableCounterSim(Sim):
    """
    A simple controllable counter simulation.
    """
    count_down: bool = False
    count: int = 0
    lower_bound: int = 0
    upper_bound: int = 100
    step: int = 10
    step_sign: Literal[1, -1] = 1

    ds_schema: DataRecordSchema = None
    controlstream_schema: DataRecordSchema = None

    def __init__(self, name: str, app: OSHConnect, node: Node):
        super().__init__(name, app, node)
        self.count = self.lower_bound

        self.ds_schema = DataRecordSchema(label="Controllable Counter",
                                          description="A simple controllable counter simulation",
                                          definition="http://bottsinc.com/def/ControllableCounter", fields=[])
        timestamp = TimeSchema(label="Timestamp", name="timestamp", description="Timestamp of the GPS data",
                               definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
                               uom=URI(href="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"))
        counter_schema = CountSchema(label="Count", name="count", definition="http://bottsinc.com/def/CounterSim", )
        count_down_schema = BooleanSchema(label="Count Down", name="countDown",
                                          definition="http://bottsinc.com/def/CountDown")
        step_schema = CountSchema(label="Step", name="step", definition="http://bottsinc.com/def/Step")
        lower_bound_schema = CountSchema(label="Lower Bound", name="lowerBound",
                                         definition="http://bottsinc.com/def/LowerBound")
        upper_bound_schema = CountSchema(label="Upper Bound", name="upperBound",
                                         definition="http://bottsinc.com/def/UpperBound")

        self.ds_schema.fields.extend(
            [timestamp, counter_schema, count_down_schema, step_schema, lower_bound_schema, upper_bound_schema])

        self.controlstream_schema = DataRecordSchema(label="Controllable Counter Control",
                                                     description="Control stream for the controllable counter simulation",
                                                     definition="http://bottsinc.com/def/ControllableCounterControl",
                                                     fields=[])
        set_count_down_schema = BooleanSchema(label="Set Count Down", name="setCountDown",
                                              definition="http://bottsinc.com/def/SetCountDown")
        set_step_schema = CountSchema(label="Set Step", name="setStep", definition="http://bottsinc.com/def/SetStep")
        set_lower_bound_schema = CountSchema(label="Set Lower Bound", name="setLowerBound",
                                             definition="http://bottsinc.com/def/SetLowerBound")
        set_upper_bound_schema = CountSchema(label="Set Upper Bound", name="setUpperBound",
                                             definition="http://bottsinc.com/def/SetUpperBound")

        self.controlstream_schema.fields.extend(
            [set_count_down_schema, set_step_schema, set_lower_bound_schema, set_upper_bound_schema])

    def insert(self, system: System = None, datastream_schema: DataRecordSchema = None,
               ControlStream: DataRecordSchema = None):
        """
        Inserts the default system and datastream into the node. Ignores the params from the super class for this sim.
        :param system:
        :param datastream_schema:
        :param ControlStream:
        :return:
        """
        self.system = System(name=to_lower_camel(self.name), label=self.name,
                             urn=f"urn:OCSASim:ControllableCounter:{self.name}", parent_node=self.node)
        self.node.add_system(self.system, self.node, True)

        self.datastream = self.system.add_insert_datastream(self.ds_schema)
        self.controlstream = self.system.add_and_insert_control_stream(self.controlstream_schema)

    def simulation(self):
        # check for commands
        # update based on commands

        # check pickling
        # pickle.dumps(self.count_func)
        # pickle.dumps(self.cmd_listener)

        listener_t = Thread(target=self.cmd_listener)
        counter_t = Thread(target=self.count_func)
        # count!!!
        listener_t.start()
        counter_t.start()

        while self.should_simulate:
            pass

        listener_t.join()
        counter_t.join()

    def count_func(self):
        while True:
            self.count += self.step * self.step_sign
            match self.step_sign:
                case 1:
                    if self.count >= self.upper_bound:
                        self.count = self.lower_bound
                case -1:
                    if self.count <= self.lower_bound:
                        self.count = self.upper_bound
            print(f"Counter: {self.count}")
            obs = self.create_obs(self.count)
            self.datastream.publish(json.dumps(obs))
            time.sleep(5)

    def cmd_listener(self):
        self.controlstream.subscribe(APIResourceTypes.COMMAND.value)

        while self.should_simulate:
            inbound = self.controlstream.get_inbound_deque()
            if len(inbound) > 0:
                cmd = inbound.popleft()
                self.process_command(cmd)

    def process_command(self, command):
        # for now just print command data
        print(f"PROCESSING COUNTSIM COMMAND: {command}")
        self.parse_command(command)

    def parse_command(self, command):
        # bytes to dict
        cmd_dict = json.loads(command)
        params = cmd_dict.get("parameters")



        cd_old = copy.copy(self.count_down)
        step_old = copy.copy(self.step)
        lb_old = copy.copy(self.lower_bound)
        ub_old = copy.copy(self.upper_bound)

        did_update = False

        if "setCountDown" in params:
            if params["setCountDown"] != cd_old:
                self.count_down = params["setCountDown"]
                self.step_sign = -1 if self.count_down else 1
                did_update = True

        if "setStep" in params:
            if params["setStep"] != step_old:
                self.step = params["setStep"]
                did_update = True

        if "setLowerBound" in params:
            if params["setLowerBound"] != lb_old:
                self.lower_bound = params["setLowerBound"]
                did_update = True
        if "setUpperBound" in params:
            if params["setUpperBound"] != ub_old:
                self.upper_bound = params["setUpperBound"]
                did_update = True

        if did_update:
            print(f"UPDATED COUNTSIM COMMAND: {command}")
            status = {
                "command@id": cmd_dict["id"],
                "statusCode": "COMPLETED",
                "message": "Command processed successfully",
            }
            self.controlstream.publish(json.dumps(status), APIResourceTypes.STATUS.value)

    def create_obs(self, count):
        ti = TimeInstant.now_as_time_instant().get_iso_time()

        obs = {
            "resultTime": ti,
            "phenomenonTime": ti,
            "result": {
                "count": count,
                "countDown": self.count_down,
                "step": self.step,
                "lowerBound": self.lower_bound,
                "upperBound": self.upper_bound
            }
        }

        return obs
