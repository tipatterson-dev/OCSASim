import copy
import json
import pickle
import time
from multiprocessing import Process, Queue
from threading import Thread
from typing import Literal

from oshconnect import OSHConnect, Node, System
from oshconnect.api_utils import URI, UCUMCode
from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.swe_components import DataRecordSchema, TimeSchema, CountSchema, BooleanSchema, VectorSchema, \
    QuantitySchema
from oshconnect.timemanagement import TimeInstant
from pydantic.v1.utils import to_lower_camel

from sims.sim import Sim


class LoBSim(Sim):
    interval: int = 0.5  # seconds
    raw_lob = 0.0  # Initial angle in degrees
    angle_step = 10.0  # Angle step in degrees per iteration

    def __init__(self, name: str, app: OSHConnect, node: Node):

        if name is None:
            name = "LoB Sim"

        super().__init__(name, app, node)

        self.ds_schema = DataRecordSchema(label="LoB Sim",
                                          description="A sim line of bearing simulation",
                                          definition="http://bottsinc.com/def/LineOfBearingSim", fields=[])
        timestamp = TimeSchema(label="Timestamp", name="timestamp", description="Timestamp of the GPS data",
                               definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
                               uom=URI(href="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"))
        location = VectorSchema(label="Location", name="location", description="GPS Location",
                                definition="http://www.opengis.net/def/property/OGC/0/SensorLocation",
                                reference_frame="http://www.opengis.net/def/crs/EPSG/0/4979",
                                coordinates=[QuantitySchema(label="Latitude", name="lat",
                                                            definition="http://sensorml.com/ont/swe/property/Latitude",
                                                            uom=UCUMCode(code='deg', label='degrees')),
                                             QuantitySchema(label="Longitude", name="lon",
                                                            definition="http://sensorml.com/ont/swe/property/Longitude",
                                                            uom=UCUMCode(code='deg', label='degrees')),
                                             QuantitySchema(label="Altitude", name="alt",
                                                            definition="http://sensorml.com/ont/swe/property/Altitude",
                                                            uom=UCUMCode(code='m', label='meters'))])

        raw_lob = QuantitySchema(label="Raw LoB", name="raw_lob", description="Signal Bearing - Deg from N",
                                 definition="http://sensorml.com/ont/swe/property/Bearing",
                                 uom=UCUMCode(code='deg', label='degrees'))

        self.ds_schema.fields.extend(
            [timestamp, location, raw_lob])

    def insert(self, system: System = None, datastream_schema: DataRecordSchema = None,
               controlstream: DataRecordSchema = None):
        """
               Inserts the default system and datastream into the node. Ignores the params from the super class for this sim.
               :param system:
               :param datastream_schema:
               :param ControlStream:
               :return:
               """
        self.system = System(name=to_lower_camel(self.name), label=self.name,
                             urn=f"urn:OCSASim:SimLoB:{self.name}", parent_node=self.node)
        self.node.add_system(self.system, self.node, True)

        self.datastream = self.system.add_insert_datastream(self.ds_schema)

    def simulation(self):
        counter_t = Thread(target=self.lob_sim())
        counter_t.start()

        while self.should_simulate:
            pass

        counter_t.join()

    def lob_sim(self):
        while self.should_simulate:
            timestamp = TimeInstant.now_as_time_instant().get_iso_time()
            lat = 37.7749  # Example latitude
            lon = -122.4194  # Example longitude
            alt = 30.0  # Example altitude in meters

            self.raw_lob = ((self.raw_lob + self.angle_step + 180.0) % 360.0) - 180.0

            observation = {
                "resultTime": timestamp,
                "phenomenonTime": timestamp,
                "result": {
                    "location": {
                        "lat": lat,
                        "lon": lon,
                        "alt": alt
                    },
                    "raw_lob": self.raw_lob
                }
            }

            print(f"LoB Observation: {observation}")

            self.datastream.publish(json.dumps(observation))

            time.sleep(self.interval)
