import math
import multiprocessing
import time
import uuid

from oshconnect import OSHConnect, Node, System, Datastream
from oshconnect.datamodels.api_utils import URI, UCUMCode
from oshconnect.datamodels.swe_components import TimeSchema, VectorSchema, QuantitySchema
from oshconnect.osh_connect_datamodels import DataRecordSchema
from oshconnect.timemanagement import TimeInstant
from pydantic.v1.utils import to_lower_camel


class GPSSim:
    name: str
    app: OSHConnect
    node: Node
    system: System
    datastream: Datastream
    should_simulate: bool
    # It's best to expect this to misbehave at extreme latitudes
    base_lat = 34.7304  # Huntsville latitude
    base_lon = -86.5861  # Huntsville longitude
    base_alt = 200.0  # Example: base altitude in meters for Huntsville
    radius_deg = 0.003  # ~111m per 0.001 deg latitude
    alt_variation = 10.0  # Altitude varies +/-10m
    angle = 0.0
    angle_step = math.radians(5)  # 5 degree step per iteration

    def __init__(self, name: str, app: OSHConnect, node: Node):
        self.name = name
        self.app = app
        self.node = node
        self._uuid = uuid.uuid1()
        self.system = None
        self.datastream = None

    def insert(self):
        if self.system is None:
            self.system = System(name=to_lower_camel(self.name), label=self.name, urn=f"urn:OCSASim:GPS:111")
            # self.system = System(name=to_lower_camel(self.name), label=self.name, urn=f"urn:OCSASim:GPS:{self._uuid}")

        self.app.add_system(self.system, self.node, True)
        datastream_schema = DataRecordSchema(label="GPS Simulated Location",
                                             description="GPS Simulated Location",
                                             definition="http://sensorml.com/ont/swe/property/Position", fields=[])
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

        orientation = QuantitySchema(label="Orientation", name="orientation", description="GPS Orientation - Heading",
                                     definition="http://sensorml.com/ont/swe/property/Orientation",
                                     uom=UCUMCode(code='deg', label='degrees'))
        datastream_schema.fields.append(timestamp)
        datastream_schema.fields.append(location)
        datastream_schema.fields.append(orientation)

        self.datastream = self.system.add_insert_datastream(datastream_schema)

        if self.datastream is None:
            return False
        else:
            return True

    def start(self):
        """
        Simulates the GPS data until the sim is set to "stop simulating".
        :return:
        """
        self.should_simulate = True

        return multiprocessing.Process(target=self.simulation(), args=(self,))

    def stop(self):
        self.should_simulate = False

    def simulation(self):
        print("Starting inner GPS simulation process")
        while self.should_simulate:
            timeinstant = TimeInstant.now_as_time_instant()
            timedata = timeinstant.get_iso_time()
            # Calculate point on circle
            lat = self.base_lat + self.radius_deg * math.cos(self.angle)
            lon = self.base_lon + self.radius_deg * math.sin(self.angle)
            alt = self.base_alt + self.alt_variation * math.sin(self.angle)
            self.angle += self.angle_step

            gps_sim_data = {
                "resultTime": timedata,
                "phenomenonTime": timedata,
                "result": {
                    # "timestamp": timeinstant.epoch_time,
                    "location": {
                        "lat": lat,
                        "lon": lon,
                        "alt": alt
                    },
                    "orientation": 180.0
                }
            }

            self.datastream.insert_observation_dict(gps_sim_data)
            time.sleep(2)  # Wait 2 seconds before next iteration