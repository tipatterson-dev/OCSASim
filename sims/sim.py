import asyncio
import uuid
from abc import ABC, abstractmethod
# from multiprocessing import Process
from threading import Thread

from oshconnect import OSHConnect, Node, System, Datastream
from oshconnect.streamableresource import StreamableModes, ControlStream
from oshconnect.swe_components import DataRecordSchema


class Sim(ABC):
    """
    A base class for a simulation.
    """
    name: str
    app: OSHConnect
    node: Node
    system: System
    datastream: Datastream
    controlstream: ControlStream
    should_simulate: bool = False
    thread: Thread = None

    def __init__(self, name: str, app: OSHConnect, node: Node):
        self.name = name
        self.app = app
        self.node = node
        self._uuid = uuid.uuid1()
        self.system = None
        self.datastream = None
        self.controlstream = None
        self.thread = None

    def insert(self, system: System, datastream_schema: DataRecordSchema, ControlStream: DataRecordSchema = None):
        """
        Inserts the included system and datastream into the node.
        :return:
        """
        self.node.add_system(system, self.node, True)
        self.datastream = self.system.add_insert_datastream(datastream_schema)
        self.datastream.set_connection_mode(StreamableModes.BIDIRECTIONAL)

        if ControlStream is not None:
            self.controlstream = self.system.add_and_insert_control_stream(ControlStream)
            self.controlstream.set_connection_mode(StreamableModes.BIDIRECTIONAL)

    def start(self):
        self.should_simulate = True
        self.datastream.initialize()
        self.datastream.start()

        if self.controlstream is not None:
            self.controlstream.initialize()
            self.controlstream.start()

        # self.process = Process(target=self.run_sim)
        self.thread = Thread(target=self.simulation)
        self.thread.start()

    def stop(self):
        self.should_simulate = False
        self.thread.join()

    @abstractmethod
    def simulation(self):
        """
        The main simulation loop. This method should be implemented by subclasses.
        :return:
        """
        pass

    def run_sim(self):
        # loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        # loop.run_until_complete(self.simulation())
        pass
