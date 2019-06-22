from src.node.address import Address
from src.node.node import Node, NodeConfiguration
from src.cluster.cluster import ClusterConfiguration
from src.message.messageFactory import MessageFactory
from src.message.messages_pb2 import Message, NodeInfoProto, ServiceRequest, ServiceResponse
from src.mesh.nodeInfo import NodeInfo
from src.mesh.mesh import Mesh, MeshFactory
from src.service.service import ServiceManager
import zmq
from zmq.asyncio import Context
from enum import Enum
from concurrent.futures import ProcessPoolExecutor
import time
import uuid
import random
import asyncio

class WorkerNode(Node):
    def __init__(self,
            eventLoop: asyncio.BaseEventLoop,
            zmqContext: zmq.Context,
            mesh: Mesh,
            serviceManager: ServiceManager,
            config = NodeConfiguration(),
            executor=ProcessPoolExecutor()):
        super().__init__(eventLoop,
            zmqContext,
            mesh,
            serviceManager,
            config)
        self.executor = executor       


    def start(self) -> None:
        print(f"Starting node...")
        super().start()
        self.loop.create_task(self.join_to_cluster(self.clusterJoinAddr))
        self.loop.create_task(self.handle_work_loop(self.clusterWorkAddr))
        self.loop.run_forever()


    async def join_to_cluster(self, clusterAddr: Address) -> None:
        print(f"Attempting to join cluster {clusterAddr}...")
        respMsg = await asyncio.wait_for(self.join(clusterAddr), 2)
        if not respMsg:
            print(f"Could not connect to cluster...")
        else:
            print(f"Successfully joined cluster...")
        return None
        

    async def handle_work_loop(self, workAddr: Address):
        '''connect to worker queue'''
        workZMQAddr = f'tcp://{workAddr.host}:{workAddr.port}'
        print(f"Connecting to work queue {workZMQAddr}...")
        self.work = self.zmqContext.socket(zmq.REP)
        self.work.connect(workZMQAddr)
        while True:
            packet = await self.work.recv()
            self.loop.create_task(self.handle_work_item(self.work, packet))


    async def handle_work_item(self, work, packet:str):
        print(f"RECEIVED: {packet}")
        '''unpack wrapper messages'''
        msg = MessageFactory.newFromString(packet)
        serviceResponse = self.serviceManager.handleServiceRequest(msg.message)
        msg.message.Pack(serviceResponse)
        await work.send(msg.SerializeToString())


class WorkerNodeFactory():
    @classmethod
    def newWorker(self) -> WorkerNode:
        loop = asyncio.get_event_loop()
        zmqContext = zmq.asyncio.Context.instance()
        
        mesh = MeshFactory.newMesh()
        serviceManager = ServiceManager()

        return WorkerNode( loop, 
                            zmqContext,
                            mesh,
                            serviceManager )
