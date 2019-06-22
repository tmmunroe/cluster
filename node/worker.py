from src.node.address import Address
from src.node.nodeInfo import NodeInfo
from src.node.node import Node, NodeConfiguration
from src.cluster.cluster import ClusterConfiguration
from src.message.messageFactory import MessageFactory
from src.message.messages_pb2 import Message, NodeInfoProto, ServiceRequest, ServiceResponse
from src.node.neighborManager import NeighborManager
from src.service.service import ServiceManager
from src.gossip.gossipManager import GossipManager
from src.swim.swimProtocol import SwimManager
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
            neighborManager: NeighborManager,
            gossipManager: GossipManager,
            swimManager: SwimManager,
            serviceManager: ServiceManager,
            config = NodeConfiguration(),
            executor=ProcessPoolExecutor()):
        super().__init__(eventLoop,
            zmqContext,
            neighborManager,
            gossipManager,
            swimManager,
            serviceManager,
            config)
        self.executor = executor       


    def start(self):
        '''attempt to join cluster'''
        print(f"Attempting to join cluster...")
        self.loop.create_task(self.join_to_cluster(self.clusterJoinAddr))
        self.loop.create_task(self.gossipManager.start())
        self.loop.create_task(self.handleDirectMessages())
        self.loop.create_task(self.handleWork(self.clusterWorkAddr))
        self.loop.create_task(self.periodicallyReportNetworkView())
        self.loop.create_task(self.swimManager.start())
        self.loop.run_forever()

    async def join_to_cluster(self, clusterAdd: Address):
        respMsg = await asyncio.wait_for(self.join(self.clusterJoinAddr), 2)
        if not respMsg:
            print(f"Could not connect to cluster...")
            return None
        else:
            print(f"Successfully joined cluster")
        

    async def handleWork(self, workAddr: Address):
        '''connect to worker queue'''
        print(f"Connecting to work queue...")
        workZMQAddr = f'tcp://{workAddr.host}:{workAddr.port}'
        print(f"Connecting to {workZMQAddr}...")
        self.work = self.zmqContext.socket(zmq.REP)
        self.work.connect(workZMQAddr)
        while True:
            packet = await self.work.recv()
            self.loop.create_task(self.doWork(self.work, packet))


    async def doWork(self, work, packet:str):
        print(f"RECEIVED: {packet}")
        '''unpack wrapper messages'''
        msg = MessageFactory.newFromString(packet)
        serviceResponse = self.serviceManager.handleServiceRequest(msg.message)
        msg.message.Pack(serviceResponse)
        await work.send(msg.SerializeToString())




class WorkerNodeFactory():
    @classmethod
    def newWorker(self):
        loop = asyncio.get_event_loop()
        zmqContext = zmq.asyncio.Context.instance()
        gossipQueue = asyncio.Queue(loop=loop)

        neighborManager = NeighborManager()
        gossipManager = GossipManager(loop, zmqContext, gossipQueue, neighborManager)
        serviceManager = ServiceManager()
        swimManager = SwimManager(neighborManager, zmqContext, loop)

        return WorkerNode( loop, 
                            zmqContext,
                            neighborManager, 
                            gossipManager,
                            swimManager,
                            serviceManager )
