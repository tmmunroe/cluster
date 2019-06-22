from src.node.address import Address
from src.node.nodeInfo import NodeInfo
from src.node.node import Node, NodeConfiguration
from src.cluster.cluster import ClusterConfiguration
from src.message.messageFactory import MessageFactory
from src.message.messages_pb2 import Message, NodeInfoProto
from src.node.neighborManager import NeighborManager
from src.service.service import ServiceManager
from src.gossip.gossipManager import GossipManager
from src.swim.swimProtocol import SwimManager
from src.mesh.mesh import Mesh, MeshFactory
import zmq
from zmq.asyncio import Context
from enum import Enum
import time
import uuid
import random
import asyncio
import concurrent.futures



class ManagerNode(Node):

    def start(self):
        print(f"Listening...")
        self.loop.create_task(self.gossipManager.start())
        self.loop.create_task(self.handleDirectMessages())
        self.loop.create_task(self.joinOrSetupCluster())
        self.loop.create_task(self.periodicallyReportNetworkView())
        self.loop.create_task(self.swimManager.start())
        self.loop.run_forever()

    async def joinOrSetupCluster(self):
        try:
            print(f"Trying to join cluster...")
            await asyncio.wait_for(self.join(self.clusterJoinAddr), 2)
        except concurrent.futures.TimeoutError:
            print(f"Unable to connect... Starting cluster...")
            self.loop.create_task(self.handleJoinRequests())
            self.loop.create_task(self.handleWorkerResponses())
            self.loop.create_task(self.handleClientRequests())


    async def handleJoinRequests(self):
        '''bind to cluster addr socket to handle joiners'''
        print(f"Listening for Join requests...")
        self.joinClusterNodeInfo = NodeInfo(self.clusterJoinAddr,
            'JoinEndpoint', 1)
        joinersAddr = f'tcp://*:{self.joinClusterNodeInfo.addr.port}'
        print(f"Binding to {joinersAddr}...")
        self.joiners = self.zmqContext.socket(zmq.ROUTER)
        self.joiners.bind(joinersAddr)
        while True:
            print("Listening for joiners")
            data = await self.joiners.recv_multipart()
            self.loop.create_task(self.handle_new_joiner(data))


    async def handle_new_joiner(self, data):
        routing_addr, empty, msg = data

        msg = MessageFactory.newFromString(msg)
        nodeInfo = NodeInfo.fromProto(msg.senderInfo)
        self.neighborManager.registerNode(nodeInfo)

        '''create full response message, serialize, and send'''
        joinAcceptMessage = MessageFactory.newJoinAcceptMessage(self.info, self.neighborManager.getNodeInfos())
        multipart_msg = [routing_addr, empty, joinAcceptMessage.SerializeToString()]
        await self.joiners.send_multipart(multipart_msg)


    async def handleWorkerResponses(self):
        '''bind to dealer address to send work to workers'''
        print(f"Listening for Worker responses...")
        self.workerNodeInfo = NodeInfo(self.clusterWorkAddr,'WorkEndpoint', 1)
        workerAddr = f'tcp://*:{self.workerNodeInfo.addr.port}'
        print(f"Binding to {workerAddr}...")
        self.workers = self.zmqContext.socket(zmq.DEALER)
        self.workers.bind(workerAddr)
        while True:
            print("Listening for worker responses")
            data = await self.workers.recv_multipart()
            await self.clients.send_multipart(data)

    async def handleClientRequests(self):
        '''bind to router address to handle client requests'''
        self.clientNodeInfo = NodeInfo(self.clusterClientAddr,'ClientEndpoint', 1)
        clientAddr = f'tcp://*:{self.clientNodeInfo.addr.port}'
        print(f"Binding to {clientAddr}...")
        self.clients = self.zmqContext.socket(zmq.ROUTER)
        self.clients.bind(clientAddr)
        while True:
            print("Listening for client requests")
            data = await self.clients.recv_multipart()
            await self.workers.send_multipart(data)




class ManagerNodeFactory():
    @classmethod
    def newManagerOld(self):
        loop = asyncio.get_event_loop()
        zmqContext = zmq.asyncio.Context.instance()
        gossipQueue = asyncio.Queue(loop=loop)

        neighborManager = NeighborManager()
        gossipManager = GossipManager(loop, zmqContext, gossipQueue, neighborManager)
        serviceManager = ServiceManager()
        swimManager = SwimManager(neighborManager, zmqContext, loop)

        return ManagerNode( loop, 
                            zmqContext,
                            neighborManager, 
                            gossipManager,
                            swimManager,
                            serviceManager )

    @classmethod
    def newManager(self):
        mesh = MeshFactory.newMesh()
        return ManagerNode(mesg)