from src.node.address import Address
from src.mesh.nodeInfo import NodeInfo
from src.node.node import Node, NodeConfiguration
from src.cluster.cluster import ClusterConfiguration
from src.message.messageFactory import MessageFactory
from src.message.messages_pb2 import Message, NodeInfoProto
from src.service.service import ServiceManager
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

    def start(self) -> None:
        super().start()
        print(f"Listening...")
        self.loop.create_task(self.joinOrSetupCluster())
        self.loop.run_forever()
        return None

    async def joinOrSetupCluster(self) -> None:
        try:
            print(f"Trying to join cluster...")
            await asyncio.wait_for(self.join(self.clusterJoinAddr), 2)
        except concurrent.futures.TimeoutError:
            print(f"Unable to connect... Starting cluster...")
            self.loop.create_task(self.handle_join_requests())
            self.loop.create_task(self.handle_worker_responses())
            self.loop.create_task(self.handle_client_requests())
        return None


    async def handle_join_requests(self) -> None:
        '''bind to cluster addr socket to handle joiners'''
        joinersAddr = f'tcp://*:{self.clusterJoinAddr.port}'
        print(f"Binding to {joinersAddr}...")
        self.joiners = self.zmqContext.socket(zmq.ROUTER)
        self.joiners.bind(joinersAddr)
        while True:
            print("Listening for joiners")
            data = await self.joiners.recv_multipart()
            self.loop.create_task(self.handle_new_joiner(data))
        return None


    async def handle_new_joiner(self, data) -> None:
        routing_addr, empty, msg = data

        msg = MessageFactory.newFromString(msg)
        nodeInfo = NodeInfo.fromProto(msg.senderInfo)
        self.mesh.registerNode(nodeInfo)

        '''create full response message, serialize, and send'''
        joinAcceptMessage = MessageFactory.newJoinAcceptMessage(self.mesh.localNode, self.mesh.getNodeInfos())
        multipart_msg = [routing_addr, empty, joinAcceptMessage.SerializeToString()]
        await self.joiners.send_multipart(multipart_msg)
        return None


    async def handle_worker_responses(self):
        '''bind to dealer address to send work to workers'''
        print(f"Listening for Worker responses...")
        workerAddr = f'tcp://*:{self.clusterWorkAddr.port}'
        print(f"Binding to {workerAddr}...")
        self.workers = self.zmqContext.socket(zmq.DEALER)
        self.workers.bind(workerAddr)
        while True:
            print("Listening for worker responses")
            data = await self.workers.recv_multipart()
            await self.clients.send_multipart(data)


    async def handle_client_requests(self):
        '''bind to router address to handle client requests'''
        clientAddr = f'tcp://*:{self.clusterClientAddr.port}'
        print(f"Binding to {clientAddr}...")
        self.clients = self.zmqContext.socket(zmq.ROUTER)
        self.clients.bind(clientAddr)
        while True:
            print("Listening for client requests")
            data = await self.clients.recv_multipart()
            await self.workers.send_multipart(data)




class ManagerNodeFactory():

    @classmethod
    def newManager(self) -> ManagerNode:
        loop = asyncio.get_event_loop()
        zmqContext = zmq.asyncio.Context.instance()

        mesh = MeshFactory.newMesh()
        serviceManager = ServiceManager()


        return ManagerNode( loop, 
                            zmqContext,
                            mesh,
                            serviceManager )