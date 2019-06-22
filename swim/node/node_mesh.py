from src.node.address import Address
from src.node.nodeInfo import NodeInfo
from src.cluster.cluster import ClusterConfiguration
from src.message.messageFactory import MessageFactory
from src.message.messages_pb2 import Message, NodeInfoProto, Gossip, JoinAccept, Ping, PingReq, Ack
from src.service.service import ServiceManager
from src.mesh.mesh import Mesh
from src.mesh.networkView import NetworkView
import zmq
from zmq.asyncio import Context
from enum import Enum
import time
import uuid
import random
import asyncio
import functools



class NodeConnectionConfiguration():
    def __init__(self):
        self.min_port = 59101
        self.max_port = 61101
        self.max_connect_retries = 5

class NodeConfiguration():
    def __init__(self,
            clusterConfig = ClusterConfiguration(), 
            connectionConfig=NodeConnectionConfiguration()):
        self.name = 'Node_' + str(uuid.uuid4())
        self.addr = Address('localhost', None)
        self.clusterConfig = clusterConfig
        self.connectionConfig = connectionConfig


class Node():
    def __init__(self,
            eventLoop: asyncio.BaseEventLoop,
            zmqContext: zmq.Context,
            mesh: Mesh,
            serviceManager: ServiceManager,
            config = NodeConfiguration()):
        '''set basic resources'''
        self.zmqContext = zmqContext
        self.loop = eventLoop
        self.serviceManager = serviceManager
        self.mesh = mesh

        '''unpack cluster info from config'''
        self.connectionConfig = config.connectionConfig
        self.clusterJoinAddr = config.clusterConfig.joinAddr
        self.clusterWorkAddr = config.clusterConfig.workAddr
        self.clusterClientAddr = config.clusterConfig.clientAddr

    async def join(self, cluster: Address) -> Message:
        joinMsg = MessageFactory.newJoinRequestMessage(self.info).SerializeToString()
        joinSocket = self.zmqContext.socket(zmq.REQ)
        joinSocket.connect(f'tcp://{self.clusterJoinAddr}')

        await joinSocket.send(joinMsg)
        data = await joinSocket.recv()
        respMsg = MessageFactory.newFromString(data)
        joinAccept = JoinAccept()
        respMsg.message.Unpack(joinAccept)
        self.mesh.neighborManager.updateWithNetworkViewMessage( NetworkView.fromProto(joinAccept.networkView) )
        return respMsg


    async def sendGreeting(self, nodeInfo: NodeInfo):
        sock = self.zmqContext.socket(zmq.REQ)
        sock.connect(f'tcp://{nodeInfo.addr}')
        msg = MessageFactory.newPingMessage(self.info)
        #print(f"Sending to peer {nodeInfo.addr}")
        await sock.send(msg.SerializeToString())
        #print(f"Waiting for response from peer...")
        data = await sock.recv()
        respMsg = MessageFactory.newFromString(data)
        #print(f"Received response from peer: {respMsg}")


    async def handleDirectMessages(self):
        '''bind to direct messaging port'''
        self.zmqSocket = self.zmqContext.socket(zmq.ROUTER)
        if self.info.addr.port is None:
            '''bind to random port and then set address port'''
            port_selected = self.zmqSocket.bind_to_random_port('tcp://*',
                min_port=self.connectionConfig.min_port,
                max_port=self.connectionConfig.max_port,
                max_tries=self.connectionConfig.max_connect_retries)
            self.info.addr.port = port_selected
            zmqAddr = f'tcp://*:{self.info.addr.port}'
        else:
            zmqAddr = f'tcp://*:{self.info.addr.port}'
            self.zmqSocket.bind(zmqAddr)

        print(f"Listening on {zmqAddr}...")

        '''report self to neighbor manager'''
        self.neighborManager.registerNode(self.info)

        '''handle incoming'''
        while True:
            zmqAddress, empty, data = await self.zmqSocket.recv_multipart()
            self.loop.create_task(self.handle_direct_message(zmqAddress, empty, data))


    async def handle_direct_message(self, zmqAddress, empty, data:str):
        msg = MessageFactory.newFromString(data)
        #print(f"Handling direct message: {msg}")

        if msg.message.Is(Ping.DESCRIPTOR):
            self.loop.create_task(self.ack_message(zmqAddress, empty, msg))
        elif msg.message.Is(PingReq.DESCRIPTOR):
            self.loop.create_task(self.handle_ping_req(zmqAddress, empty, msg))
        else:
            print("Unknown message type...")

    async def ack_message(self, zmqAddress, empty, msg: Message) -> None:
        #print(f"Sending ACK message")
        ackMsg = MessageFactory.newAckMessage(self.info)
        multipart = [ zmqAddress, empty, ackMsg.SerializeToString() ]
        await self.zmqSocket.send_multipart(multipart)
        return None

    async def handle_ping_req(self, zmqAddress, empty, msg: Message):
        print(f"handle_ping_req not implemented")
        pass


    async def periodicallyReportNetworkView(self):
        while True:
            await asyncio.sleep(5)
            self.neighborManager.reportState()


    def nextIncarnation(self):
        return self.info.nextIncarnation()

    def start(self):
        pass


