from src.node.address import Address
from src.mesh.mesh import Mesh
from src.mesh.networkView import NetworkView
from src.mesh.nodeInfo import NodeInfo
from src.cluster.cluster import ClusterConfiguration
from src.message.messageFactory import MessageFactory
from src.message.messages_pb2 import Message, NodeInfoProto, JoinAccept
from src.service.service import ServiceManager
from enum import Enum
import zmq
import zmq.asyncio
import uuid
import asyncio


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
            zmqContext: zmq.asyncio.Context,
            mesh: Mesh,
            serviceManager: ServiceManager,
            config = NodeConfiguration()):
        '''set basic resources'''
        self.loop = eventLoop
        self.zmqContext = zmqContext
        self.mesh = mesh
        self.serviceManager = serviceManager

        '''unpack cluster info from config'''
        self.connectionConfig = config.connectionConfig
        self.clusterJoinAddr = config.clusterConfig.joinAddr
        self.clusterWorkAddr = config.clusterConfig.workAddr
        self.clusterClientAddr = config.clusterConfig.clientAddr

    async def join(self, cluster: Address) -> Message:
        joinMsg = MessageFactory.newJoinRequestMessage(self.mesh.localNode).SerializeToString()
        joinSocket = self.zmqContext.socket(zmq.REQ)
        joinSocket.connect(f'tcp://{self.clusterJoinAddr}')

        await joinSocket.send(joinMsg)
        data = await joinSocket.recv()
        respMsg = MessageFactory.newFromString(data)
        joinAccept = JoinAccept()
        respMsg.message.Unpack(joinAccept)
        self.mesh.neighborManager.updateWithNetworkViewMessage( NetworkView.fromProto(joinAccept.networkView) )
        return respMsg

    async def leave(self, cluster: Address) -> None:
        pass

    def start(self):
        self.mesh.start(self.loop)


