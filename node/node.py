import zmq.asyncio
import asyncio
import uuid

from src.common.address import Address
from src.mesh.mesh import Mesh
from src.mesh.networkView import NetworkView
from src.cluster.cluster import ClusterConfiguration
from src.message.messageFactory import MessageFactory
from src.message.messages_pb2 import Message, NodeInfoProto, JoinAccept
from src.service.serviceManager import ServiceManagerAPI, ServiceNotFound
from src.service.serviceAPI import ServiceAPI, ServiceSpecification


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
            eventLoop: asyncio.AbstractEventLoop,
            mesh: Mesh,
            serviceManager: ServiceManagerAPI,
            config = NodeConfiguration()):
        '''set basic resources'''
        self.loop = eventLoop
        self.serviceManager = serviceManager
        self.mesh = mesh

        '''unpack cluster info from config'''
        self.connectionConfig = config.connectionConfig
        self.clusterJoinAddr = config.clusterConfig.joinAddr
        self.clusterWorkAddr = config.clusterConfig.workAddr
        self.clusterClientAddr = config.clusterConfig.clientAddr


    async def join(self, cluster: Address) -> Message:
        joinMsg = MessageFactory.newJoinRequestMessage(self.mesh.localNode).SerializeToString()
        zmqContext = zmq.asyncio.Context.instance()
        joinSocket = zmqContext.socket(zmq.REQ)
        joinSocket.connect(f'tcp://{self.clusterJoinAddr}')

        await joinSocket.send(joinMsg)
        data = await joinSocket.recv()
        respMsg = MessageFactory.newFromString(data)
        joinAccept = JoinAccept()
        respMsg.message.Unpack(joinAccept)
        self.mesh.neighborManager.updateWithNetworkView( NetworkView.fromProto(joinAccept.networkView) )
        return respMsg


    async def leave(self, cluster: Address) -> None:
        pass


    def addService(self, serviceSpec: ServiceSpecification):
        self.serviceManager.addService(serviceSpec)


    def startService(self, serviceName: str, loop: asyncio.AbstractEventLoop):
        if not self.serviceManager.offersService(serviceName):
            raise ServiceNotFound(f"Service {serviceName} is not available on this node")
        self.serviceManager.launchService(serviceName, loop)
    

    def start(self):
        self.mesh.start(self.loop)


