from src.common.address import Address
from src.node.nodeInfo import NodeInfo
from src.node.node import Node, NodeConfiguration
from src.cluster.cluster import ClusterConfiguration
from proto.factory.messageFactory import MessageFactory
from proto.build.mesh_messages_pb2 import NodeInfoProto
from src.node.neighborManager import NeighborManager
from src.service.service import ServiceManager
import zmq
from zmq.asyncio import Context
from enum import Enum
import time
import uuid
import random
import asyncio


class ClientNode(Node):
    def __init__(self, 
            loop,
            serviceManager: ServiceManager,
            config=NodeConfiguration() ):
        self.serviceManager = serviceManager

        self.connectionConfig = config.connectionConfig
        self.clusterJoinAddr = config.clusterConfig.joinAddr
        self.clusterWorkAddr = config.clusterConfig.workAddr
        self.clusterClientAddr = config.clusterConfig.clientAddr

        '''set up local node info'''
        self.info = NodeInfo(config.addr, config.name, 1)
        self.setup_direct_messaging_sock()

    def setup_direct_messaging_sock(self) -> None:
        '''bind to direct messaging port'''
        zmqContext = zmq.asyncio.Context.instance()
        self.zmqSocket = zmqContext.socket(zmq.ROUTER)
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
        print(f"Bound to address {zmqAddr}")
        return None



    def start(self):
        clientAddr = f'tcp://{self.clusterClientAddr.host}:{self.clusterClientAddr.port}'
        print(f"Connecting to {clientAddr}")
        zmqContext = zmq.asyncio.Context.instance()
        clientSock = zmqContext.socket(zmq.REQ)
        clientSock.connect(clientAddr)

        print(f"Starting to send messages...")
        while True:
            time.sleep(1)
            reqStr = str(uuid.uuid4())
            reqMsg = MessageFactory.newEchoRequest(self.info, reqStr)
            print(f"Sending {reqMsg}")
            clientSock.send(reqMsg.SerializeToString())

            print("Waiting for response...")
            data = clientSock.recv()
            msg = MessageFactory.fromString(data)
            echo_msg = self.serviceManager.unpackServiceResponse(msg.message)
            print(f"Received: : {echo_msg}")

            intA = random.randint(1, 100)
            intB = random.randint(101, 200)
            reqMsg = MessageFactory.newAddRequest(self.info, intA, intB)
            print(f"Sending {reqMsg}")
            clientSock.send(reqMsg.SerializeToString())

            print("Waiting for response...")
            data = clientSock.recv()
            msg = MessageFactory.fromString(data)
            add_msg = self.serviceManager.unpackServiceResponse(msg.message)
            print(f"Received: {add_msg}")



class ClientNodeFactory():
    @classmethod
    def newClient(self):
        loop = asyncio.get_event_loop()
        serviceManager = ServiceManager()

        return ClientNode( loop, serviceManager )