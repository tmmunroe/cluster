from src.node.neighborManager import NeighborManager
from src.node.nodeInfo import NodeInfo, NodeHealth
from src.message.messages_pb2 import Message, Gossip, NodeInfoProto
from src.message.messageFactory import MessageFactory
from src.node.address import Address
import zmq
import queue
import random
import asyncio
import time

class GossipConnectionConfiguration():
    def __init__(self):
        self.min_port = 59101
        self.max_port = 61101
        self.max_connect_retries = 5

class GossipConfiguration():
    def __init__(self, 
            connectionConfig = GossipConnectionConfiguration()):
        self.fanout = 2
        self.gossipInterval = 1
        self.connectionConfig = connectionConfig
        self.alreadyGossipingRetention = 120

class GossipManager():
    def __init__(self,
            loop,
            zmqContext,
            gossipQueue,
            neighborManager:NeighborManager,
            gossipConfig=GossipConfiguration()):
        self.localNode = None
        self.gossip_sock = None
        self.gossip_addr = None
        self.loop = loop
        self.zmqContext = zmqContext
        self.config = gossipConfig
        self.connectionConfig = gossipConfig.connectionConfig
        self.neighborManager = neighborManager
        self.gossipQueue = gossipQueue
        self.alreadyGossiping = {}
        self.setup_gossip_socket()

    def setLocalNodeInfo(self, nodeInfo: NodeInfo) -> None:
        self.localNode = nodeInfo
        return None

    async def start(self) -> None:
        self.loop.create_task(self.listen_for_gossip())
        self.loop.create_task(self.gossip())
        print(f"Gossip engine started")
        return None

    def setup_gossip_socket(self) -> None:
        self.gossip_sock = self.zmqContext.socket(zmq.PULL)
        port_selected = self.gossip_sock.bind_to_random_port('tcp://*',
                min_port=self.connectionConfig.min_port,
                max_port=self.connectionConfig.max_port,
                max_tries=self.connectionConfig.max_connect_retries)
        self.gossip_addr = Address('localhost', port_selected)        

    async def listen_for_gossip(self) -> None:
        '''bind to pull socket, record gossip address, and then loop forever.. '''
        while True:
            data = await self.gossip_sock.recv()
            self.loop.create_task(self.handle_gossip(data))


    async def handle_gossip(self, data: str) -> None:
        '''decode msg'''
        msg = MessageFactory.newFromString(data)

        '''unpack gossip'''
        gossip = Gossip()
        msg.message.Unpack(gossip)
        should_gossip = self.consume_gossip(gossip)

        '''if any remaining sends, then put gossip on queue'''
        if gossip.remainingSends > 0 and should_gossip:
            gossip.remainingSends = gossip.remainingSends - 1
            msg.message.Pack(gossip)
            await self.gossipQueue.put(msg.SerializeToString())

    def consume_gossip(self, gossip: Gossip) -> bool:
        '''returns a bool denoting whether or not
           to propagate this gossip'''
        if gossip.gossipId in self.alreadyGossiping:
            return False

        if gossip.message.Is(NodeInfoProto.DESCRIPTOR):
            nodeInfoProto = NodeInfoProto()
            gossip.message.Unpack(nodeInfoProto)
            nodeInfo = NodeInfo.fromProto(nodeInfoProto)
            self.neighborManager.updateWithNodeInfo(nodeInfo)

            '''if this is gossip about us, and it says we're not alive, we
                will not propagate this'''
            if nodeInfo.isSameNodeAs(self.localNode) and nodeInfo.health != NodeHealth.ALIVE:
                return False
        else:
            print(f"ERROR: unknown message type for msg {gossip}")
            return False

        self.alreadyGossiping[gossip.gossipId] = time.time()
        return True

    def flushAlreadyGossiping(self):
        print("FLUSHING ALREADYGOSSIPING")
        receivedAtLimit = time.time() - self.config.alreadyGossipingRetention
        for key, receivedAt in self.alreadyGossiping.items():
            if receivedAt < receivedAtLimit:
                self.alreadyGossiping.pop(key)


    async def gossip(self) -> None:
        '''loop forever..
        there should be some periodicity in sends so that if a lot of gossip exists,
        it can be packaged together... for now we just send one msg though'''
        while True:
            await asyncio.sleep(self.config.gossipInterval)
            '''create message'''
            data = await self.gossipQueue.get()
            self.gossipQueue.task_done()

            '''get recipients'''
            nodeInfos = [ nodeInfo for nodeInfo in self.neighborManager.getNodeInfos() 
                if nodeInfo.name != self.localNode.name ]
            if len(nodeInfos) == 0:
                print("Waiting for others to join cluster...")
                return None
            
            fanout = min(self.config.fanout, len(nodeInfos))
            recipients = random.sample(population=nodeInfos, k=fanout)
            '''create tasks to send messages to recipients'''
            for recipient in recipients:
                self.loop.create_task(self.gossip_to(recipient, data))


    async def gossip_to(self, recipient:NodeInfo, data:str) -> None:
        print(f"GOSSIPING TO {recipient.name} : {recipient.gossip_addr}!!!!")
        addr = f"tcp://{recipient.gossip_addr}"
        sock = self.zmqContext.socket(zmq.PUSH)
        sock.connect(addr)
        await sock.send(data)
        sock.close()
        print(f"SENT GOSSIP TO {recipient.name} : {recipient.gossip_addr}!!!!")
