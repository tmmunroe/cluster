from src.node.neighborManager import NeighborManager
from src.node.nodeInfo import NodeInfo, NodeHealth
from src.proto.mesh_messages_pb2 import Gossip, NodeInfoProto
from src.proto.messageFactory import MessageFactory
from src.common.address import Address
from typing import Dict
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
        self.maintenanceInterval = 60

class GossipManager():
    def __init__(self,
            localNode: NodeInfo,
            gossip_addr: Address,
            gossip_socket: zmq.Socket,
            gossip_queue: asyncio.Queue,
            neighborManager: NeighborManager,
            zmqContext: zmq.asyncio.Context,
            gossipConfig=GossipConfiguration()):
        self.localNode = localNode
        self.gossip_addr = gossip_addr
        self.gossip_sock = gossip_socket
        self.gossip_queue = gossip_queue
        self.neighborManager = neighborManager
        self.zmqContext = zmqContext
        self.config = gossipConfig
        self.connectionConfig = gossipConfig.connectionConfig
        self.alreadyGossiping: Dict[str,float] = {}


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
        print("FLUSHING ALREADY GOSSIPING")
        receivedAtLimit = time.time() - self.config.alreadyGossipingRetention
        for key, receivedAt in self.alreadyGossiping.items():
            if receivedAt < receivedAtLimit:
                self.alreadyGossiping.pop(key)


    async def handle_gossip(self, data: str) -> None:
        '''decode msg'''
        gossip = MessageFactory.newFromString(data)
        if not gossip.Is(Gossip.DESCRIPTOR):
            print("ERROR: Received unexpected message on Gossip stream")
            return None
    
        '''unpack gossip'''
        should_gossip = self.consume_gossip(gossip)

        '''if any remaining sends, then put gossip on queue'''
        if gossip.remainingSends > 0 and should_gossip:
            gossip.remainingSends = gossip.remainingSends - 1
            await self.gossip_queue.put(gossip.SerializeToString())


    async def gossip_to(self, recipient:NodeInfo, data:str) -> None:
        print(f"GOSSIPING TO {recipient.name} : {recipient.gossip_addr}!!!!")
        addr = f"tcp://{recipient.gossip_addr}"
        sock = self.zmqContext.socket(zmq.PUSH)
        sock.connect(addr)
        await sock.send(data)
        sock.close()
        print(f"SENT GOSSIP TO {recipient.name} : {recipient.gossip_addr}!!!!")



    async def listen_for_gossip_loop(self) -> None:
        '''bind to pull socket, record gossip address, and then loop forever.. '''
        while True:
            data = await self.gossip_sock.recv()
            self.loop.create_task(self.handle_gossip(data))


    async def gossip_loop(self) -> None:
        '''loop forever..
        there should be some periodicity in sends so that if a lot of gossip exists,
        it can be packaged together... for now we just send one msg though'''
        while True:
            await asyncio.sleep(self.config.gossipInterval)
            '''create message'''
            data = await self.gossip_queue.get()
            self.gossip_queue.task_done()

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

    async def maintenance_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.maintenanceInterval)
            self.flushAlreadyGossiping()

    def start(self, loop: asyncio.BaseEventLoop) -> None:
        self.loop = loop
        self.loop.create_task(self.listen_for_gossip_loop())
        self.loop.create_task(self.gossip_loop())
        self.loop.create_task(self.maintenance_loop())
        print(f"Gossip engine started")
        return None