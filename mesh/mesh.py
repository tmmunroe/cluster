from src.mesh.gossipManager import GossipManager
from src.mesh.neighborManager import NeighborManager
from src.mesh.swimManager import SwimManager
from src.mesh.nodeInfo import NodeInfo
from src.message.messageFactory import MessageFactory
from src.node.address import Address
from src.mesh.networkView import NetworkView
from typing import Sequence
import asyncio
import zmq.asyncio
import uuid

class MeshConnectionConfiguration():
    def __init__(self):
        self.min_port = 59101
        self.max_port = 61101
        self.max_connect_retries = 5

class Mesh():
    def __init__(self,
            localNode: NodeInfo,
            neighborManager: NeighborManager,
            gossipManager: GossipManager,
            swimManager: SwimManager):
        self.localNode = localNode
        self.neighborManager = neighborManager
        self.gossipManager = gossipManager
        self.swimManager = swimManager

        self.gossipManager.setLocalNodeInfo(self.localNode)
        self.neighborManager.setLocalNodeInfo(self.localNode)
        self.swimManager.setLocalNodeInfo(self.localNode)

        self.neighborManager.newNodeDelegates.append(self.gossipNewNode)
        self.neighborManager.nodeHealthChangeDelegates.append(self.gossipNodeHealthChange)
        self.neighborManager.refuteNotAliveDelegates.append(self.refuteNotAlive)

    def start(self, loop: asyncio.BaseEventLoop):
        self.loop = loop
        self.neighborManager.start(self.loop)
        self.gossipManager.start(self.loop)
        self.swimManager.start(self.loop)
        self.loop.run_forever()

    def updateWithNetworkView(self, networkView: NetworkView):
        self.neighborManager.updateWithNetworkView(networkView)

    async def gossipNewNode(self, nodeInfo: NodeInfo) -> None:
        msg, gossipMsg = MessageFactory.newGossipMessage(self.localNode)
        nodeInfoProto = nodeInfo.toProto()
        gossipMsg.message.Pack(nodeInfoProto)
        msg.message.Pack(gossipMsg)
        await self.gossipManager.gossip_queue.put(msg.SerializeToString())
        return None

    async def gossipNodeHealthChange(self, nodeInfo: NodeInfo) -> None:
        print(f"GOSSIPING ABOUT NODE HEALTH CHANGE: {nodeInfo.name} : {nodeInfo.addr} {nodeInfo.health.name}")
        msg, gossipMsg = MessageFactory.newGossipMessage(self.localNode)
        nodeInfoProto = nodeInfo.toProto()
        gossipMsg.message.Pack(nodeInfoProto)
        msg.message.Pack(gossipMsg)
        await self.gossipManager.gossip_queue.put(msg.SerializeToString())
        print("GOSSIP ABOUT NODE HEALTH CHANGE WAS ENQUEUED!!!")
        return None

    async def refuteNotAlive(self) -> None:
        self.localNode.nextIncarnation()
        msg, gossipMsg = MessageFactory.newGossipMessage(self.localNode)
        nodeInfoProto = self.localNode.toProto()
        gossipMsg.message.Pack(nodeInfoProto)
        msg.message.Pack(gossipMsg)
        await self.gossipManager.gossip_queue.put(msg.SerializeToString())
        print("REFUTED NOT ALIVE REPORT")
        return None

    def registerNode(self, nodeInfo: NodeInfo) -> None:
        self.neighborManager.registerNode(nodeInfo)
        return None

    def getNodeInfos(self) -> Sequence[NodeInfo]:
        return self.neighborManager.getNodeInfos()



class MeshFactory():
    @classmethod
    def newMesh(cls, 
            meshConnectionConfig: MeshConnectionConfiguration = MeshConnectionConfiguration() ):
        loop = asyncio.get_event_loop()
        zmqContext = zmq.asyncio.Context.instance()
        
        basic_sock = zmqContext.socket(zmq.ROUTER)
        bound_port = basic_sock.bind_to_random_port(
            "tcp://*",
            min_port = meshConnectionConfig.min_port,
            max_port = meshConnectionConfig.max_port,
            max_retries= meshConnectionConfig.max_connect_retries )
        addr = Address("localhost", bound_port)


        gossip_sock = zmqContext.socket(zmq.PULL)
        gossip_port = basic_sock.bind_to_random_port(
            "tcp://*",
            min_port = meshConnectionConfig.min_port,
            max_port = meshConnectionConfig.max_port,
            max_retries= meshConnectionConfig.max_connect_retries )
        gossip_addr = Address("localhost", gossip_port)


        swim_sock = zmqContext.socket(zmq.ROUTER)
        swim_port = basic_sock.bind_to_random_port(
            "tcp://*",
            min_port = meshConnectionConfig.min_port,
            max_port = meshConnectionConfig.max_port,
            max_retries= meshConnectionConfig.max_connect_retries )
        swim_addr = Address("localhost", swim_port)

        gossip_queue = asyncio.Queue(loop=loop)

        node_name = 'Node_' + str(uuid.uuid4())
        nodeInfo = NodeInfo(addr=addr, 
            name=node_name, 
            incarnation=1, 
            gossip_addr=gossip_addr,
            swim_addr=swim_addr)

        neighborManager = NeighborManager()
        gossipManager = GossipManager(
            localNode = nodeInfo,
            gossip_addr = gossip_addr,
            gossip_socket = gossip_sock, 
            gossip_queue = gossip_queue, 
            neighborManager = neighborManager,
            zmqContext = zmqContext )
        swimManager = SwimManager(
            localNode = nodeInfo,
            swim_addr = swim_addr,
            swim_sock = swim_sock,
            neighborManager = neighborManager,
            zmqContext = zmqContext )

        return Mesh(
            localNode = nodeInfo,
            neighborManager = neighborManager,
            gossipManager = gossipManager,
            swimManager = swimManager)