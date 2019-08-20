from src.mesh.gossipManager import GossipManager
from src.mesh.neighborManager import NeighborManager
from src.mesh.swimManager import SwimManager
from src.mesh.nodeInfo import NodeInfo, NodeHealth
from src.mesh.messageFactory import MeshMessageFactory
from src.common.address import Address
from src.mesh.networkView import NetworkView
from typing import Sequence, ValuesView
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
            swimManager: SwimManager,
            nodeInfo_queue: asyncio.Queue):
        self.localNode = localNode
        self.neighborManager = neighborManager
        self.gossipManager = gossipManager
        self.swimManager = swimManager
        self.nodeInfo_queue = nodeInfo_queue

        self.gossipManager.setLocalNodeInfo(self.localNode)
        self.neighborManager.setLocalNodeInfo(self.localNode)
        self.swimManager.setLocalNodeInfo(self.localNode)

    def start(self, loop: asyncio.BaseEventLoop):
        self.loop = loop
        self.neighborManager.start(self.loop)
        self.gossipManager.start(self.loop)
        self.swimManager.start(self.loop)
        print(f"Mesh started")
        
    def getNodeInfos(self) -> ValuesView:
        return self.neighborManager.getNodeInfos()

    def getNetworkView(self) -> NetworkView:
        return self.neighborManager.getNetworkView()


    def updateWithNetworkView(self, networkView: NetworkView):
        for _,nodeInfo in networkView.items():
            self.updateWithNodeInfo(nodeInfo)
        return None


    def updateWithNodeInfo(self, nodeInfo:NodeInfo) -> None:
        print(f"UPDATING NODE INFO: {nodeInfo.name} {nodeInfo.addr}")
        if nodeInfo.isSameNodeAs(self.localNode):
            if nodeInfo.health != NodeHealth.ALIVE:
                print(f"REFUTING NOT ALIVE")
                self.refuteNotAlive()
        else:
            myNodeInfo = self.neighborManager.getNodeInfo(nodeInfo.name)
            if myNodeInfo is None:
                print(f"REGISTERING NODE")
                self.registerNode(nodeInfo)
            else:
                print(f"SETTING NODE HEALTH")
                self.setNodeHealthFromNodeInfo(nodeInfo)
        return None


    async def gossipNodeHealthChange(self, nodeInfo: NodeInfo) -> None:
        print(f"GOSSIPING ABOUT NODE HEALTH CHANGE: {nodeInfo.name} : {nodeInfo.addr} {nodeInfo.health.name}")
        await self.gossipAboutNode(nodeInfo)
        print("GOSSIP ABOUT NODE HEALTH CHANGE WAS ENQUEUED!!!")
        return None

    async def gossipAboutNode(self, nodeInfo: NodeInfo) -> None:
        "always use local node as sender"
        gossipMsg = MeshMessageFactory.newGossipMessage(self.localNode)
        print(f"Sending gossip FROM {self.localNode.addr}.... ABOUT {nodeInfo.addr}")

        "pack info about subject node"
        nodeInfoProto = nodeInfo.toProto()
        gossipMsg.message.Pack(nodeInfoProto)
        print(f"Proto info: {nodeInfoProto.name} {nodeInfoProto.addr}")
        print(f"Gossip msg: {gossipMsg}")
        await self.gossipManager.gossip_queue.put(
            MeshMessageFactory.toString(gossipMsg)
            )
        print(f"GOSSIP ENQUEUED")
        return None
        
    async def listen_for_nodeInfo_updates(self):
        while True:
            node_info = await self.nodeInfo_queue.get()
            self.nodeInfo_queue.task_done()
            self.updateWithNodeInfo(node_info)

    def registerNode(self, nodeInfo: NodeInfo) -> None:
        print(f"Registering new node... {nodeInfo.addr} {nodeInfo.gossip_addr}")
        self.neighborManager.registerNode(nodeInfo)
        self.loop.create_task(self.gossipAboutNode(nodeInfo))
        return None


    def refuteNotAlive(self) -> None:
        self.localNode.nextIncarnation()
        self.loop.create_task(self.gossipAboutNode(self.localNode))
        return None


    def setNodeHealthFromNodeInfo(self, nodeInfo: NodeInfo) -> None:
        wasUpdated = self.neighborManager.setNodeHealthFromNodeInfo(nodeInfo)
        if wasUpdated:
            self.swimManager.onNodeHealthChange(nodeInfo)
            self.loop.create_task(self.gossipAboutNode(nodeInfo))
        return None


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
            max_tries= meshConnectionConfig.max_connect_retries )
        addr = Address("localhost", bound_port)


        gossip_sock = zmqContext.socket(zmq.PULL)
        gossip_port = gossip_sock.bind_to_random_port(
            "tcp://*",
            min_port = meshConnectionConfig.min_port,
            max_port = meshConnectionConfig.max_port,
            max_tries= meshConnectionConfig.max_connect_retries )
        gossip_addr = Address("localhost", gossip_port)
        print(f"Bound to gossip address {gossip_addr} on {gossip_sock}")

        swim_sock = zmqContext.socket(zmq.ROUTER)
        swim_port = swim_sock.bind_to_random_port(
            "tcp://*",
            min_port = meshConnectionConfig.min_port,
            max_port = meshConnectionConfig.max_port,
            max_tries= meshConnectionConfig.max_connect_retries )
        swim_addr = Address("localhost", swim_port)
        print(f"Bound to swim address {swim_addr} on {swim_sock}")

        gossip_queue: asyncio.Queue = asyncio.Queue(loop=loop)
        nodeInfo_queue: asyncio.Queue = asyncio.Queue(loop=loop)

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
            nodeInfo_queue = nodeInfo_queue,
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
            swimManager = swimManager,
            nodeInfo_queue = nodeInfo_queue)