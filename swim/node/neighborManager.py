'''TODO:
    decide where refutation protocol should be implemented
'''
from src.node.address import Address
from src.node.nodeInfo import NodeInfo, NodeHealth
from src.message.messages_pb2 import NetworkView
from typing import Dict, Sequence, Callable
import abc
import logging
import asyncio

'''Define delegate callable
name, address, incarnation, health'''
NodeHealthChangeDelegate = Callable[[NodeInfo], None]
RefuteNotAliveDelegate = Callable[[], None]
NewNodeDelegate = Callable[[NodeInfo], None]


'''NeighborManager controls the local view of the network topology;
it is responsible for state change messages about Nodes in the network,
and notifying any subscribers to those state changes'''
class NeighborManager():
    def __init__(self):
        self.neighbors: Dict[str,NodeInfo] = {}
        self.nodeHealthChangeDelegates: Sequence[ NodeHealthChangeDelegate ] = []
        self.refuteNotAliveDelegates: Sequence[ RefuteNotAliveDelegate ] = []
        self.newNodeDelegates: Sequence[ NewNodeDelegate ] = []
        self.logger = logging.Logger('NeighborManager')

    def setLocalNodeInfo(self, nodeInfo: NodeInfo) -> None:
        self.localNode = nodeInfo

    def registerNode(self, nodeInfo: NodeInfo) -> None:
        self.neighbors[nodeInfo.name] = nodeInfo
        #print(f"REGISTERING NODE: {nodeInfo.name} from {nodeInfo.addr}")
        #print(f"{len(self.neighbors)} nodes registered")
        self.onNewNode(nodeInfo)
        return None

    def deregisterNode(self, name: str) -> NodeInfo:
        removedNode = self.neighbors.pop(name, None)
        return removedNode

    def getNodeInfo(self, name: str) -> NodeInfo:
        node = self.neighbors.get(name, None)
        return node

    def getNodeAddresses(self) -> Sequence:
        return [ node.addr for node in self.getNodeInfos() ]

    def getNodeInfos(self) -> Sequence:
        return self.neighbors.values()

    def setNodeHealth(self, name:str , health: NodeHealth, incarnation: int) -> None:
        nodeInfo = self.getNodeInfo(name)
        if (nodeInfo and (nodeInfo.health != health) and (nodeInfo.incarnation <= incarnation)):
            nodeInfo.health = health
            nodeInfo.incarnation = incarnation
            print(f"Node HEALTH CHANGED: {name} : {health}")
            self.onNodeHealthChange(nodeInfo)
        return None


    def setNodeHealthFromNodeInfo(self, nodeInfo: NodeInfo) -> None:
        return self.setNodeHealth(nodeInfo.name, nodeInfo.health, nodeInfo.incarnation)


    def onNodeHealthChange(self, nodeInfo: NodeInfo) -> None:
        for delegate in self.nodeHealthChangeDelegates:
            asyncio.create_task(delegate(nodeInfo))
        return None
    

    def onNewNode(self, nodeInfo: NodeInfo) -> None:
        for delegate in self.newNodeDelegates:
            asyncio.create_task(delegate(nodeInfo))


    def refuteNotAlive(self) -> None:
        for delegate in self.refuteNotAliveDelegates:
            asyncio.create_task(delegate())


    def updateWithNodeInfo(self, nodeInfo:NodeInfo) -> None:
        if nodeInfo.isSameNodeAs(self.localNode):
            print(f"RECEIVED NODE INFO ABOUT LOCAL NODE: {nodeInfo.name}")
            if nodeInfo.health != NodeHealth.ALIVE:
                self.refuteNotAlive()
        else:
            myNodeInfo = self.getNodeInfo(nodeInfo.name)
            if myNodeInfo is None:
                print(f"REGISTERING NODE {nodeInfo.name}")
                self.registerNode(nodeInfo)
            else:
                print(f"SETTING NODE HEALTH {nodeInfo.name} : {nodeInfo.health}")
                self.setNodeHealthFromNodeInfo(nodeInfo)
        return None


    def updateWithNetworkViewMessage(self, locNetworkViewMsg:NetworkView):
        nodeInfos = ( NodeInfo.fromProto(nodeInfoProto) for nodeInfoProto in locNetworkViewMsg.nodes )
        for nodeInfo in nodeInfos:
            self.updateWithNodeInfo(nodeInfo)


    def reportState(self):
        nameAndAddr = lambda nodeInfo: f"  {nodeInfo.name}  @  {nodeInfo.addr}  : {nodeInfo.health.name} {nodeInfo.incarnation}"
        print("NeighborManager state:")
        print(f"  LocalNode: {nameAndAddr(self.localNode)}")
        print(f"  Neighbors:")
        for nodeInfo in self.getNodeInfos():
            if nodeInfo.name != self.localNode.name:
                print(f"    {nameAndAddr(nodeInfo)}")

    def __len__(self):
        return len(self.neighbors)