'''TODO:
    decide where refutation protocol should be implemented
'''
from src.common.address import Address
from src.node.nodeInfo import NodeInfo, NodeHealth
from src.mesh.networkView import NetworkView
from typing import Dict, Callable, List, Awaitable
import abc
import logging
import asyncio

'''Define delegate callable
name, address, incarnation, health'''
NodeHealthChangeDelegate = Callable[[NodeInfo], Awaitable]
RefuteNotAliveDelegate = Callable[[], Awaitable]
NewNodeDelegate = Callable[[NodeInfo], Awaitable]


'''NeighborManager controls the local view of the network topology;
it is responsible for state change messages about Nodes in the network,
and notifying any subscribers to those state changes'''
class NeighborManager():
    def __init__(self, networkView: NetworkView = NetworkView()):
        self.reportViewPeriod = 10
        self.networkView = networkView
        self.nodeHealthChangeDelegates: List[ NodeHealthChangeDelegate ] = []
        self.refuteNotAliveDelegates: List[ RefuteNotAliveDelegate ] = []
        self.newNodeDelegates: List[ NewNodeDelegate ] = []
        self.logger = logging.Logger('NeighborManager')

    def addNodeHealthChangeDelegate(self, delegate: Callable):
        self.nodeHealthChangeDelegates.append(delegate)
        return None

    def removeNodeHealthChangeDelegate(self, delegate: Callable):
        try:
            self.nodeHealthChangeDelegates.remove(delegate)
        except ValueError:
            print(f"Delegate {delegate} did not exist")
        return None
    

    def addRefuteNotAliveDelegates(self, delegate: Callable):
        self.refuteNotAliveDelegates.append(delegate)
        return None

    def removeRefuteNotAliveDelegates(self, delegate: Callable):
        try:
            self.refuteNotAliveDelegates.remove(delegate)
        except ValueError:
            print(f"Delegate {delegate} did not exist")
        return None
    

    def addNewNodeDelegates(self, delegate: Callable):
        self.newNodeDelegates.append(delegate)
        return None

    def removeNewNodeDelegates(self, delegate: Callable):
        try:
            self.newNodeDelegates.remove(delegate)
        except ValueError:
            print(f"Delegate {delegate} did not exist")
        return None


    def setLocalNodeInfo(self, nodeInfo: NodeInfo) -> None:
        self.localNode = nodeInfo
        return None

    def registerNode(self, nodeInfo: NodeInfo) -> None:
        self.networkView[nodeInfo.name] = nodeInfo
        self.onNewNode(nodeInfo)
        return None

    def deregisterNode(self, name: str) -> NodeInfo:
        removedNode = self.networkView.pop(name, None)
        return removedNode

    def getNodeInfo(self, name: str) -> NodeInfo:
        node = self.networkView.get(name, None)
        return node

    def getNodeAddresses(self) -> List:
        return [ node.addr for node in self.getNodeInfos() ]

    def getNodeInfos(self) -> List:
        return self.networkView.values()

    def setNodeHealth(self, name:str , health: NodeHealth, incarnation: int) -> None:
        nodeInfo = self.getNodeInfo(name)
        if (nodeInfo and (nodeInfo.health != health) and (nodeInfo.incarnation <= incarnation)):
            nodeInfo.health = health
            nodeInfo.incarnation = incarnation
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
        return None


    def refuteNotAlive(self) -> None:
        for delegate in self.refuteNotAliveDelegates:
            asyncio.create_task(delegate())
        return None


    def updateWithNodeInfo(self, nodeInfo:NodeInfo) -> None:
        if nodeInfo.isSameNodeAs(self.localNode):
            if nodeInfo.health != NodeHealth.ALIVE:
                self.refuteNotAlive()
        else:
            myNodeInfo = self.getNodeInfo(nodeInfo.name)
            if myNodeInfo is None:
                self.registerNode(nodeInfo)
            else:
                self.setNodeHealthFromNodeInfo(nodeInfo)
        return None


    def updateWithNetworkView(self, networkView:NetworkView):
        for _,nodeInfo in networkView.items():
            self.updateWithNodeInfo(nodeInfo)
        return None


    def reportState(self):
        nameAndAddr = lambda nodeInfo: f"  {nodeInfo.name}  @  {nodeInfo.addr}  : {nodeInfo.health.name} {nodeInfo.incarnation}"
        print("NeighborManager state:")
        print(f"  LocalNode: {nameAndAddr(self.localNode)}")
        print(f"  Neighbors:")
        for nodeInfo in self.getNodeInfos():
            if nodeInfo.name != self.localNode.name:
                print(f"    {nameAndAddr(nodeInfo)}")
        return None

    def __len__(self):
        return len(self.networkView)


    async def report_state_loop(self):
        while True:
            await asyncio.sleep(self.reportViewPeriod)
            self.reportState()

    def start(self, loop: asyncio.BaseEventLoop) -> None:
        self.loop = loop
        self.loop.create_task(self.report_state_loop())
