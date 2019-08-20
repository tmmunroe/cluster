'''TODO:
    decide where refutation protocol should be implemented
'''
from src.common.address import Address
from src.mesh.nodeInfo import NodeInfo, NodeHealth
from src.mesh.networkView import NetworkView
from typing import Dict, Callable, List, Awaitable, ValuesView
import abc
import logging
import asyncio


'''NeighborManager controls the local view of the network topology;
it is responsible for state change messages about Nodes in the network,
and notifying any subscribers to those state changes'''
class NeighborManager():
    def __init__(self, networkView: NetworkView = NetworkView()):
        self.reportViewPeriod = 5
        self.networkView = networkView
        self.logger = logging.Logger('NeighborManager')

    def setLocalNodeInfo(self, nodeInfo: NodeInfo) -> None:
        self.localNode = nodeInfo
        return None

    def registerNode(self, nodeInfo: NodeInfo) -> None:
        self.networkView[nodeInfo.name] = nodeInfo
        return None

    def deregisterNode(self, name: str) -> NodeInfo:
        removedNode = self.networkView.pop(name, None)
        return removedNode

    def getNodeInfo(self, name: str) -> NodeInfo:
        node = self.networkView.get(name, None)
        return node

    def getNodeAddresses(self) -> List:
        return [ node.addr for node in self.getNodeInfos() ]

    def getNodeInfos(self) -> ValuesView:
        return self.networkView.values()

    def getNetworkView(self) -> NetworkView:
        return self.networkView

    def setNodeHealth(self, name:str , health: NodeHealth, incarnation: int) -> bool:
        nodeInfo = self.getNodeInfo(name)
        if (nodeInfo and (nodeInfo.health != health) and (nodeInfo.incarnation <= incarnation)):
            nodeInfo.health = health
            nodeInfo.incarnation = incarnation
            return True
        return False


    def setNodeHealthFromNodeInfo(self, nodeInfo: NodeInfo) -> bool:
        return self.setNodeHealth(nodeInfo.name, nodeInfo.health, nodeInfo.incarnation)


    def reportState(self):
        nameAndAddr = lambda nodeInfo: f"  {nodeInfo.name}  @  {nodeInfo.addr}  gossip: {nodeInfo.gossip_addr} : {nodeInfo.health.name} {nodeInfo.incarnation}"
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
        print(f"NeighborManager started")
        return None
