from src.common.address import Address
from src.proto.mesh_messages_pb2 import NodeInfoProto
from enum import Enum

class NodeHealth(Enum):
    DEAD = NodeInfoProto.NodeHealth.DEAD
    SUSPECT = NodeInfoProto.NodeHealth.SUSPECT
    ALIVE = NodeInfoProto.NodeHealth.ALIVE

'''NodeInfo describes the state of a node as seen by the NeighborManager'''
class NodeInfo():

    ''
    def __init__(self, 
            addr:Address, 
            name:str, 
            incarnation:int, 
            gossip_addr:Address,
            swim_addr: Address,
            health:NodeHealth=NodeHealth.ALIVE
            ):
        self.addr: Address = addr
        self.name: str = name
        self.incarnation: int = incarnation
        self.health: NodeHealth = health
        self._gossip_addr: Address = gossip_addr
        self._swim_addr: Address = swim_addr
    
    @classmethod
    def fromProto(self, nodeInfoProto: NodeInfoProto):
        addr = Address(nodeInfoProto.addr.host, nodeInfoProto.addr.port)
        gossip_addr = Address(nodeInfoProto.gossip_addr.gossipHost, nodeInfoProto.gossip_addr.gossipPort)
        swim_addr = Address(nodeInfoProto.swim_addr.swimHost, nodeInfoProto.swim_addr.swimPort)
        health_dec = NodeHealth(nodeInfoProto.health)
        return NodeInfo(addr,
            nodeInfoProto.name, 
            nodeInfoProto.incarnation,
            gossip_addr,
            swim_addr,
            health_dec)

    def toProto(self) -> NodeInfoProto:
        nodeInfoProto = NodeInfoProto()
        nodeInfoProto.name = self.name
        nodeInfoProto.incarnation = self.incarnation
        nodeInfoProto.health = self.health.value
        if self.addr:
            self.addr.packProtoAddress(nodeInfoProto.addr)
        if self.gossip_addr:
            self.gossip_addr.packProtoAddress(nodeInfoProto.gossip_addr)
        if self.swim_addr:
            self.swim_addr.packProtoAddress(nodeInfoProto.swim_addr)
        return nodeInfoProto
    
    def isSameNodeAs(self, other: NodeInfo) -> bool:
        return (self.name == other.name) and (self.addr == other.addr)


    @property
    def gossip_addr(self):
        return self._gossip_addr

    @gossip_addr.setter
    def gossip_addr(self, gossip_addr: Address):
        self._gossip_addr = gossip_addr

    @property
    def swim_addr(self):
        return self._swim_addr

    @swim_addr.setter
    def swim_addr(self, swim_addr: Address):
        self._swim_addr = swim_addr

    def nextIncarnation(self):
        self.incarnation += 1
        return self.incarnation

