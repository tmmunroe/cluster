from src.node.address import Address
from src.message.messages_pb2 import NodeInfoProto
from enum import Enum
from collections import namedtuple

class NodeHealth(Enum):
    DEAD = NodeInfoProto.NodeHealth.DEAD
    SUSPECT = NodeInfoProto.NodeHealth.SUSPECT
    ALIVE = NodeInfoProto.NodeHealth.ALIVE


'''NodeInfo describes the state of a node as seen by the NeighborManager'''
class NodeInfo():
    def __init__(self, addr:Address, 
            name:str, 
            incarnation:int, 
            health:NodeHealth=NodeHealth.ALIVE,
            gossip_addr=None):
        self.addr: Address = addr
        self.name: str = name
        self.incarnation: int = incarnation
        self.health: NodeHealth = health
        self.gossip_addr: Address = gossip_addr
    
    @classmethod
    def fromProto(self, nodeInfoProto: NodeInfoProto):
        addr = Address(nodeInfoProto.host, nodeInfoProto.port)
        gossip_addr = Address(nodeInfoProto.gossipHost, nodeInfoProto.gossipPort)
        health_dec = NodeHealth(nodeInfoProto.health)
        return NodeInfo(addr,
            nodeInfoProto.name, 
            nodeInfoProto.incarnation,
            health_dec,
            gossip_addr)

    def toProto(self) -> NodeInfoProto:
        nodeInfoProto = NodeInfoProto()
        nodeInfoProto.name = self.name
        nodeInfoProto.host = self.addr.host
        nodeInfoProto.port = self.addr.port
        nodeInfoProto.gossipHost = self.gossip_addr.host
        nodeInfoProto.gossipPort = self.gossip_addr.port
        nodeInfoProto.incarnation = self.incarnation
        nodeInfoProto.health = self.health.value
        return nodeInfoProto
    
    def isSameNodeAs(self, other: object) -> bool:
        return (self.name == other.name) and (self.addr == other.addr)

    def nextIncarnation(self):
        self.incarnation += 1
        return self.incarnation

