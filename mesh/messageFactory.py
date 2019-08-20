from proto.mesh_messages_pb2 import Ping, PingReq, Ack, NodeInfoProto, NetworkView, Gossip
from src.common.address import Address
from src.mesh.nodeInfo import NodeInfo
from uuid import uuid4
from src.common.messageFactory import MessageFactory

class UnsupportedMessageType(Exception):
    pass

class MeshMessageFactory(MessageFactory):
    messageClasses = [
        Ping,
        PingReq,
        Ack,
        NodeInfoProto,
        NetworkView,
        Gossip
    ]

    @classmethod
    def newGossipMessage(self, nodeInfo: NodeInfo):
        '''create gossip message'''
        gossip = Gossip()
        gossip.remainingSends = 2
        gossip.gossipId = str(uuid4())
        gossip.originNode.name = nodeInfo.name
        gossip.originNode.incarnation = nodeInfo.incarnation
        gossip.originNode.health = nodeInfo.health.value
        if nodeInfo.addr:
            nodeInfo.addr.packProtoAddress(gossip.originNode.addr)
        if nodeInfo.swim_addr:
            nodeInfo.swim_addr.packProtoAddress(gossip.originNode.swim_addr)
        if nodeInfo.gossip_addr:
            nodeInfo.gossip_addr.packProtoAddress(gossip.originNode.gossip_addr)
        return gossip

    @classmethod
    def newPingMessage(self, nodeInfo: NodeInfo, targetName:str, targetAddress: Address) -> Ping:
        pingMsg = Ping()
        pingMsg.targetName = targetName
        targetAddress.packProtoAddress(pingMsg.targetAddress)
        return pingMsg

    @classmethod
    def newPingRequestMessage(self, nodeInfo: NodeInfo, targetName:str, targetAddress: Address) -> PingReq:
        pingMsg = PingReq()
        pingMsg.targetName = targetName
        targetAddress.packProtoAddress(pingMsg.targetAddress)
        return pingMsg

    @classmethod
    def newAckMessage(self, nodeInfo: NodeInfo, targetName: str, targetAddress: Address) -> Ack:
        ackMsg = Ack()
        ackMsg.targetName = targetName
        targetAddress.packProtoAddress(ackMsg.targetAddress)
        return ackMsg
