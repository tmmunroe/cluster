import src.mesh.messages.messages_pb2 as messages_pb2
from src.node.address import Address
from src.mesh.nodeInfo import NodeInfo
from uuid import uuid4

class MeshMessageFactory():
    @classmethod
    def newMessage(self, nodeInfo: NodeInfo):
        msg = messages_pb2.Message()
        '''set sender info'''
        msg.senderInfo.name = nodeInfo.name
        msg.senderInfo.incarnation = nodeInfo.incarnation
        msg.senderInfo.health = nodeInfo.health.value
        if nodeInfo.addr:
            nodeInfo.addr.packProtoAddress(msg.senderInfo.addr)
        if nodeInfo.gossip_addr:
            nodeInfo.gossip_addr.packProtoAddress(msg.senderInfo.gossip_addr)
        if nodeInfo.swim_addr:
            nodeInfo.swim_addr.packProtoAddress(msg.senderInfo.swim_addr)
        return msg

    @classmethod
    def newFromString(self, msgString:str) -> messages_pb2.Message:
        msg = messages_pb2.Message()
        msg.ParseFromString(msgString)
        return msg

    @classmethod
    def newGossipMessage(self, nodeInfo: NodeInfo):
        wrapper = self.newMessage(nodeInfo)
        gossip = messages_pb2.Gossip()
        gossip.remainingSends = 2
        gossip.gossipId = str(uuid4())
        '''set origin info'''
        gossip.originNode.name = nodeInfo.name
        gossip.originNode.incarnation = nodeInfo.incarnation
        gossip.originNode.health = nodeInfo.health.value
        if nodeInfo.addr:
            nodeInfo.addr.packProtoAddress(gossip.originNode.addr)
        if nodeInfo.swim_addr:
            nodeInfo.swim_addr.packProtoAddress(gossip.originNode.swim_addr)
        if nodeInfo.gossip_addr:
            nodeInfo.gossip_addr.packProtoAddress(gossip.originNode.gossip_addr)
        return wrapper, gossip

    @classmethod
    def newPingMessage(self, nodeInfo: NodeInfo, targetName:str, targetAddress: Address) -> messages_pb2.Message:
        wrapper = self.newMessage(nodeInfo)
        pingMsg = messages_pb2.Ping()
        pingMsg.targetName = targetName
        targetAddress.packProtoAddress(pingMsg.targetAddr)
        wrapper.message.Pack(pingMsg)
        return wrapper

    @classmethod
    def newPingRequestMessage(self, nodeInfo: NodeInfo, targetName:str, targetAddress: Address) -> messages_pb2.Message:
        wrapper = self.newMessage(nodeInfo)
        pingMsg = messages_pb2.PingReq()
        pingMsg.targetName = targetName
        targetAddress.packProtoAddress(pingMsg.targetAddr)
        wrapper.message.Pack(pingMsg)
        return wrapper

    @classmethod
    def newAckMessage(self, nodeInfo: NodeInfo) -> messages_pb2.Message:
        wrapper = self.newMessage(nodeInfo)
        ackMsg = messages_pb2.Ack()
        wrapper.message.Pack(ackMsg)
        return wrapper
