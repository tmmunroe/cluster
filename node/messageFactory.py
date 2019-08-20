
from src.mesh.nodeInfo import NodeInfo
from src.mesh.networkView import NetworkView
from proto.cluster_messages_pb2 import Join, JoinAccept, Shutdown, Leave
from src.common.messageFactory import MessageFactory

class ClusterMessageFactory(MessageFactory):
    messageClasses = [
        Join,
        JoinAccept,
        Shutdown,
        Leave
    ]

    @classmethod
    def newLeaveMessage(cls, nodeInfo: NodeInfo):
        msg = Leave()
        msg.nodeInfo.CopyFrom(nodeInfo.toProto())
        return msg

    @classmethod
    def newJoinMessage(cls, nodeInfo: NodeInfo):
        msg = Join()
        msg.nodeInfo.CopyFrom(nodeInfo.toProto())
        return msg
    
    @classmethod
    def newJoinAcceptMessage(cls, nodeInfo: NodeInfo, networkView:NetworkView):
        msg = JoinAccept()
        msg.networkView.CopyFrom(networkView.toProto())
        return msg
