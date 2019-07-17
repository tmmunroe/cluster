from src.proto.messages_pb2 import Message,Gossip,Ping,PingReq,Ack,JoinRequest,JoinAccept,JoinResponse,AddRequest,AddResponse,SimpleString,NetworkView,ServiceResponse,ServiceRequest
from src.common.address import Address
from src.node.nodeInfo import NodeInfo
from uuid import uuid4
from typing import Tuple, Dict, Callable, Any, Optional

class MessageFactory():
    serviceRequestMessageMap: Dict[str, Callable] = {
        'AddTwoNumbers': AddRequest,
        'Echo': SimpleString,
        'Ping': SimpleString,
        'NetworkViewSync':SimpleString
    }
    
    serviceResponseMessageMap: Dict[str, Callable]  = {
        'AddTwoNumbers': AddResponse,
        'Echo': SimpleString,
        'Ping': SimpleString,
        'NetworkViewSync':NetworkView
    }

    @classmethod
    def newMessage(self, nodeInfo: NodeInfo):
        msg = Message()
        '''set sender info'''
        msg.senderInfo.name = nodeInfo.name
        msg.senderInfo.host = nodeInfo.addr.host
        msg.senderInfo.port = nodeInfo.addr.port
        if nodeInfo.gossip_addr:
            msg.senderInfo.gossipHost = nodeInfo.gossip_addr.host
            msg.senderInfo.gossipPort = nodeInfo.gossip_addr.port
        msg.senderInfo.incarnation = nodeInfo.incarnation
        msg.senderInfo.health = nodeInfo.health.value
        return msg

    @classmethod
    def newFromString(self, msgString:str) -> Message:
        msg = Message()
        msg.ParseFromString(msgString)
        return msg

    @classmethod
    def newGossipMessage(self, nodeInfo: NodeInfo) -> Tuple[Message, Gossip]:
        wrapper = self.newMessage(nodeInfo)
        gossip = Gossip()
        gossip.remainingSends = 2
        gossip.gossipId = str(uuid4())
        '''set origin info'''
        gossip.originNode.name = nodeInfo.name
        gossip.originNode.host = nodeInfo.addr.host
        gossip.originNode.port = nodeInfo.addr.port
        gossip.originNode.incarnation = nodeInfo.incarnation
        gossip.originNode.health = nodeInfo.health.value
        return wrapper, gossip

    @classmethod
    def newPingMessage(self, nodeInfo: NodeInfo, targetInfo: NodeInfo) -> Message:
        wrapper = self.newMessage(nodeInfo)
        pingMsg = Ping()
        pingMsg.targetInfo.name = targetInfo.name
        pingMsg.targetInfo.host = targetInfo.addr.host
        pingMsg.targetInfo.port = targetInfo.addr.port
        wrapper.message.Pack(pingMsg)
        return wrapper

    @classmethod
    def newPingRequestMessage(self, nodeInfo: NodeInfo, targetInfo: NodeInfo) -> Message:
        wrapper = self.newMessage(nodeInfo)
        pingMsg = PingReq()
        pingMsg.targetInfo.name = targetInfo.name
        pingMsg.targetInfo.host = targetInfo.addr.host
        pingMsg.targetInfo.port = targetInfo.addr.port
        wrapper.message.Pack(pingMsg)
        return wrapper

    @classmethod
    def newAckMessage(self, nodeInfo: NodeInfo) -> Message:
        wrapper = self.newMessage(nodeInfo)
        ackMsg = Ack()
        wrapper.message.Pack(ackMsg)
        return wrapper

    @classmethod
    def newJoinRequestMessage(self, nodeInfo: NodeInfo) -> Message:
        wrapper = self.newMessage(nodeInfo)
        joinReq = JoinRequest()
        wrapper.message.Pack(joinReq)
        return wrapper

    @classmethod
    def newJoinAcceptMessage(self, nodeInfo: NodeInfo, knownNodeInfos) -> Message:
        wrapper = self.newMessage(nodeInfo)
        joinAccept = JoinAccept()
        joinAccept.networkView.nodes.extend( [ nodeInfo.toProto() for nodeInfo in knownNodeInfos ] )
        wrapper.message.Pack(joinAccept)
        return wrapper

    @classmethod
    def unpackServiceRequest(self, serviceName:str, data) -> Optional[ Tuple[Message,ServiceRequest,Any] ]:
        msg = MessageFactory.newFromString(data)
        serviceMsgWrapper = ServiceRequest()
        msg.message.Unpack(serviceMsgWrapper)

        serviceMsgConstructor = self.serviceRequestMessageMap.get(serviceName)
        if serviceMsgConstructor:
            serviceMsg = serviceMsgConstructor()
            serviceMsgWrapper.message.Unpack(serviceMsg)
            return msg, serviceMsgWrapper, serviceMsg
        return None

    @classmethod
    def unpackServiceResponse(self, serviceName:str, any_msg) -> Optional[ Tuple[ServiceResponse, Any] ]:
        msg = ServiceResponse()
        serviceMsgConstructor = self.serviceResponseMessageMap.get(serviceName)
        if serviceMsgConstructor:
            serviceMsg = serviceMsgConstructor()
            if any_msg.Unpack(msg) and msg.message.Unpack(serviceMsg):
                return msg, serviceMsg
        else:
            raise Exception()
        return None

    @classmethod
    def newServiceRequestMessage(self, nodeInfo:NodeInfo, serviceName:str):
        wrapper = self.newMessage(nodeInfo)
        serviceWrapper = ServiceRequest()
        serviceWrapper.serviceName = serviceName
        serviceMsgBuilder = self.serviceRequestMessageMap.get(serviceName)
        if serviceMsgBuilder:
            serviceMsg = serviceMsgBuilder()
            return wrapper, serviceWrapper, serviceMsg
        return None

    @classmethod
    def newServiceResponseMessage(self, nodeInfo:NodeInfo, serviceName:str):
        wrapper = self.newMessage(nodeInfo)
        serviceWrapper = ServiceResponse()
        serviceWrapper.serviceName = serviceName
        serviceMsgBuilder = self.serviceResponseMessageMap.get(serviceName)
        if serviceMsgBuilder:
            serviceMsg = serviceMsgBuilder()
            return wrapper, serviceWrapper, serviceMsg
        return None
        
    @classmethod
    def newAddRequest(self, nodeInfo: NodeInfo, numA: int, numB: int):
        serviceName = 'AddTwoNumbers'
        msg, serviceMsg, addMsg = self.newServiceRequestMessage(nodeInfo, serviceName)
        addMsg.numA = numA
        addMsg.numB = numB

        serviceMsg.message.Pack(addMsg)
        msg.message.Pack(serviceMsg)
        return msg

    @classmethod
    def newEchoRequest(self, nodeInfo: NodeInfo, echo: str):
        serviceName = 'Echo'
        msg, serviceMsg, echoMsg = self.newServiceRequestMessage(nodeInfo, serviceName)
        echoMsg.aString = echo

        serviceMsg.message.Pack(echoMsg)
        msg.message.Pack(serviceMsg)
        return msg