import src.message.messages_pb2 as messages_pb2
from src.node.address import Address
from src.node.nodeInfo import NodeInfo
from uuid import uuid4

class MessageFactory():
    serviceRequestMessageMap = {
        'AddTwoNumbers': messages_pb2.AddRequest,
        'Echo': messages_pb2.SimpleString,
        'Ping': messages_pb2.SimpleString,
        'NetworkViewSync':messages_pb2.SimpleString
    }
    
    serviceResponseMessageMap = {
        'AddTwoNumbers': messages_pb2.AddResponse,
        'Echo': messages_pb2.SimpleString,
        'Ping': messages_pb2.SimpleString,
        'NetworkViewSync':messages_pb2.NetworkView
    }

    @classmethod
    def newMessage(self, nodeInfo: NodeInfo):
        msg = messages_pb2.Message()
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
        gossip.originNode.host = nodeInfo.addr.host
        gossip.originNode.port = nodeInfo.addr.port
        gossip.originNode.incarnation = nodeInfo.incarnation
        gossip.originNode.health = nodeInfo.health.value
        return wrapper, gossip

    @classmethod
    def newPingMessage(self, nodeInfo: NodeInfo, targetInfo: NodeInfo) -> messages_pb2.Message:
        wrapper = self.newMessage(nodeInfo)
        pingMsg = messages_pb2.Ping()
        pingMsg.targetInfo.name = targetInfo.name
        pingMsg.targetInfo.host = targetInfo.addr.host
        pingMsg.targetInfo.port = targetInfo.addr.port
        wrapper.message.Pack(pingMsg)
        return wrapper

    @classmethod
    def newPingRequestMessage(self, nodeInfo: NodeInfo, targetInfo: NodeInfo) -> messages_pb2.Message:
        wrapper = self.newMessage(nodeInfo)
        pingMsg = messages_pb2.PingReq()
        pingMsg.targetInfo.name = targetInfo.name
        pingMsg.targetInfo.host = targetInfo.addr.host
        pingMsg.targetInfo.port = targetInfo.addr.port
        wrapper.message.Pack(pingMsg)
        return wrapper

    @classmethod
    def newAckMessage(self, nodeInfo: NodeInfo) -> messages_pb2.Message:
        wrapper = self.newMessage(nodeInfo)
        ackMsg = messages_pb2.Ack()
        wrapper.message.Pack(ackMsg)
        return wrapper

    @classmethod
    def newJoinRequestMessage(self, nodeInfo: NodeInfo) -> messages_pb2.Message:
        wrapper = self.newMessage(nodeInfo)
        joinReq = messages_pb2.JoinRequest()
        wrapper.message.Pack(joinReq)
        return wrapper

    @classmethod
    def newJoinAcceptMessage(self, nodeInfo: NodeInfo, knownNodeInfos) -> messages_pb2.Message:
        wrapper = self.newMessage(nodeInfo)
        joinAccept = messages_pb2.JoinAccept()
        joinAccept.networkView.nodes.extend( [ nodeInfo.toProto() for nodeInfo in knownNodeInfos ] )
        wrapper.message.Pack(joinAccept)
        return wrapper

    @classmethod
    def unpackServiceRequest(self, serviceName:str, data):
        msg = MessageFactory.newFromString(data)
        print(f"MSG: {msg}")

        serviceMsgWrapper = messages_pb2.ServiceRequest()
        msg.message.Unpack(serviceMsgWrapper)
        print(f"SERVICEMSGWRAPPER: {serviceMsgWrapper}")


        serviceMsgConstructor = self.serviceRequestMessageMap.get(serviceName)
        serviceMsg = serviceMsgConstructor()
        print(f"serviceMsg: {serviceMsg}")
        print(f"serviceMessage: {serviceMsgWrapper.message}")

        serviceMsgWrapper.message.Unpack(serviceMsg)
        print(f"SERVICEMSG: {serviceMsg}")

        return msg, serviceMsgWrapper, serviceMsg


    @classmethod
    def unpackServiceResponse(self, serviceName:str, any_msg):
        msg = messages_pb2.ServiceResponse()
        serviceMsgConstructor = self.serviceResponseMessageMap.get(serviceName)
        serviceMsg = serviceMsgConstructor()
        if any_msg.Unpack(msg) and msg.message.Unpack(serviceMsg):
            return msg, serviceMsg
        else:
            raise Exception()

    @classmethod
    def newServiceRequestMessage(self, nodeInfo:NodeInfo, serviceName:str):
        wrapper = self.newMessage(nodeInfo)
        serviceWrapper = messages_pb2.ServiceRequest()
        serviceWrapper.serviceName = serviceName
        serviceMsgBuilder = self.serviceRequestMessageMap.get(serviceName)
        serviceMsg = serviceMsgBuilder()
        return wrapper, serviceWrapper, serviceMsg

    @classmethod
    def newServiceResponseMessage(self, nodeInfo:NodeInfo, serviceName:str):
        wrapper = self.newMessage(nodeInfo)
        serviceWrapper = messages_pb2.ServiceResponse()
        serviceWrapper.serviceName = serviceName
        serviceMsgBuilder = self.serviceResponseMessageMap.get(serviceName)
        serviceMsg = serviceMsgBuilder()
        return wrapper, serviceWrapper, serviceMsg
        
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
