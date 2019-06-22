from src.message.messages_pb2 import SimpleString, AddRequest, AddResponse, ServiceRequest, ServiceResponse
from src.message.messageFactory import MessageFactory
from src.node.nodeInfo import NodeInfo
from typing import Callable

serviceSpecifications = {}
rpc_serviceSpecifications = {}

class ServiceSpecification():
    def __init__(self, 
            serviceName:str,
            moduleName:str, 
            funcName:str):
        self.serviceName = serviceName
        self.moduleName = moduleName
        self.funcName = funcName

def service_spec(name:str) -> Callable:
    def service_spec_decorator(func:Callable) -> Callable:
        service_name:str = name
        func_name:str = func.__name__
        module_name:str = func.__module__
        serviceSpecifications[service_name] = ServiceSpecification(service_name, module_name, func_name)
        return func
    return service_spec_decorator


class RPCServiceSpecification():
    def __init__(self, 
            serviceName:str,
            moduleName:str, 
            funcName:str,
            responseMsgFactory,
            requestMsgFactory):
        self.serviceSpec = ServiceSpecification(serviceName, moduleName, funcName)
        self.responseMsgFactory = responseMsgFactory
        self.requestMsgFactory = requestMsgFactory

    @property
    def funcName(self):
        return self.serviceSpec.funcName

    @property
    def moduleName(self):
        return self.serviceSpec.moduleName

    @property
    def serviceName(self):
        return self.serviceSpec.serviceName

    def unpackRequestMsg(self, data: str):
        '''unpack request message'''
        requestMsg = self.requestMsgFactory()
        data.Unpack(requestMsg)

        return requestMsg
    

    def unpackResponseMsg(self, data: str):
        '''unpack request message'''
        responseMsg = self.responseMsgFactory()
        data.Unpack(responseMsg)

        return responseMsg


    def packResponseMsg(self, **kwResponses):
        responseMsg = self.responseMsgFactory()
        for key,value in kwResponses.items():
            setattr(responseMsg, key, value)
        return responseMsg


    def packRequestMsg(self, **kwRequests):
        requestMsg = self.requestMsgFactory()
        for key,value in kwRequests.items():
            setattr(requestMsg, key, value)
        return requestMsg



def rpc_service_spec(name:str, reqMsg, respMsg) -> Callable:
    def service_spec_decorator(func:Callable) -> Callable:
        service_name:str = name
        func_name:str = func.__name__
        module_name:str = func.__module__
        rpc_serviceSpecifications[service_name] = RPCServiceSpecification(service_name, module_name, func_name, respMsg, reqMsg)
        return func
    return service_spec_decorator
    


@rpc_service_spec("Echo", SimpleString, SimpleString)
def echoServicer(msg: SimpleString) -> SimpleString:
    return msg

@rpc_service_spec("AddTwoNumbers", AddRequest, AddResponse)
def addTwoNumbersServicer(msg: AddRequest) -> AddResponse:
    responseMsg = AddResponse()
    responseMsg.result = msg.numA + msg.numB
    return responseMsg

