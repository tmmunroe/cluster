from typing import Dict, Optional, Any, Callable
from collections import namedtuple
import abc
import asyncio
import zmq.asyncio
from src.common.address import Address
from src.service.serviceProxy import ServiceProxyConfig
from proto.service_messages_pb2 import ServiceRequest, ServiceResponse

class ServiceMethodNotFound(Exception):
    pass


class ServiceSpecification:
    def __init__(self, name:str, moduleName: str, serviceName: str, proxyConfig: ServiceProxyConfig):
        self.name = name
        self.moduleName = moduleName
        self.serviceName = serviceName
        self.serviceProxyConfig = proxyConfig


class ServiceMethodSpecification:
    def __init__(self, name:str, func: Callable, reqMsg: Callable, respMsg: Callable):
        self.name = name
        self.func = func
        self.reqMsg = reqMsg
        self.respMsg = respMsg


class ServiceAPI():
    serviceMethodSpecifications: Dict[str, ServiceMethodSpecification] = {}

    def __init__(self, specification: ServiceSpecification):
        self.specification = specification


    @property
    def name(self):
        return self.specification.name


    @classmethod
    def register(cls, serviceName:str, reqMsg:Callable, respMsg:Callable):
        def registerFunc(func: Callable):
            serviceSpec = ServiceMethodSpecification(serviceName, func, reqMsg, respMsg)
            cls.serviceMethodSpecifications[serviceName] = serviceSpec
            return func
        return registerFunc


    @classmethod
    def services(cls):
        return cls.serviceMethodSpecifications.keys()


    def getServiceMethodEntry(self, name: str) -> Optional[ServiceMethodSpecification]:
        return self.serviceMethodSpecifications.get(name, None)


    def hasService(self, name: str) -> bool:
        return name in self.serviceMethodSpecifications


    def processRequest(self, packet: bytes) -> bytes:
        '''unpack service request'''
        msg = ServiceRequest()
        msg.ParseFromString(packet)

        '''get service method'''
        serviceSpec = self.getServiceMethodEntry(msg.serviceName)
        if serviceSpec is None:
            raise ServiceMethodNotFound(f'Service {msg.serviceName} could not be resolved')

        '''unpack service-specific message'''
        serviceReqMsg = serviceSpec.reqMsg()
        msg.message.Unpack(serviceReqMsg)

        '''call service function (returns service-specific response message)'''
        serviceRespMsg = serviceSpec.func(serviceReqMsg)

        '''create and pack service response message'''
        rMsg = ServiceResponse()
        rMsg.serviceName = msg.serviceName
        rMsg.request_id = msg.request_id
        rMsg.message.Pack(serviceRespMsg)

        '''return serialized response message'''
        return rMsg.SerializeToString()


    async def serve(self) -> None:
        '''connect to the service proxy'''
        serviceProxyAddr = self.specification.serviceProxyConfig.backendAddr
        zmqContext = zmq.asyncio.Context.instance()
        conn = zmqContext.socket(zmq.REP)
        zmqServiceProxyAddr = f'tcp://{serviceProxyAddr.host}:{serviceProxyAddr.port}'
        conn.connect(zmqServiceProxyAddr)
        while True:
            requester, emptyFrame, reqMsg = await conn.recv_multipart()
            respMsg = self.processRequest(reqMsg)
            await conn.send_multipart((requester,emptyFrame,respMsg))

        return None
