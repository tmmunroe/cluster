import importlib
from src.service.serviceImpl import ServiceSpecification, serviceSpecifications, rpc_serviceSpecifications
from src.message.messages_pb2 import ServiceRequest, ServiceResponse
from typing import Callable, Dict

class ServiceNotFound(Exception):
    pass


class Service():
    def __init__(self, serviceSpec: ServiceSpecification):
        self.serviceSpec = serviceSpec
        self.function = None

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)
    

class ServiceFactory():
    @classmethod
    def uninitializedServiceFor(self, serviceSpec: ServiceSpecification) -> Service:
        return Service(serviceSpec)


    @classmethod
    def importFuncFor(self, serviceSpec: ServiceSpecification) -> Callable:
        module = importlib.import_module(serviceSpec.moduleName)
        func = getattr(module, serviceSpec.funcName)
        return func


    @classmethod
    def serviceFor(self, serviceSpec: ServiceSpecification) -> Service:
        service = self.uninitializedServiceFor(serviceSpec)
        service.function = self.importFuncFor(serviceSpec)
        return service


class ServiceManager():
    def __init__(self, serviceFactory = ServiceFactory()):
        self.services: Dict[str, Service] = {}
        self.serviceFactory: ServiceFactory = serviceFactory
        self.addStandardServices()
    
    def addStandardServices(self):
        for _, serviceSpec in rpc_serviceSpecifications.items():
            self.addService(serviceSpec)
    

    def addService(self, serviceSpec:ServiceSpecification) -> None:
        serviceName = serviceSpec.serviceName
        print(f"Adding service: {serviceName}")
        if serviceName not in self.services:
            newService = self.serviceFactory.serviceFor(serviceSpec)
            self.services[serviceName] = newService
        return None


    def removeService(self, serviceName:str) -> Service:
        service = self.services.pop(serviceName, None)
        if service is None:
            raise ServiceNotFound()
        return service


    def getService(self, serviceName:str) -> Service:
        service = self.services.get(serviceName, None)
        if service is None:
            raise ServiceNotFound()
        return service

    def handleServiceRequest(self, serviceRequestPacket:str) -> ServiceResponse:
        '''unpack service request'''
        serviceRequestMsg = ServiceRequest()
        serviceRequestPacket.Unpack(serviceRequestMsg)

        '''unpack and call service'''
        service = self.getService(serviceRequestMsg.serviceName)
        serviceMsg = service.serviceSpec.unpackRequestMsg(serviceRequestMsg.message)
        responseMsg = service(serviceMsg)

        '''pack and send response'''
        serviceResponse = ServiceResponse()
        serviceResponse.request_id = serviceRequestMsg.request_id
        serviceResponse.serviceName = serviceRequestMsg.serviceName
        serviceResponse.message.Pack(responseMsg)

        return serviceResponse


    def unpackServiceResponse(self, serviceResponsePacket:str):
        '''unpack service request'''
        serviceResponseMsg = ServiceResponse()
        serviceResponsePacket.Unpack(serviceResponseMsg)

        '''unpack service response msg'''
        service = self.getService(serviceResponseMsg.serviceName)
        return service.serviceSpec.unpackResponseMsg(serviceResponseMsg.message)
