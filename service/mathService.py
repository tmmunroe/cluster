import abc
from src.common.address import Address
from proto.build.service_messages_pb2 import AddRequest,AddResponse
from src.service.serviceAPI import ServiceAPI, ServiceSpecification
from src.service.serviceProxy import ServiceProxyConfig

mathServiceProxyConfig = ServiceProxyConfig("MathService", 
    frontendAddr = Address(host="*", port=9091),
    backendAddr = Address(host="*", port=9093))

mathServiceSpecification = ServiceSpecification(
    name="MathService",
    moduleName="src.service.mathService",
    serviceName="MathService",
    proxyConfig=mathServiceProxyConfig)


class MathService(ServiceAPI):
    @staticmethod
    @MathService.register("Add", AddRequest, AddResponse)
    def addService(addRequest: AddRequest) -> AddResponse:
        addResp = AddResponse()
        addResp.result = addRequest.numA + addRequest.numB
        return addResp
    
