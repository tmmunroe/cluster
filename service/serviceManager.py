import abc
from importlib import import_module
from src.service.serviceAPI import ServiceAPI, ServiceSpecification
from src.service.serviceProxy import ServiceProxy
from typing import Dict, Optional, DefaultDict
from collections import defaultdict
from src.common.address import Address
import zmq.asyncio
import asyncio

class ServiceNotFound(Exception):
    pass

class ServiceManagerAPI(meta=abc.ABCMeta):
    '''Manages Service instances and proxies'''
    def __init__(self):
        self.services: Dict[str, ServiceAPI] = {}
        self.launchedServices: DefaultDict[str, List[Task]] = defaultdict(list)
        self.serviceProxies: Dict[str, ServiceProxy] = {}


    def loadServiceInstance(self, serviceSpec: ServiceSpecification) -> ServiceAPI:
        module = import_module(serviceSpec.moduleName)
        service = getattr(module, serviceSpec.serviceName)
        return service(serviceSpec)


    def addService(self, serviceSpec: ServiceSpecification) -> None:
        service = self.loadServiceInstance(serviceSpec)
        self.services[serviceSpec.name] = service


    def removeService(self, serviceName: str) -> None:
        self.stopServing(serviceName)
        self.stopProxy(serviceName)
        service = self.services.pop(serviceName, None)
        return None


    def getService(self, serviceName: str) -> Optional[ServiceAPI]:
        return self.services.get(serviceName,None)


    def offersService(self, serviceName: str) -> bool:
        return serviceName in self.services


    def launchService(self, 
            serviceName: str,
            loop: asyncio.AbstractEventLoop) -> None:
        serviceInstance = self.getService(serviceName)
        if not serviceInstance:
            raise ServiceNotFound(f"Service {serviceName} has not been added yet")
        
        '''TODO: we might want to launch this in a separate process instead
            of as part of the event loop'''
        launchedTask = loop.create_task(serviceInstance.serve())
        self.launchedServices[serviceName].append(launchedTask)
        return None

 
    def stopServing(self, serviceName: str) -> None:
        launchedTasks = self.launchedServices[serviceName]
        while len(launchedTasks) != 0:
            task = launchedTasks.pop()
            task.cancel()
        return None


    def launchServiceProxy(self,
            serviceName: str,
            loop: asyncio.AbstractEventLoop) -> None:
        serviceInstance = self.getService(serviceName)
        if not serviceInstance:
            raise ServiceNotFound(f"Service {serviceName} has not been added yet")

        serviceSpec = serviceInstance.specification
        proxyConfig = serviceSpec.serviceProxyConfig
        proxy = ServiceProxy(proxyConfig)
        proxy.start(loop)

        self.serviceProxies[serviceName] = proxy
        
     
    def stopProxy(self, serviceName: str) -> None:
        proxy = self.serviceProxies[serviceName]
        if proxy is not None:
            proxy.stop()
        return None