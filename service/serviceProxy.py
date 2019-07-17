from src.common.address import Address
import zmq.asyncio
import asyncio
from typing import Tuple, Optional


class ServiceProxyConfig():
    def __init__(self, serviceName: str, frontendAddr: Address, backendAddr: Address ):
        self.serviceName = serviceName
        self.frontendAddr = frontendAddr
        self.backendAddr = backendAddr


class ServiceProxy():
    def __init__(self, config: ServiceProxyConfig):
        self.config = config


    async def handle_communications(self, frontend, backend) -> None:
        while True:
            msg = await frontend.recv_multipart()
            await backend.send_multipart(msg)

    
    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        zmqContext = zmq.asyncio.Context.instance()

        frontendZMQAddr = f"tcp://*.{self.config.frontendAddr.port}"
        frontend = zmqContext.socket(zmq.ROUTER)
        frontend.bind(frontendZMQAddr)

        backendZMQAddr = f"tcp://*.{self.config.backendAddr.port}"
        backend = zmqContext.socket(zmq.DEALER)
        backend.bind(backendZMQAddr)

        print(f"Starting ServiceProxy {self.config.serviceName}")
        print(f" frontend: {frontendZMQAddr}")
        print(f" backend: {backendZMQAddr}")
        
        self.clientListener: asyncio.Task = loop.create_task(self.handle_communications(frontend, backend))
        self.serviceListener: asyncio.Task = loop.create_task(self.handle_communications(backend,frontend))


    def stop(self) -> None:
        if self.clientListener:
            self.clientListener.cancel()

        if self.serviceListener:
            self.serviceListener.cancel()
        
        return None