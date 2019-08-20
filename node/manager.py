
import zmq.asyncio
import asyncio
import concurrent.futures

from src.mesh.nodeInfo import NodeInfo
from src.node.node import Node
from src.node.messageFactory import ClusterMessageFactory
from src.service.serviceManager import ServiceManagerAPI, ServiceSpecification, ServiceNotFound
from src.mesh.mesh import Mesh, MeshFactory


class ManagerNode(Node):

    def start(self) -> None:
        super().start()
        print(f"Listening...")
        self.loop.create_task(self.joinOrSetupCluster())
        self.loop.run_forever()
        return None

    async def joinOrSetupCluster(self) -> None:
        try:
            print(f"Trying to join cluster...")
            await asyncio.wait_for(self.join(self.clusterJoinAddr), 2)
        except concurrent.futures.TimeoutError:
            print(f"Unable to connect... Starting cluster...")
            self.loop.create_task(self.handle_join_requests())
        return None


    async def handle_join_requests(self) -> None:
        '''bind to cluster addr socket to handle joiners'''
        joinersAddr = f'tcp://*:{self.clusterJoinAddr.port}'
        print(f"Binding to {joinersAddr}...")
        zmqContext = zmq.asyncio.Context.instance()
        self.joiners = zmqContext.socket(zmq.ROUTER)
        self.joiners.bind(joinersAddr)
        while True:
            print("Listening for joiners")
            data = await self.joiners.recv_multipart()
            self.loop.create_task(self.handle_new_joiner(data))
        return None


    async def handle_new_joiner(self, data) -> None:
        routing_addr, empty, msg = data

        msg = ClusterMessageFactory.fromString(msg)
        nodeInfo = NodeInfo.fromProto(msg.nodeInfo)
        print(f"Handling new Joiner {msg.nodeInfo}")
        self.mesh.registerNode(nodeInfo)

        '''create full response message, serialize, and send'''
        joinAcceptMessage = ClusterMessageFactory.newJoinAcceptMessage(self.mesh.localNode, self.mesh.getNetworkView())
        multipart_msg = [routing_addr, empty, ClusterMessageFactory.toString(joinAcceptMessage)]
        await self.joiners.send_multipart(multipart_msg)
        return None

    def startServiceProxy(self, serviceName: str):
        try:
            self.serviceManager.launchServiceProxy(serviceName, self.loop)
        except ServiceNotFound:
            print(f"ServiceProxy could not be launched for {serviceName} because it has not been added")


class ManagerNodeFactory():

    @classmethod
    def newManager(self) -> ManagerNode:
        loop = asyncio.get_event_loop()

        mesh = MeshFactory.newMesh()
        serviceManager = ServiceManagerAPI()

        return ManagerNode( loop,
                            mesh,
                            serviceManager )