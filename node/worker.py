import zmq.asyncio
import asyncio

from src.common.address import Address
from src.node.node import Node, NodeConfiguration
from src.mesh.mesh import Mesh, MeshFactory
from src.service.serviceManager import ServiceManagerAPI


class WorkerNode(Node):
    def start(self) -> None:
        print(f"Starting node...")
        super().start()
        self.loop.create_task(self.join_to_cluster(self.clusterJoinAddr))
        self.loop.run_forever()


    async def join_to_cluster(self, clusterAddr: Address) -> None:
        print(f"Attempting to join cluster {clusterAddr}...")
        respMsg = await asyncio.wait_for(self.join(clusterAddr), 2)
        if not respMsg:
            print(f"Could not connect to cluster...")
        else:
            print(f"Successfully joined cluster...")
        return None


class WorkerNodeFactory():
    @classmethod
    def newWorker(self) -> WorkerNode:
        loop = asyncio.get_event_loop()
        zmqContext = zmq.asyncio.Context.instance()
        
        mesh = MeshFactory.newMesh()
        serviceManager = ServiceManagerAPI()

        return WorkerNode( loop, 
                            zmqContext,
                            mesh,
                            serviceManager )
