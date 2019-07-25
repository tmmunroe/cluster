from src.mesh.neighborManager import NeighborManager
from src.mesh.nodeInfo import NodeInfo, NodeHealth
from src.mesh.messageFactory import MeshMessageFactory
from src.proto.mesh_messages_pb2 import Ping, PingReq, Ack
from src.common.address import Address
from typing import Dict, Optional
import zmq
import random
import asyncio

class SwimConnectionConfiguration():
    def __init__(self):
        self.min_port = 59101
        self.max_port = 61101
        self.max_connect_retries = 5

class SwimConfiguration():
    def __init__(self,
            swim_connection_config = SwimConnectionConfiguration()):
        self.healthCheckPeriod = 5
        self.pingReqFanout = 1
        self.pingTimeout = 2
        self.pingReqTimeout = 5
        self.suspectNodeTolerance = 25
        self.deadNodeTolerance = 60
        self.swim_connection_config = swim_connection_config

class SwimManager():

    def __init__(self,
            localNode: NodeInfo,
            swim_addr: Address,
            swim_sock: zmq.Socket,
            neighborManager:NeighborManager,
            zmqContext: zmq.asyncio.Context,
            swimConfig:SwimConfiguration = SwimConfiguration()):
        self.localNode = localNode
        self.swim_addr = swim_addr
        self.swim_sock = swim_sock
        self.neighborManager = neighborManager
        self.zmqContext = zmqContext
        self.config = swimConfig
        self.connectionConfig = swimConfig.swim_connection_config
        self.degradedNodeTimers: Dict[str, asyncio.TimerHandle] = {}

        '''add onNodeHealthChange listener to neighbor manager'''
        self.neighborManager.nodeHealthChangeDelegates.append(self.onNodeHealthChange)


    async def onNodeHealthChange(self, nodeInfo: NodeInfo) -> None:
        '''we want to handle health changes as such:
        FIRST, cancel any pre-existing timers
        NEXT, act accordng to the new health:
        to Suspect -> set a timer for how long a node can stay
            suspect, degrading it to dead if it hasn't refuted
            by the time it goes off
        to Dead -> set a timer for how long a node can stay dead,
            removing it if from the neighbor list if it hasn't
            refuted by the time it goes off
        to Alive -> do nothing
        NOTE: we make the assumption that only valid health changes will trigger
            a callback.. primarily, that the incarnation number will have been 
            checked upstream'''
        self.cancelDegradedNodeTimersFor(nodeInfo.name)
        if nodeInfo.health == NodeHealth.SUSPECT:
            print(f"SCHEDULING DEGRADATION OF NODE: {nodeInfo.name}")
            timer = self.loop.call_later(
                self.config.suspectNodeTolerance,
                self.degradeSuspectNode,
                nodeInfo.name,
                nodeInfo.incarnation)
            self.degradedNodeTimers[nodeInfo.name] = timer
        elif nodeInfo.health == NodeHealth.DEAD:
            print(f"SCHEDULING REMOVAL OF NODE: {nodeInfo.name}")
            timer = self.loop.call_later(
                self.config.deadNodeTolerance,
                self.removeDeadNode,
                nodeInfo.name)
            self.degradedNodeTimers[nodeInfo.name] = timer
        elif nodeInfo.health == NodeHealth.ALIVE:
            print(f"NODE IS ALIVE: {nodeInfo.name}")
        else:
            print(f"UNEXPECTED NODE HEALTH: {nodeInfo.name} : {nodeInfo.health}")
        
        return None


    def cancelDegradedNodeTimersFor(self, nodeName: str) -> None:
        timer = self.degradedNodeTimers.get(nodeName, None)
        if timer:
            timer.cancel()
        return None


    def degradeSuspectNode(self, nodeName: str, incarnation: int) -> None:
        print(f"DEGRADING SUSPECT NODE: {nodeName}")
        self.neighborManager.setNodeHealth(nodeName,
            NodeHealth.DEAD,
            incarnation)
        return None
    

    def removeDeadNode(self, nodeName: str) -> None:
        print(f"REMOVING DEAD NODE: {nodeName}")
        self.neighborManager.deregisterNode(nodeName)
        return None


    def setLocalNodeInfo(self, nodeInfo: NodeInfo) -> None:
        self.localNode = nodeInfo
        return None


    async def sendReceive(self, targetNode: NodeInfo, data:bytes) -> bytes:
        targetNodeAddr = f"tcp://{targetNode.swim_addr}"
        conn = self.zmqContext.socket(zmq.REQ)
        conn.connect(targetNodeAddr)
        await conn.send(data)
        received = await conn.recv()
        return received


    async def ping(self, targetNode: NodeInfo) -> Optional[NodeInfo]:
        pingMsg = MeshMessageFactory.newPingMessage( self.localNode, targetNode.name, targetNode.swim_addr )
        try:
            sendReceiveTask = self.loop.create_task(self.sendReceive(targetNode, pingMsg.SerializeToString()))
            receivedData = await asyncio.wait_for(sendReceiveTask, timeout = self.config.pingTimeout)
            #print(f"PING RECEIVED BACK: {receivedData}")
        except asyncio.TimeoutError:
            print("Ping request timed out")
            return None

        ackMsg = MeshMessageFactory.newFromString(receivedData)

        '''unpack ack target info'''
        senderInfo = self.neighborManager.getNodeInfo(ackMsg.targetName)
        senderAddr = Address.fromProtoAddress(ackMsg.targetAddress)
        if targetNode.swim_addr != senderAddr:
            print(f"ERROR: For {targetNode.name}, have target address {targetNode.swim_addr} but received target address {senderAddr}")

        if not senderInfo.isSameNodeAs(targetNode):
            print(f"ERROR: Unexpected Node Info received:")
            print(f"{senderInfo.name} @ {senderInfo.swim_addr}")
            print(f"{targetNode.name} @ {targetNode.swim_addr}")
        if ackMsg.message.Is(Ack.DESCRIPTOR):
            #print(f"RECEIVED PING ACK")
            return senderInfo
        else:
            print(f"ERROR: Unknown message type received")
            return None


    async def ping_req(self, targetNode: NodeInfo, bridgeNode: NodeInfo) -> Optional[NodeInfo]:
        pingReqMsg = MeshMessageFactory.newPingRequestMessage( self.localNode, targetNode.name, targetNode.swim_addr )
        try:
            sendReceiveTask = self.loop.create_task(self.sendReceive(bridgeNode, pingReqMsg.SerializeToString()))
            receivedData = await asyncio.wait_for(sendReceiveTask, timeout = self.config.pingReqTimeout)
            print(f"PINGREQ RECEIVED BACK: {receivedData}")
        except asyncio.TimeoutError:
            print("Ping request timed out")
            return None

        msg = MeshMessageFactory.newFromString(receivedData)

        '''unpack ack target info'''
        senderInfo = self.neighborManager.getNodeInfo(msg.targetName)
        senderAddr = Address.fromProtoAddress(msg.targetAddress)
        if targetNode.swim_addr != senderAddr:
            print(f"ERROR: For {targetNode.name}, have target address {targetNode.swim_addr} but received target address {senderAddr}")

        if not senderInfo.isSameNodeAs(targetNode):
            print(f"ERROR: Unexpected Node Info received:")
            print(f"{senderInfo.name} @ {senderInfo.swim_addr}")
            print(f"{targetNode.name} @ {targetNode.swim_addr}")
        if msg.message.Is(Ack.DESCRIPTOR):
            print("RECEIVED PING REQ ACK")
            return senderInfo
        else:
            return None


    async def health_check(self, targetNode: NodeInfo) -> None:
        print(f"Performing health check for {targetNode.name} @ {targetNode.swim_addr}")
        receivedNodeInfo = await self.ping(targetNode)
        if receivedNodeInfo:
            #print(f"SUCCESSFULLY PINGED: {targetNode.name}")
            self.neighborManager.setNodeHealth(receivedNodeInfo.name, 
                receivedNodeInfo.health, 
                receivedNodeInfo.incarnation)
            return None

        '''get recipients'''
        print(f"FAILED PING: {targetNode.name}... TRYING PINGREQ")
        nodeInfos = [ nodeInfo for nodeInfo in self.neighborManager.getNodeInfos() 
            if not nodeInfo.isSameNodeAs(self.localNode) ]
        if len(nodeInfos) == 0:
            print("Waiting for others to join cluster...")
            return None
            
        fanout = min(self.config.pingReqFanout, len(nodeInfos))
        bridgeNodes = random.sample(nodeInfos, k=fanout)
        tasks = [ self.loop.create_task(self.ping_req(targetNode, bridgeNode)) for bridgeNode in bridgeNodes ]
        for future in asyncio.as_completed( tasks ):
            receivedNodeInfo = await future
            if receivedNodeInfo:
                self.neighborManager.setNodeHealth(receivedNodeInfo.name, 
                    receivedNodeInfo.health, 
                    receivedNodeInfo.incarnation)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                return None
        
        '''if we have gotten to this point, then there were no successful
            PINGs or PINGREQs.. there are two cases based on the node's
            current health rating:
            Node is ALIVE -> mark it suspect
            Otherwise -> do nothing.. we're already suspicious about it'''
        if targetNode.health == NodeHealth.ALIVE:
            print(f"SETTING Node Health for {targetNode.name} to SUSPECT")
            self.neighborManager.setNodeHealth( targetNode.name,
                NodeHealth.SUSPECT,
                targetNode.incarnation )
        return None


    async def handle_direct_message(self, zmqAddress, empty, data:str):
        msg = MeshMessageFactory.newFromString(data)
        #print(f"Handling direct message: {msg}")

        if msg.message.Is(Ping.DESCRIPTOR):
            await self.handle_ping(zmqAddress, empty, msg)
        elif msg.message.Is(PingReq.DESCRIPTOR):
            await self.handle_ping_req(zmqAddress, empty, msg)
        else:
            print("Unknown message type...")
        return None


    async def handle_ping(self, zmqAddress, empty, pingMsg: Ping) -> None:
        ackMsg = MeshMessageFactory.newAckMessage(self.localNode, self.localNode.name, 
            self.localNode.swim_addr)
        multipart = [ zmqAddress, empty, ackMsg.SerializeToString() ]
        await self.swim_sock.send_multipart(multipart)
        return None


    async def handle_ping_req(self, zmqAddress, empty, pingReqMsg: PingReq) -> None:
        '''unpack ping request target info'''
        targetNode = self.neighborManager.getNodeInfo(pingReqMsg.targetName)
        targetAddr = Address.fromProtoAddress(pingReqMsg.targetAddress)
        if targetNode.swim_addr != targetAddr:
            print(f"ERROR: For {targetNode.name}, have target address {targetNode.swim_addr} but received target address {targetAddr}")

        print(f"Performing PING REQ check for {targetNode.name} @ {targetNode.swim_addr}")
        receivedNodeInfo = await self.ping(targetNode)
        if receivedNodeInfo:
            print("PING REQ WAS SUCCESSFUL")
            '''update ourselves and also send along result'''
            self.neighborManager.setNodeHealth(receivedNodeInfo.name, 
                receivedNodeInfo.health, 
                receivedNodeInfo.incarnation)
            
            ackMsg = MeshMessageFactory.newAckMessage(receivedNodeInfo, 
                receivedNodeInfo.name, receivedNodeInfo.swim_addr)
            multipart = [ zmqAddress, empty, ackMsg.SerializeToString() ]
            await self.swim_sock.send_multipart(multipart)
        else:
            print("PING REQ FAILED")
        return None


    async def handle_direct_message_loop(self):
        '''handle incoming'''
        while True:
            zmqAddress, empty, data = await self.swim_sock.recv_multipart()
            self.loop.create_task(self.handle_direct_message(zmqAddress, empty, data))
    
    
    async def health_check_loop(self):
        while True:
            await asyncio.sleep(self.config.healthCheckPeriod)
            nodeInfos = [ nodeInfo for nodeInfo in self.neighborManager.getNodeInfos() 
                if (nodeInfo.name != self.localNode.name) and (nodeInfo.health == NodeHealth.ALIVE) ]

            if len(nodeInfos) == 0:
                print("Waiting for others to join cluster...")
            else:
                targetNode = random.choice(nodeInfos)
                await self.health_check(targetNode)
        return None


    async def start(self, loop: asyncio.BaseEventLoop) -> None:
        self.loop = loop
        self.loop.create_task(self.handle_direct_message_loop())
        self.loop.create_task(self.health_check_loop())


        

