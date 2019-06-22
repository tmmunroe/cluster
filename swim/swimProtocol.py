from src.node.neighborManager import NeighborManager
from src.node.nodeInfo import NodeInfo, NodeHealth
from src.message.messageFactory import MessageFactory
from src.message.messages_pb2 import Ping, PingReq, Ack
import zmq
import random
import asyncio

class SwimConfiguration():
    def __init__(self):
        self.healthCheckPeriod = 5
        self.pingReqFanout = 1
        self.pingTimeout = 2
        self.pingReqTimeout = 5
        self.suspectNodeTolerance = 25
        self.deadNodeTolerance = 60

class SwimManager():

    def __init__(self,
            neighborManager:NeighborManager,
            zmqContext,
            loop,
            swimConfig:SwimConfiguration = SwimConfiguration()):
        self.localNode = None
        self.config = swimConfig
        self.neighborManager = neighborManager
        self.zmqContext = zmqContext
        self.loop = loop
        self.degradedNodeTimers = {}
        
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

    async def sendReceive(self, targetNode: NodeInfo, data:str) -> str:
        targetNodeAddr = f"tcp://{targetNode.addr}"
        conn = self.zmqContext.socket(zmq.REQ)
        conn.connect(targetNodeAddr)
        await conn.send(data)
        received = await conn.recv()
        return received


    async def ping(self, targetNode: NodeInfo) -> NodeInfo:
        pingMsg = MessageFactory.newPingMessage( self.localNode, targetNode )
        try:
            sendReceiveTask = self.loop.create_task(self.sendReceive(targetNode, pingMsg.SerializeToString()))
            receivedData = await asyncio.wait_for(sendReceiveTask, timeout = self.config.pingTimeout)
            #print(f"PING RECEIVED BACK: {receivedData}")
        except asyncio.TimeoutError:
            print("Ping request timed out")
            return None

        msg = MessageFactory.newFromString(receivedData)
        senderInfo = NodeInfo.fromProto(msg.senderInfo)
        if ((senderInfo.name != targetNode.name) 
                or (senderInfo.addr.host != targetNode.addr.host) 
                or (senderInfo.addr.port != targetNode.addr.port)):
            print(f"ERROR: Unexpected Node Info received:")
            print(f"{senderInfo.name} @ {senderInfo.addr}")
            print(f"{targetNode.name} @ {targetNode.addr}")
        if msg.message.Is(Ack.DESCRIPTOR):
            #print(f"RECEIVED PING ACK")
            return senderInfo
        else:
            print(f"ERROR: Unknown message type received")
            return None
    
    async def pingReq(self, targetNode: NodeInfo, bridgeNode: NodeInfo) -> NodeInfo:
        bridgeNodeAddr = f"tcp://{bridgeNode.addr}"
        pingReqMsg = MessageFactory.newPingRequestMessage( self.localNode, targetNode )
        try:
            sendReceiveTask = self.loop.create_task(self.sendReceive(targetNode, pingReqMsg.SerializeToString()))
            receivedData = await asyncio.wait_for(sendReceiveTask, timeout = self.config.pingReqTimeout)
            #print(f"PINGREQ RECEIVED BACK: {receivedData}")
        except asyncio.TimeoutError:
            print("Ping request timed out")
            return None

        msg = MessageFactory.newFromString(receivedData)
        senderInfo = NodeInfo.fromProto(msg.senderInfo)
        if ((senderInfo.name != targetNode.name) 
                or (senderInfo.addr.host != targetNode.addr.host) 
                or (senderInfo.addr.port != targetNode.addr.port)):
            print(f"ERROR: Unexpected Node Info received:")
            print(f"{senderInfo.name} @ {senderInfo.addr}")
            print(f"{targetNode.name} @ {targetNode.addr}")
        if msg.message.Is(Ack.DESCRIPTOR):
            print("RECEIVED PING REQ ACK")
            return senderInfo
        else:
            return None

    async def healthCheck(self, targetNode: NodeInfo) -> None:
        print(f"Performing health check for {targetNode.name} @ {targetNode.addr}")
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
            if nodeInfo.name != self.localNode.name ]
        if len(nodeInfos) == 0:
            print("Waiting for others to join cluster...")
            return None
            
        fanout = min(self.config.pingReqFanout, len(nodeInfos))
        bridgeNodes = random.sample(nodeInfos, k=fanout)
        tasks = [ self.loop.create_task(self.pingReq(targetNode, bridgeNode)) for bridgeNode in bridgeNodes ]
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

    async def start(self) -> None:
        while True:
            await asyncio.sleep(self.config.healthCheckPeriod)
            nodeInfos = [ nodeInfo for nodeInfo in self.neighborManager.getNodeInfos() 
                if (nodeInfo.name != self.localNode.name) and (nodeInfo.health == NodeHealth.ALIVE) ]

            if len(nodeInfos) == 0:
                print("Waiting for others to join cluster...")
            else:
                targetNode = random.choice(nodeInfos)
                await self.healthCheck(targetNode)
        return None

        

