
from src.mesh.nodeInfo import NodeInfo

class ClusterMessageFactory():

    @classmethod
    def newJoinRequestMessage(cls, nodeInfo: NodeInfo):
        pass
    
    @classmethod
    def newJoinAcceptMessage(cls, nodeInfo: NodeInfo, nodeInfos):
        pass

    @classmethod
    def newFromString(self, str):
        pass