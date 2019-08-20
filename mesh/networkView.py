from src.common.address import Address
from src.mesh.nodeInfo import NodeInfo, NodeHealth
import proto.mesh_messages_pb2 as messages_pb2
from typing import Dict, Sequence, Callable
import collections
import abc
import logging
import asyncio

class NetworkView(collections.abc.MutableMapping):
    
    @classmethod
    def fromProto(self, networkViewProto: messages_pb2.NetworkView):
        networkView = NetworkView()
        networkView.neighbors = { nodeInfoP.name: NodeInfo.fromProto(nodeInfoP) 
            for nodeInfoP in networkViewProto.nodes }
        return networkView

    def toProto(self) -> messages_pb2.NetworkView:
        protoNetworkView = messages_pb2.NetworkView()
        protoNetworkView.nodes.extend( [
            nodeInfo.toProto() for nodeInfo in self.neighbors.values()
        ] )
        return protoNetworkView
    
    def __init__(self):
        self._neighbors: Dict[str,NodeInfo] = {}
    
    def __getitem__(self, name:str):
        return self._neighbors.get(name)

    def __setitem__(self, name:str, nodeInfo: NodeInfo):
        self._neighbors[name] = nodeInfo
    
    def __delitem__(self, name:str):
        del self._neighbors[name]

    def __iter__(self):
        return iter(self._neighbors)

    def __len__(self):
        return len(self._neighbors)

    @property
    def neighbors(self):
        return self._neighbors

    @neighbors.setter
    def neighbors(self, newNeighbors: Dict[str,NodeInfo]):
        self._neighbors = newNeighbors

    