from src.common.address import Address
from src.node.nodeInfo import NodeInfo, NodeHealth
import src.mesh.messages_pb2 as messages_pb2
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

    