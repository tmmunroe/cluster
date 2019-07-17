# @generated by generate_proto_mypy_stubs.py.  Do not edit!
import sys
from common_messages_pb2 import (
    Address as common_messages_pb2___Address,
)

from google.protobuf.any_pb2 import (
    Any as google___protobuf___any_pb2___Any,
)

from google.protobuf.descriptor import (
    EnumDescriptor as google___protobuf___descriptor___EnumDescriptor,
)

from google.protobuf.internal.containers import (
    RepeatedCompositeFieldContainer as google___protobuf___internal___containers___RepeatedCompositeFieldContainer,
)

from google.protobuf.message import (
    Message as google___protobuf___message___Message,
)

from typing import (
    Iterable as typing___Iterable,
    List as typing___List,
    Optional as typing___Optional,
    Text as typing___Text,
    Tuple as typing___Tuple,
    cast as typing___cast,
)

from typing_extensions import (
    Literal as typing_extensions___Literal,
)


class NodeInfoProto(google___protobuf___message___Message):
    class NodeHealth(int):
        DESCRIPTOR: google___protobuf___descriptor___EnumDescriptor = ...
        @classmethod
        def Name(cls, number: int) -> str: ...
        @classmethod
        def Value(cls, name: str) -> NodeInfoProto.NodeHealth: ...
        @classmethod
        def keys(cls) -> typing___List[str]: ...
        @classmethod
        def values(cls) -> typing___List[NodeInfoProto.NodeHealth]: ...
        @classmethod
        def items(cls) -> typing___List[typing___Tuple[str, NodeInfoProto.NodeHealth]]: ...
    DEAD = typing___cast(NodeHealth, 0)
    SUSPECT = typing___cast(NodeHealth, 2)
    ALIVE = typing___cast(NodeHealth, 4)

    name = ... # type: typing___Text
    incarnation = ... # type: int
    health = ... # type: NodeInfoProto.NodeHealth

    @property
    def addr(self) -> common_messages_pb2___Address: ...

    @property
    def gossip_addr(self) -> common_messages_pb2___Address: ...

    @property
    def swim_addr(self) -> common_messages_pb2___Address: ...

    def __init__(self,
        name : typing___Optional[typing___Text] = None,
        addr : typing___Optional[common_messages_pb2___Address] = None,
        gossip_addr : typing___Optional[common_messages_pb2___Address] = None,
        swim_addr : typing___Optional[common_messages_pb2___Address] = None,
        incarnation : typing___Optional[int] = None,
        health : typing___Optional[NodeInfoProto.NodeHealth] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> NodeInfoProto: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    if sys.version_info >= (3,):
        def HasField(self, field_name: typing_extensions___Literal[u"addr",u"gossip_addr",u"swim_addr"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"addr",u"gossip_addr",u"health",u"incarnation",u"name",u"swim_addr"]) -> None: ...
    else:
        def HasField(self, field_name: typing_extensions___Literal[u"addr",b"addr",u"gossip_addr",b"gossip_addr",u"swim_addr",b"swim_addr"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[b"addr",b"gossip_addr",b"health",b"incarnation",b"name",b"swim_addr"]) -> None: ...

class Ping(google___protobuf___message___Message):
    targetName = ... # type: typing___Text

    @property
    def targetAddress(self) -> common_messages_pb2___Address: ...

    def __init__(self,
        targetName : typing___Optional[typing___Text] = None,
        targetAddress : typing___Optional[common_messages_pb2___Address] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Ping: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    if sys.version_info >= (3,):
        def HasField(self, field_name: typing_extensions___Literal[u"targetAddress"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"targetAddress",u"targetName"]) -> None: ...
    else:
        def HasField(self, field_name: typing_extensions___Literal[u"targetAddress",b"targetAddress"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[b"targetAddress",b"targetName"]) -> None: ...

class PingReq(google___protobuf___message___Message):
    targetName = ... # type: typing___Text

    @property
    def targetAddress(self) -> common_messages_pb2___Address: ...

    def __init__(self,
        targetName : typing___Optional[typing___Text] = None,
        targetAddress : typing___Optional[common_messages_pb2___Address] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> PingReq: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    if sys.version_info >= (3,):
        def HasField(self, field_name: typing_extensions___Literal[u"targetAddress"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"targetAddress",u"targetName"]) -> None: ...
    else:
        def HasField(self, field_name: typing_extensions___Literal[u"targetAddress",b"targetAddress"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[b"targetAddress",b"targetName"]) -> None: ...

class Ack(google___protobuf___message___Message):
    targetName = ... # type: typing___Text

    @property
    def targetAddress(self) -> common_messages_pb2___Address: ...

    def __init__(self,
        targetName : typing___Optional[typing___Text] = None,
        targetAddress : typing___Optional[common_messages_pb2___Address] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Ack: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    if sys.version_info >= (3,):
        def HasField(self, field_name: typing_extensions___Literal[u"targetAddress"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"targetAddress",u"targetName"]) -> None: ...
    else:
        def HasField(self, field_name: typing_extensions___Literal[u"targetAddress",b"targetAddress"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[b"targetAddress",b"targetName"]) -> None: ...

class Gossip(google___protobuf___message___Message):
    remainingSends = ... # type: int
    gossipId = ... # type: typing___Text

    @property
    def originNode(self) -> NodeInfoProto: ...

    @property
    def message(self) -> google___protobuf___any_pb2___Any: ...

    def __init__(self,
        originNode : typing___Optional[NodeInfoProto] = None,
        remainingSends : typing___Optional[int] = None,
        gossipId : typing___Optional[typing___Text] = None,
        message : typing___Optional[google___protobuf___any_pb2___Any] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> Gossip: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    if sys.version_info >= (3,):
        def HasField(self, field_name: typing_extensions___Literal[u"message",u"originNode"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[u"gossipId",u"message",u"originNode",u"remainingSends"]) -> None: ...
    else:
        def HasField(self, field_name: typing_extensions___Literal[u"message",b"message",u"originNode",b"originNode"]) -> bool: ...
        def ClearField(self, field_name: typing_extensions___Literal[b"gossipId",b"message",b"originNode",b"remainingSends"]) -> None: ...

class NetworkView(google___protobuf___message___Message):

    @property
    def nodes(self) -> google___protobuf___internal___containers___RepeatedCompositeFieldContainer[NodeInfoProto]: ...

    def __init__(self,
        nodes : typing___Optional[typing___Iterable[NodeInfoProto]] = None,
        ) -> None: ...
    @classmethod
    def FromString(cls, s: bytes) -> NetworkView: ...
    def MergeFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    def CopyFrom(self, other_msg: google___protobuf___message___Message) -> None: ...
    if sys.version_info >= (3,):
        def ClearField(self, field_name: typing_extensions___Literal[u"nodes"]) -> None: ...
    else:
        def ClearField(self, field_name: typing_extensions___Literal[b"nodes"]) -> None: ...
