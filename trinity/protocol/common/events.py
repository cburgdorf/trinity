from dataclasses import (
    dataclass,
)
from typing import (
    Dict,
    NamedTuple,
    Tuple,
    Type,
)

from eth_typing import (
    BlockNumber,
    Hash32,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from p2p.abc import CommandAPI, NodeAPI
from p2p.disconnect import DisconnectReason
from p2p.typing import Payload


@dataclass
class ConnectToNodeCommand(BaseEvent):
    """
    Event that wraps a node URI that the pool should connect to.
    """
    remote: NodeAPI


@dataclass
class PeerCountResponse(BaseEvent):
    """
    Response event that wraps the count of peers connected to the pool.
    """

    peer_count: int


class PeerCountRequest(BaseRequestResponseEvent[PeerCountResponse]):
    """
    Request event to get the count of peers connected to the pool.
    """

    @staticmethod
    def expected_response_type() -> Type[PeerCountResponse]:
        return PeerCountResponse


@dataclass
class DisconnectPeerEvent(BaseEvent):
    """
    Event broadcasted when we want to disconnect from a peer
    """
    remote: NodeAPI
    reason: DisconnectReason


@dataclass
class PeerJoinedEvent(BaseEvent):
    """
    Event broadcasted when a new peer joined the pool.
    """
    remote: NodeAPI


@dataclass
class PeerLeftEvent(BaseEvent):
    """
    Event broadcasted when a peer left the pool.
    """
    remote: NodeAPI


class ChainPeerMetaData(NamedTuple):
    head_td: int
    head_hash: Hash32
    head_number: BlockNumber
    max_headers_fetch: int


@dataclass
class GetPeerMetaDataResponse(BaseEvent):

    meta_data: ChainPeerMetaData
    error: Exception = None


@dataclass
class GetPeerMetaDataRequest(BaseRequestResponseEvent[GetPeerMetaDataResponse]):

    remote: NodeAPI

    @staticmethod
    def expected_response_type() -> Type[GetPeerMetaDataResponse]:
        return GetPeerMetaDataResponse


@dataclass
class GetPeerPerfMetricsResponse(BaseEvent):

    metrics: Dict[Type[CommandAPI], float]
    error: Exception = None


@dataclass
class GetPeerPerfMetricsRequest(BaseRequestResponseEvent[GetPeerPerfMetricsResponse]):

    remote: NodeAPI

    @staticmethod
    def expected_response_type() -> Type[GetPeerPerfMetricsResponse]:
        return GetPeerPerfMetricsResponse


@dataclass
class GetHighestTDPeerResponse(BaseEvent):

    remote: NodeAPI
    error: Exception = None


@dataclass
class GetHighestTDPeerRequest(BaseRequestResponseEvent[GetHighestTDPeerResponse]):

    timeout: float

    @staticmethod
    def expected_response_type() -> Type[GetHighestTDPeerResponse]:
        return GetHighestTDPeerResponse


@dataclass
class GetConnectedPeersResponse(BaseEvent):

    remotes: Tuple[NodeAPI, ...]


class GetConnectedPeersRequest(BaseRequestResponseEvent[GetConnectedPeersResponse]):

    @staticmethod
    def expected_response_type() -> Type[GetConnectedPeersResponse]:
        return GetConnectedPeersResponse


@dataclass
class PeerPoolMessageEvent(BaseEvent):
    """
    Base event for all peer messages that are relayed on the event bus. The events are mapped
    to individual subclasses for every different ``cmd`` to allow efficient consumption through
    the event bus.
    """
    remote: NodeAPI
    cmd: CommandAPI
    msg: Payload
