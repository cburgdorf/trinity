from typing import (
    Generic,
    NamedTuple,
    Tuple,
    Type,
    TypeVar,
)

from eth_typing import (
    BlockNumber,
    Hash32,
)
from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from p2p.peer import (
    IdentifiablePeer,
)
from p2p.p2p_proto import (
    DisconnectReason,
)
from p2p.protocol import (
    Command,
    PayloadType,
)


class ConnectToNodeCommand(BaseEvent):
    """
    Event that wraps a node URI that the pool should connect to.
    """

    def __init__(self, node: str) -> None:
        self.node = node


class PeerCountResponse(BaseEvent):
    """
    Response event that wraps the count of peers connected to the pool.
    """

    def __init__(self, peer_count: int) -> None:
        self.peer_count = peer_count


class PeerCountRequest(BaseRequestResponseEvent[PeerCountResponse]):
    """
    Request event to get the count of peers connected to the pool.
    """

    @staticmethod
    def expected_response_type() -> Type[PeerCountResponse]:
        return PeerCountResponse


TPeer = TypeVar('TPeer')


class BasePeerPoolMessageEvent(BaseEvent, Generic[TPeer]):
    """
    Event broadcasted when a peer sends a command.
    """

    def __init__(self, peer: TPeer, cmd: Command, msg: PayloadType) -> None:
        self.peer = peer
        self.cmd = cmd
        self.msg = msg


# It appears BasePeerPoolMessageEvent[IdentifiablePeer] can't be pickled
# so we have to use a non-generic version here.
class PeerPoolMessageEvent(BaseEvent):
    """
    Event broadcasted when a peer sends a command.
    """

    def __init__(self, peer: IdentifiablePeer, cmd: Command, msg: PayloadType) -> None:
        self.peer = peer
        self.cmd = cmd
        self.msg = msg


class PeerJoinedEvent(BaseEvent):
    """
    Event broadcasted when a new peer joined the pool.
    """

    def __init__(self, peer: IdentifiablePeer) -> None:
        self.peer = peer


class PeerLeftEvent(BaseEvent):
    """
    Event broadcasted when a peer left the pool.
    """

    def __init__(self, peer: IdentifiablePeer) -> None:
        self.peer = peer


class DisconnectPeerEvent(BaseEvent):
    """
    Event broadcasted when we want to disconnect from a peer
    """

    def __init__(self, peer: IdentifiablePeer, reason: DisconnectReason) -> None:
        self.peer = peer
        self.reason = reason


class ChainPeerMetaData(NamedTuple):
    head_td: int
    head_hash: Hash32
    head_number: BlockNumber
    max_headers_fetch: int


class GetPeerMetaDataResponse(BaseEvent):

    def __init__(self,
                 meta_data: ChainPeerMetaData) -> None:
        self.meta_data = meta_data


class GetPeerMetaDataRequest(BaseRequestResponseEvent[GetPeerMetaDataResponse]):

    def __init__(self, peer: IdentifiablePeer) -> None:
        self.peer = peer

    @staticmethod
    def expected_response_type() -> Type[GetPeerMetaDataResponse]:
        return GetPeerMetaDataResponse


class GetHighestTDPeerResponse(BaseEvent):

    def __init__(self,
                 dto_peer: IdentifiablePeer) -> None:
        self.dto_peer = dto_peer


class GetHighestTDPeerRequest(BaseRequestResponseEvent[GetHighestTDPeerResponse]):

    def __init__(self,
                 timeout: float) -> None:
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetHighestTDPeerResponse]:
        return GetHighestTDPeerResponse


class GetConnectedPeersResponse(BaseEvent):

    def __init__(self,
                 dto_peers: Tuple[IdentifiablePeer, ...]) -> None:
        self.dto_peers = dto_peers


class GetConnectedPeersRequest(BaseRequestResponseEvent[GetConnectedPeersResponse]):

    def __init__(self, min_td: int = 0) -> None:
        self.min_td = min_td

    @staticmethod
    def expected_response_type() -> Type[GetConnectedPeersResponse]:
        return GetConnectedPeersResponse
