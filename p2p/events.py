from typing import (
    Tuple,
    Type,
)

from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)
from p2p.peer import (
    IdentifiablePeer,
)
from p2p.protocol import (
    Command,
    PayloadType,
)


class BaseDiscoveryServiceResponse(BaseEvent):

    def __init__(self, error: Exception) -> None:
        self.error = error


class PeerCandidatesResponse(BaseDiscoveryServiceResponse):

    def __init__(self, candidates: Tuple[str, ...], error: Exception=None) -> None:
        super().__init__(error)
        self.candidates = candidates


class PeerCandidatesRequest(BaseRequestResponseEvent[PeerCandidatesResponse]):

    def __init__(self, max_candidates: int) -> None:
        self.max_candidates = max_candidates

    @staticmethod
    def expected_response_type() -> Type[PeerCandidatesResponse]:
        return PeerCandidatesResponse


class RandomBootnodeRequest(BaseRequestResponseEvent[PeerCandidatesResponse]):

    @staticmethod
    def expected_response_type() -> Type[PeerCandidatesResponse]:
        return PeerCandidatesResponse


class PeerCountResponse(BaseEvent):

    def __init__(self, peer_count: int) -> None:
        self.peer_count = peer_count


class PeerCountRequest(BaseRequestResponseEvent[PeerCountResponse]):

    @staticmethod
    def expected_response_type() -> Type[PeerCountResponse]:
        return PeerCountResponse


class ConnectToNodeCommand(BaseEvent):

    def __init__(self, node: str) -> None:
        self.node = node


class ConnectedPeersResponse(BaseEvent):

    def __init__(self, peers: Tuple[IdentifiablePeer, ...]) -> None:
        self.peers = peers


class ConnectedPeersRequest(BaseRequestResponseEvent[ConnectedPeersResponse]):

    @staticmethod
    def expected_response_type() -> Type[ConnectedPeersResponse]:
        return ConnectedPeersResponse


class PeerPoolMessageEvent(BaseEvent):

    def __init__(self, peer: IdentifiablePeer, cmd: Command, msg: PayloadType) -> None:
        self.peer = peer
        self.cmd = cmd
        self.msg = msg
