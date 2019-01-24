from typing import (
    cast,
    Iterable,
)
from lahja import (
    BroadcastConfig,
    Endpoint,
)
from p2p.peer import (
    BasePeer,
    BasePeerContext,
    BasePeerFactory,
    IdentifiablePeer,
)
from p2p.peer_pool import (
    BasePeerPool,
)
from p2p.peer_pool_event_bus_request_handler import (
    BasePeerPoolEventBusRequestHandler,
)
from p2p.protocol import (
    Command,
    _DecodedMsgType,
)

from .events import GetSumRequest
from .proto import ParagonProtocol, RemoteParagonProtocol


class ParagonRemotePeer:
    """
    A ``ParagonPeer`` that can be used from any process and can be used as a drop-in
    replacement for the real peer. Any action performed on the ``ParagonRemotePeer`` is delegated
    to the real peer behind the scenes.
    """

    def __init__(self, sub_proto: RemoteParagonProtocol):
        self.sub_proto = sub_proto


def to_remote_peer(dto_peer: IdentifiablePeer,
                   event_bus: Endpoint,
                   broadcast_config: BroadcastConfig) -> ParagonRemotePeer:
        return ParagonRemotePeer(RemoteParagonProtocol(dto_peer, event_bus, broadcast_config))


class ParagonPeer(BasePeer):
    _supported_sub_protocols = [ParagonProtocol]
    sub_proto: ParagonProtocol = None

    async def send_sub_proto_handshake(self) -> None:
        pass

    async def process_sub_proto_handshake(
            self, cmd: Command, msg: _DecodedMsgType) -> None:
        pass

    async def do_sub_proto_handshake(self) -> None:
        pass


class ParagonContext(BasePeerContext):
    # nothing magic here.  Simply an example of how the context class can be
    # used to store data specific to a certain peer class.
    paragon: str = "paragon"


class ParagonPeerFactory(BasePeerFactory):
    peer_class = ParagonPeer
    context: ParagonContext


class ParagonPeerPoolEventBusRequestHandler(BasePeerPoolEventBusRequestHandler):

    async def _run(self) -> None:
        self.logger.info("Running ParagonPeerPoolEventBusRequestHandler")
        self.run_daemon_task(self.handle_get_sum_requests())
        await super()._run()

    async def handle_get_sum_requests(self) -> None:
        async for req in self.wait_iter(self._event_bus.stream(GetSumRequest)):
            peer = self.get_peer_from_pool(req.dto_peer)
            peer.sub_proto.send_get_sum(req.a, req.b)

    def get_peer_from_pool(self, dto_peer: IdentifiablePeer) -> ParagonPeer:
        return cast(ParagonPeer, self._peer_pool.super_shitty_peer_lookup(dto_peer))


class ParagonPeerPool(BasePeerPool):
    peer_factory_class = ParagonPeerFactory
    context: ParagonContext


class ParagonMockPeerPoolWithConnectedPeers(ParagonPeerPool):
    def __init__(self, peers: Iterable[ParagonPeer]) -> None:
        super().__init__(privkey=None, context=None)
        for peer in peers:
            self.connected_nodes[peer.remote] = peer

    async def _run(self) -> None:
        raise NotImplementedError("This is a mock PeerPool implementation, you must not _run() it")
