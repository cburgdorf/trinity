from abc import (
    abstractmethod,
)
from typing import (
    AsyncIterator,
    cast,
    Dict,
    Generic,
    TypeVar,
)
from cancel_token import (
    CancelToken,
)
from lahja import (
    BroadcastConfig,
    Endpoint,
)

from p2p.exceptions import (
    PeerConnectionLost,
)
from p2p.kademlia import (
    from_uris,
)
from p2p.peer import (
    BasePeer,
    DataTransferPeer,
    PeerSubscriber,
)
from p2p.peer_pool import (
    BasePeerPool,
)
from p2p.protocol import (
    Command,
)
from p2p.service import (
    BaseService,
)

from trinity.endpoint import (
    TrinityEventBusEndpoint,
)

from .events import (
    BasePeerPoolMessageEvent,
    ConnectToNodeCommand,
    DisconnectPeerEvent,
    GetConnectedPeersRequest,
    GetHighestTDPeerRequest,
    PeerCountRequest,
    PeerCountResponse,
    PeerPoolMessageEvent,
    PeerJoinedEvent,
    PeerLeftEvent,
)
from .peer import (
    BaseChainProxyPeer,
)


TPeer = TypeVar('TPeer', bound=BasePeer)
TPeerPool = TypeVar('TPeerPool', bound=BasePeerPool)


class PeerPoolEventServer(BaseService, Generic[TPeer, TPeerPool]):
    """
    Base request handler that listens for requests on the event bus that should be delegated to
    the peer pool to either perform an action or return some response. Subclasses should extend
    this class with custom event handlers.
    """

    def __init__(self,
                 event_bus: Endpoint,
                 peer_pool: TPeerPool,
                 token: CancelToken = None) -> None:
        super().__init__(token)
        self.peer_pool = peer_pool
        self.event_bus = event_bus

    async def accept_connect_commands(self) -> None:
        async for command in self.wait_iter(self.event_bus.stream(ConnectToNodeCommand)):
            self.logger.debug('Received request to connect to %s', command.node)
            self.run_task(self.peer_pool.connect_to_nodes(from_uris([command.node])))

    async def handle_peer_count_requests(self) -> None:
        async for req in self.wait_iter(self.event_bus.stream(PeerCountRequest)):
            self.event_bus.broadcast(
                PeerCountResponse(len(self.peer_pool)),
                req.broadcast_config()
            )

    async def handle_disconnect_peer_events(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(DisconnectPeerEvent)):
            try:
                peer = self.get_peer(ev.peer)
            except PeerConnectionLost:
                pass
            else:
                peer.disconnect(ev.reason)

    async def _run(self) -> None:
        self.logger.debug("Running PeerPoolEventServer")

        self.run_daemon_task(self.handle_peer_count_requests())
        self.run_daemon_task(self.accept_connect_commands())
        self.run_daemon_task(self.handle_disconnect_peer_events())

        await self.cancel_token.wait()

    def get_peer(self, dto_peer: DataTransferPeer) -> TPeer:

        try:
            peer = self.peer_pool.connected_nodes[dto_peer.uri]
        except KeyError:
            self.logger.debug("Peer %s does not exist in the pool anymore", dto_peer.uri)
            raise PeerConnectionLost()
        else:
            if not peer.is_operational:
                self.logger.debug("Peer %s is not operational when selecting from pool", peer)
                raise PeerConnectionLost()
            else:
                return cast(TPeer, peer)


DefaultPeerPoolEventServer = PeerPoolEventServer[BasePeer, BasePeerPool]


class BasePeerPoolMessageRelayer(BaseService, PeerSubscriber):
    """
    Base class that relays peer pool events on the event bus. The following events are exposed
    on the event bus:
      - Incoming peer messages -> :class:`~trinity.protocol.common.events.PeerPoolMessageEvent`
      - Peer joined the pool -> :class:`~trinity.protocol.common.events.PeerJoinedEvent`
      - Peer left the pool -> :class:`~trinity.protocol.common.events.PeerLeftEvent`

    Subclasses should extend this to relay events specific to other peer pools.
    """

    msg_queue_maxsize: int = 2000

    subscription_msg_types = frozenset({Command})

    def __init__(self,
                 peer_pool: BasePeerPool,
                 event_bus: Endpoint,
                 token: CancelToken = None) -> None:
        super().__init__(token)
        self._event_bus = event_bus
        self._peer_pool = peer_pool

    async def _run(self) -> None:
        self.logger.info("Running BasePeerPoolMessageRelayer")

        with self.subscribe(self._peer_pool):
            while self.is_operational:
                peer, cmd, msg = await self.wait(self.msg_queue.get())
                self._event_bus.broadcast(PeerPoolMessageEvent(peer.to_dto(), cmd, msg))

    def register_peer(self, peer: BasePeer) -> None:
        self.logger.info("Broadcasting PeerJoinedEvent for %s", peer)
        self._event_bus.broadcast(PeerJoinedEvent(peer.to_dto()))

    def deregister_peer(self, peer: BasePeer) -> None:
        self.logger.info("Broadcasting PeerLeftEvent for %s", peer)
        self._event_bus.broadcast(PeerLeftEvent(peer.to_dto()))


TProxyPeer = TypeVar('TProxyPeer', bound=BaseChainProxyPeer)


class BaseProxyPeerPool(BaseService, Generic[TProxyPeer]):

    def __init__(self,
                 event_bus: TrinityEventBusEndpoint,
                 broadcast_config: BroadcastConfig,
                 token: CancelToken=None):
        super().__init__(token)
        self.event_bus = event_bus
        self.broadcast_config = broadcast_config
        self.connected_peers: Dict[str, TProxyPeer] = dict()

    async def stream_existing_and_joining_peers(self) -> AsyncIterator[TProxyPeer]:
        async for proxy_peer in self.wait_iter(self.get_connected_peers()):
            yield proxy_peer

        async for new_proxy_peer in self.wait_iter(self.stream_peers_joining()):
            yield new_proxy_peer

    # FIXME: This isn't robust. We need to create a uuid or maybe id(BasePeer) and include it
    # when we raise PeerJoinedEvent and PeerLeftEvent so that we can do the bookkeeping
    # not just based on the uri. Because what might happen is that these events arrive out
    # of order and then if a peer joins and leaves multiple times we may remove a peer that
    # should stay or add a peer that should be removed.
    # Also, write tests against the BaseProxyPeerPool to ensure this stuff works
    async def stream_peers_joining(self) -> AsyncIterator[TProxyPeer]:
        async for ev in self.wait_iter(self.event_bus.stream(PeerJoinedEvent)):
            yield await self.create_or_update_proxy_peer(ev.peer)

    async def watch_peers_joining(self) -> None:
        async for peer in self.wait_iter(self.stream_peers_joining()):
            # We just want to consume the AsyncIterator
            self.logger.info("New Proxy Peer joined %s", peer)

    async def stream_peer_messages(self) -> AsyncIterator[BasePeerPoolMessageEvent[TProxyPeer]]:
        async for ev in self.wait_iter(self.event_bus.stream(PeerPoolMessageEvent)):
            proxy_peer = await self.create_or_update_proxy_peer(ev.peer)
            yield BasePeerPoolMessageEvent(proxy_peer, ev.cmd, ev.msg)

    async def watch_peers_leaving(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(PeerLeftEvent)):
            if ev.peer.uri not in self.connected_peers:
                self.logger.warning("Wanted to remove peer but it is missing %s", ev.peer.uri)
            else:
                proxy_peer = self.connected_peers.pop(ev.peer.uri)
                # FIXME: This looks problematic. If we assume events can arrive out of order then
                # We might cancel a peer while the data is just getting ready to be consumed by
                # the peer. What we really want is peer.run_task(...) to throw for new tasks but
                # keep existing tasks running at least for some extended period.
                await proxy_peer.cancel()
                self.logger.warning("Removed proxy peer from proxy pool %s", ev.peer.uri)

    async def get_highest_td_peer(self) -> TProxyPeer:
        response = await self.wait(
            self.event_bus.request(
                GetHighestTDPeerRequest(2),
                self.broadcast_config
            ),
            timeout=10.0
        )
        if response.dto_peer is None:
            return None

        return await self.create_or_update_proxy_peer(response.dto_peer)

    async def get_connected_peers(self, min_td: int = 0) -> AsyncIterator[TProxyPeer]:
        response = await self.wait(self.event_bus.request(GetConnectedPeersRequest()))
        for peer in response.dto_peers:
            yield await self.create_or_update_proxy_peer(peer)

    @abstractmethod
    def to_proxy_peer(self,
                      peer: DataTransferPeer,
                      event_bus: TrinityEventBusEndpoint,
                      broadcast_config: BroadcastConfig) -> TProxyPeer:
        pass

    async def create_or_update_proxy_peer(self, dto_peer: DataTransferPeer) -> TProxyPeer:
        if dto_peer.uri in self.connected_peers:
            for key, val in dto_peer.cache.items():
                self.connected_peers[dto_peer.uri].update_cache_item(key, val)
        else:
            proxy_peer = self.to_proxy_peer(
                dto_peer,
                self.event_bus,
                self.broadcast_config
            )
            self.connected_peers[dto_peer.uri] = proxy_peer
            self.run_child_service(proxy_peer)
            await proxy_peer.events.started.wait()

        return self.connected_peers[dto_peer.uri]

    async def _run(self) -> None:
        self.run_daemon_task(self.watch_peers_joining())
        self.run_daemon_task(self.watch_peers_leaving())
        await self.cancellation()
