from abc import (
    abstractmethod,
)
from concurrent.futures import CancelledError
from typing import (
    Any,
    cast,
    Dict,
    List,
)
import typing_extensions

from cancel_token import OperationCancelled
from eth_utils import encode_hex
from lahja import (
    BroadcastConfig,
)
from p2p.exceptions import (
    HandshakeFailure,
    NoConnectedPeers,
    PeerConnectionLost,
    WrongNetworkFailure,
    WrongGenesisFailure,
)
from p2p.peer import (
    IdentifiablePeer,
)
from p2p.protocol import (
    Command,
    _DecodedMsgType,
)
from p2p.p2p_proto import DisconnectReason

from trinity.endpoint import (
    TrinityEventBusEndpoint,
)
from trinity.protocol.common.events import (
    GetConnectedPeersRequest,
    GetConnectedPeersResponse,
    GetHighestTDPeerRequest,
    GetHighestTDPeerResponse,
)
from trinity.protocol.common.peer import (
    BaseChainPeer,
    BaseChainDTOPeer,
    BaseChainPeerFactory,
    BaseChainPeerPool,
    BaseChainProxyPeer,
)
from trinity.protocol.common.peer_pool_event_bus import (
    PeerPoolEventServer,
    BaseProxyPeerPool,
)

from .commands import (
    NewBlock,
    Status,
)
from .constants import MAX_HEADERS_FETCH
from .events import (
    SendBlockBodiesEvent,
    SendBlockHeadersEvent,
    SendNodeDataEvent,
    SendReceiptsEvent,
    GetBlockHeadersRequest,
    GetBlockHeadersResponse,
    GetBlockBodiesRequest,
    GetBlockBodiesResponse,
    GetNodeDataRequest,
    GetNodeDataResponse,
    GetReceiptsRequest,
    GetReceiptsResponse,
)
from .proto import (
    ETHProtocol,
    ETHProtocolLike,
    ProxyETHProtocol,
)
from .handlers import (
    ETHExchangeHandler,
    ETHExchangeHandlerLike,
    ProxyETHExchangeHandler,
)


class ETHPeerLike(typing_extensions.Protocol):

    @property
    @abstractmethod
    def sub_proto(self) -> ETHProtocolLike:
        pass

    @property
    @abstractmethod
    def requests(self) -> ETHExchangeHandlerLike:
        pass

    @property
    @abstractmethod
    def is_operational(self) -> bool:
        pass


class ETHPeer(BaseChainPeer):
    max_headers_fetch = MAX_HEADERS_FETCH

    _supported_sub_protocols = [ETHProtocol]
    sub_proto: ETHProtocol = None

    _requests: ETHExchangeHandler = None

    def get_extra_stats(self) -> List[str]:
        stats_pairs = self.requests.get_stats().items()
        return ['%s: %s' % (cmd_name, stats) for cmd_name, stats in stats_pairs]

    @property
    def requests(self) -> ETHExchangeHandler:
        if self._requests is None:
            self._requests = ETHExchangeHandler(self)
        return self._requests

    def handle_sub_proto_msg(self, cmd: Command, msg: _DecodedMsgType) -> None:
        if isinstance(cmd, NewBlock):
            msg = cast(Dict[str, Any], msg)
            header, _, _ = msg['block']
            actual_head = header.parent_hash
            actual_td = msg['total_difficulty'] - header.difficulty
            if actual_td > self.head_td:
                self.head_hash = actual_head
                self.head_td = actual_td

        super().handle_sub_proto_msg(cmd, msg)

    async def send_sub_proto_handshake(self) -> None:
        self.sub_proto.send_handshake(await self._local_chain_info)

    async def process_sub_proto_handshake(
            self, cmd: Command, msg: _DecodedMsgType) -> None:
        if not isinstance(cmd, Status):
            await self.disconnect(DisconnectReason.subprotocol_error)
            raise HandshakeFailure(f"Expected a ETH Status msg, got {cmd}, disconnecting")
        msg = cast(Dict[str, Any], msg)
        if msg['network_id'] != self.network_id:
            await self.disconnect(DisconnectReason.useless_peer)
            raise WrongNetworkFailure(
                f"{self} network ({msg['network_id']}) does not match ours "
                f"({self.network_id}), disconnecting"
            )
        genesis = await self.genesis
        if msg['genesis_hash'] != genesis.hash:
            await self.disconnect(DisconnectReason.useless_peer)
            raise WrongGenesisFailure(
                f"{self} genesis ({encode_hex(msg['genesis_hash'])}) does not "
                f"match ours ({genesis.hex_hash}), disconnecting"
            )
        self.head_td = msg['td']
        self.head_hash = msg['best_hash']


class ETHProxyPeer(BaseChainProxyPeer):
    """
    A ``ETHPeer`` that can be used from any process as a drop-in replacement for the actual
    peer that sits in the peer pool. Any action performed on the ``ETHProxyPeer`` is delegated
    to the actual peer in the pool.
    """

    def __init__(self,
                 dto_peer: BaseChainDTOPeer,
                 event_bus: TrinityEventBusEndpoint,
                 sub_proto: ProxyETHProtocol,
                 requests: ProxyETHExchangeHandler):

        super().__init__(dto_peer, event_bus)

        self.sub_proto = sub_proto
        self.requests = requests

    @classmethod
    def from_dto_peer(cls,
                      dto_peer: BaseChainDTOPeer,
                      event_bus: TrinityEventBusEndpoint,
                      broadcast_config: BroadcastConfig) -> 'ETHProxyPeer':
        return cls(
            dto_peer,
            event_bus,
            ProxyETHProtocol(dto_peer, event_bus, broadcast_config),
            ProxyETHExchangeHandler(dto_peer, event_bus, broadcast_config),
        )


class ETHPeerFactory(BaseChainPeerFactory):
    peer_class = ETHPeer


class ETHPeerPoolEventServer(PeerPoolEventServer[ETHPeer, BaseChainPeerPool]):
    """
    A request handler to handle ETH specific requests to the peer pool.
    """

    async def _run(self) -> None:
        self.logger.debug("Running ETHPeerPoolEventServer")
        self.run_daemon_task(self.handle_send_blockheader_events())
        self.run_daemon_task(self.handle_send_block_bodies_events())
        self.run_daemon_task(self.handle_send_nodes_events())
        self.run_daemon_task(self.handle_send_receipts_events())
        self.run_daemon_task(self.handle_get_block_headers_requests())
        self.run_daemon_task(self.handle_get_block_bodies_requests())
        self.run_daemon_task(self.handle_get_node_data_requests())
        self.run_daemon_task(self.handle_get_receipts_requests())
        self.run_daemon_task(self.handle_get_highest_td_peer_requests())
        self.run_daemon_task(self.handle_connected_peers_requests())
        await super()._run()

    async def handle_send_blockheader_events(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(SendBlockHeadersEvent)):
            try:
                peer = self.get_peer(ev.peer)
            except PeerConnectionLost:
                pass
            else:
                peer.sub_proto.send_block_headers(ev.headers)

    async def handle_send_block_bodies_events(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(SendBlockBodiesEvent)):
            try:
                peer = self.get_peer(ev.peer)
            except PeerConnectionLost:
                pass
            else:
                peer.sub_proto.send_block_bodies(ev.blocks)

    async def handle_send_nodes_events(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(SendNodeDataEvent)):
            try:
                peer = self.get_peer(ev.peer)
            except PeerConnectionLost:
                pass
            else:
                peer.sub_proto.send_node_data(ev.nodes)

    async def handle_send_receipts_events(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(SendReceiptsEvent)):
            try:
                peer = self.get_peer(ev.peer)
            except PeerConnectionLost:
                pass
            else:
                peer.sub_proto.send_receipts(ev.receipts)

    async def handle_get_block_headers_requests(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(GetBlockHeadersRequest)):
            try:
                peer = self.get_peer(ev.peer)
                headers = await peer.requests.get_block_headers(
                    ev.block_number_or_hash,
                    ev.max_headers,
                    ev.skip,
                    ev.reverse,
                    ev.timeout,
                )
            except TimeoutError as e:
                self.logger.debug("Timed out waiting on %s from %s", GetBlockHeadersRequest, peer)
                self.event_bus.broadcast(
                    GetBlockHeadersResponse(tuple(), e), ev.broadcast_config())
            except (CancelledError, OperationCancelled, PeerConnectionLost) as e:
                self.logger.warning("Error performing action on peer %s. Doing nothing.", peer)
                self.event_bus.broadcast(
                    GetBlockHeadersResponse(tuple(), e), ev.broadcast_config())
            else:
                self.event_bus.broadcast(GetBlockHeadersResponse(headers), ev.broadcast_config())

    async def handle_get_block_bodies_requests(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(GetBlockBodiesRequest)):
            try:
                peer = self.get_peer(ev.peer)
                bundles = await peer.requests.get_block_bodies(
                    ev.headers,
                    ev.timeout,
                )
            except TimeoutError as e:
                self.logger.debug("Timed out waiting on %s from %s", GetBlockBodiesRequest, peer)
                self.event_bus.broadcast(GetBlockBodiesResponse(tuple(), e), ev.broadcast_config())
            except (CancelledError, OperationCancelled, PeerConnectionLost) as e:
                self.logger.warning("Error performing action on peer %s. Doing nothing.", peer)
                self.event_bus.broadcast(GetBlockBodiesResponse(tuple(), e), ev.broadcast_config())
            else:
                self.logger.warning("Sending %s block bodies from %s", len(bundles), peer)
                self.event_bus.broadcast(GetBlockBodiesResponse(bundles), ev.broadcast_config())

    async def handle_get_node_data_requests(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(GetNodeDataRequest)):
            try:
                peer = self.get_peer(ev.peer)
                bundles = await peer.requests.get_node_data(ev.node_hashes, ev.timeout)
            except TimeoutError as e:
                self.logger.debug("Timed out waiting on %s from %s", GetNodeDataRequest, peer)
                self.event_bus.broadcast(GetNodeDataResponse(tuple(), e), ev.broadcast_config())
            except (CancelledError, OperationCancelled, PeerConnectionLost) as e:
                self.logger.warning("Error performing action on peer %s. Doing nothing.", peer)
                self.event_bus.broadcast(GetNodeDataResponse(tuple(), e), ev.broadcast_config())
            else:
                self.event_bus.broadcast(GetNodeDataResponse(bundles), ev.broadcast_config())

    async def handle_get_receipts_requests(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(GetReceiptsRequest)):
            try:
                peer = self.get_peer(ev.peer)
                bundles = await peer.requests.get_receipts(ev.headers, ev.timeout)
            except TimeoutError as e:
                self.logger.debug("Timed out waiting on %s from %s", GetReceiptsRequest, peer)
                self.event_bus.broadcast(GetReceiptsResponse(tuple(), e), ev.broadcast_config())
            except (CancelledError, OperationCancelled, PeerConnectionLost) as e:
                self.logger.warning(
                    "Error performing action on peer %s. Reason %s, Doing nothing.", peer, e)
                self.event_bus.broadcast(GetReceiptsResponse(tuple(), e), ev.broadcast_config())
            else:
                self.logger.warning("Sending %s receipts from %s", len(bundles), peer)
                self.event_bus.broadcast(GetReceiptsResponse(bundles), ev.broadcast_config())

    async def handle_get_highest_td_peer_requests(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(GetHighestTDPeerRequest)):
            self.logger.warning("Highest TD peer requested")
            try:
                highest_td_peer = self.peer_pool.highest_td_peer.to_dto()
            except NoConnectedPeers:
                # no peers are available right now
                highest_td_peer = None

            self.logger.warning("Sending Highest TD peer %s", highest_td_peer)
            self.event_bus.broadcast(
                GetHighestTDPeerResponse(highest_td_peer),
                ev.broadcast_config()
            )

    async def handle_connected_peers_requests(self) -> None:
        async for ev in self.wait_iter(self.event_bus.stream(GetConnectedPeersRequest)):

            peers = self.peer_pool.get_peers(ev.min_td)
            dto_peers = tuple(peer.to_dto() for peer in peers)
            self.event_bus.broadcast(
                GetConnectedPeersResponse(dto_peers),
                ev.broadcast_config()
            )


class ETHPeerPool(BaseChainPeerPool):
    peer_factory_class = ETHPeerFactory


class ETHProxyPeerPool(BaseProxyPeerPool[ETHProxyPeer]):

    def to_proxy_peer(self,
                      peer: IdentifiablePeer,
                      event_bus: TrinityEventBusEndpoint,
                      broadcast_config: BroadcastConfig) -> ETHProxyPeer:
        return ETHProxyPeer.from_dto_peer(
            cast(BaseChainDTOPeer, peer),
            self.event_bus,
            self.broadcast_config
        )
