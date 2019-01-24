from typing import (
    Any,
    cast,
    Dict,
    List,
)
import typing_extensions

from eth_utils import encode_hex
from lahja import (
    BroadcastConfig,
    Endpoint,
)
from p2p.exceptions import (
    HandshakeFailure,
    WrongNetworkFailure,
    WrongGenesisFailure,
)
from p2p.peer import IdentifiablePeer
from p2p.peer_pool_event_bus_request_handler import BasePeerPoolEventBusRequestHandler
from p2p.p2p_proto import DisconnectReason
from p2p.protocol import (
    Command,
    _DecodedMsgType,
)

from trinity.protocol.common.peer import (
    BaseChainPeer,
    BaseChainPeerFactory,
    BaseChainPeerPool,
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
)
from .proto import ETHProtocol, ETHProtocolLike, RemoteETHProtocol
from .handlers import ETHExchangeHandler


class ETHPeerLike(typing_extensions.Protocol):

    @property
    def sub_proto(self) -> ETHProtocolLike:
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


class ETHRemotePeer:
    """
    An ``ETHPeer`` that can be used from any process as a drop-in replacement for the real
    peer that lives in the peer pool process. Any action performed on the ``ParagonRemotePeer`` is
    delegated to the real peer behind the scenes.
    """

    def __init__(self, sub_proto: RemoteETHProtocol):
        self.sub_proto = sub_proto


def to_remote_peer(dto_peer: IdentifiablePeer,
                   event_bus: Endpoint,
                   broadcast_config: BroadcastConfig) -> ETHRemotePeer:
    return ETHRemotePeer(RemoteETHProtocol(dto_peer, event_bus, broadcast_config))


class ETHPeerFactory(BaseChainPeerFactory):
    peer_class = ETHPeer


class ETHPeerPoolEventBusRequestHandler(BasePeerPoolEventBusRequestHandler):

    async def _run(self) -> None:
        self.logger.info("Running ETHPeerPoolEventBusRequestHandler")
        self.run_daemon_task(self.handle_send_blockheader_events())
        self.run_daemon_task(self.handle_send_block_bodies_events())
        self.run_daemon_task(self.handle_send_nodes_events())
        self.run_daemon_task(self.handle_send_receipts_events())
        await super()._run()

    async def handle_send_blockheader_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendBlockHeadersEvent)):
            peer = self.get_peer_from_pool(ev.dto_peer)
            peer.sub_proto.send_block_headers(ev.headers)

    async def handle_send_block_bodies_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendBlockBodiesEvent)):
            peer = self.get_peer_from_pool(ev.dto_peer)
            peer.sub_proto.send_block_bodies(ev.blocks)

    async def handle_send_nodes_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendNodeDataEvent)):
            peer = self.get_peer_from_pool(ev.dto_peer)
            peer.sub_proto.send_node_data(ev.nodes)

    async def handle_send_receipts_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendReceiptsEvent)):
            peer = self.get_peer_from_pool(ev.dto_peer)
            peer.sub_proto.send_receipts(ev.receipts)

    def get_peer_from_pool(self, dto_peer: IdentifiablePeer) -> ETHPeer:
        return cast(ETHPeer, self._peer_pool.super_shitty_peer_lookup(dto_peer))


class ETHPeerPool(BaseChainPeerPool):
    peer_factory_class = ETHPeerFactory
