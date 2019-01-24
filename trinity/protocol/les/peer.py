from typing import (
    Any,
    cast,
    Dict,
    List,
    Union,
)
import typing_extensions

from eth_typing import (
    BlockNumber,
    Hash32,
)

from eth_utils import encode_hex
from lahja import (
    BroadcastConfig,
    Endpoint
)
from p2p.exceptions import (
    HandshakeFailure,
)
from p2p.peer import (
    IdentifiablePeer,
)
from p2p.peer_pool_event_bus_request_handler import (
    BasePeerPoolEventBusRequestHandler,
)
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
    Announce,
    Status,
    StatusV2,
)
from .constants import (
    MAX_HEADERS_FETCH,
)
from .events import (
    SendBlockHeadersEvent,
)
from .proto import (
    LESProtocol,
    LESProtocolLike,
    LESProtocolV2,
    RemoteLESProtocol,
)
from .handlers import LESExchangeHandler


class LESPeerLike(typing_extensions.Protocol):

    @property
    def sub_proto(self) -> LESProtocolLike:
        pass


class LESPeer(BaseChainPeer):
    max_headers_fetch = MAX_HEADERS_FETCH

    _supported_sub_protocols = [LESProtocol, LESProtocolV2]
    sub_proto: LESProtocol = None

    _requests: LESExchangeHandler = None

    def get_extra_stats(self) -> List[str]:
        stats_pairs = self.requests.get_stats().items()
        return ['%s: %s' % (cmd_name, stats) for cmd_name, stats in stats_pairs]

    @property
    def requests(self) -> LESExchangeHandler:
        if self._requests is None:
            self._requests = LESExchangeHandler(self)
        return self._requests

    def handle_sub_proto_msg(self, cmd: Command, msg: _DecodedMsgType) -> None:
        head_info = cast(Dict[str, Union[int, Hash32, BlockNumber]], msg)
        if isinstance(cmd, Announce):
            self.head_td = cast(int, head_info['head_td'])
            self.head_hash = cast(Hash32, head_info['head_hash'])
            self.head_number = cast(BlockNumber, head_info['head_number'])

        super().handle_sub_proto_msg(cmd, msg)

    async def send_sub_proto_handshake(self) -> None:
        self.sub_proto.send_handshake(await self._local_chain_info)

    async def process_sub_proto_handshake(
            self, cmd: Command, msg: _DecodedMsgType) -> None:
        if not isinstance(cmd, (Status, StatusV2)):
            await self.disconnect(DisconnectReason.subprotocol_error)
            raise HandshakeFailure(f"Expected a LES Status msg, got {cmd}, disconnecting")
        msg = cast(Dict[str, Any], msg)
        if msg['networkId'] != self.network_id:
            await self.disconnect(DisconnectReason.useless_peer)
            raise HandshakeFailure(
                f"{self} network ({msg['networkId']}) does not match ours "
                f"({self.network_id}), disconnecting"
            )
        genesis = await self.genesis
        if msg['genesisHash'] != genesis.hash:
            await self.disconnect(DisconnectReason.useless_peer)
            raise HandshakeFailure(
                f"{self} genesis ({encode_hex(msg['genesisHash'])}) does not "
                f"match ours ({genesis.hex_hash}), disconnecting"
            )
        # Eventually we might want to keep connections to peers where we are the only side serving
        # data, but right now both our chain syncer and the Peer.boot() method expect the remote
        # to reply to header requests, so if they don't we simply disconnect here.
        if 'serveHeaders' not in msg:
            await self.disconnect(DisconnectReason.useless_peer)
            raise HandshakeFailure(f"{self} doesn't serve headers, disconnecting")
        self.head_td = msg['headTd']
        self.head_hash = msg['headHash']
        self.head_number = msg['headNum']


class LESRemotePeer:
    """
    An ``LESPeer`` that can be used from any process as a drop-in replacement for the real
    peer that lives in the peer pool process. Any action performed on the ``ParagonRemotePeer`` is
    delegated to the real peer behind the scenes.
    """

    def __init__(self, sub_proto: RemoteLESProtocol):
        self.sub_proto = sub_proto


def to_remote_peer(dto_peer: IdentifiablePeer,
                   event_bus: Endpoint,
                   broadcast_config: BroadcastConfig) -> LESRemotePeer:
    return LESRemotePeer(RemoteLESProtocol(dto_peer, event_bus, broadcast_config))


class LESPeerFactory(BaseChainPeerFactory):
    peer_class = LESPeer


class LESPeerPoolEventBusRequestHandler(BasePeerPoolEventBusRequestHandler):

    async def _run(self) -> None:
        self.logger.info("Running LESPeerPoolEventBusRequestHandler")
        self.run_daemon_task(self.handle_send_blockheader_events())
        await super()._run()

    async def handle_send_blockheader_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendBlockHeadersEvent)):
            peer = self.get_peer_from_pool(ev.dto_peer)
            peer.sub_proto.send_block_headers(ev.headers, ev.buffer_value, ev.request_id)

    def get_peer_from_pool(self, dto_peer: IdentifiablePeer) -> LESPeer:
        return cast(LESPeer, self._peer_pool.super_shitty_peer_lookup(dto_peer))


class LESPeerPool(BaseChainPeerPool):
    peer_factory_class = LESPeerFactory
