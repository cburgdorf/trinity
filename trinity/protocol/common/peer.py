from abc import abstractmethod
import operator
import random
from typing import (
    Any,
    Dict,
    List,
    Iterable,
    NamedTuple,
    Tuple,
    Type,
)

from cancel_token import CancelToken
from eth_typing import (
    BlockNumber,
    Hash32,
)

from eth_utils import to_dict
from eth_utils.toolz import groupby

from eth.constants import GENESIS_BLOCK_NUMBER
from eth.rlp.headers import BlockHeader
from eth.vm.base import BaseVM

from p2p.exceptions import NoConnectedPeers
from p2p.peer import (
    BasePeer,
    BasePeerFactory,
    IdentifiablePeer,
)
from p2p.peer_pool import (
    BasePeerPool,
)
from p2p.protocol import Command
from p2p.p2p_proto import DisconnectReason
from p2p.service import BaseService

from trinity.constants import TO_NETWORKING_BROADCAST_CONFIG
from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.endpoint import TrinityEventBusEndpoint
from trinity.protocol.common.handlers import BaseChainExchangeHandler

from .boot import DAOCheckBootManager
from .context import ChainContext
from .events import (
    ChainPeerMetaData,
    DisconnectPeerEvent,
    GetPeerMetaDataRequest,
)


class ChainInfo(NamedTuple):
    block_number: BlockNumber
    block_hash: Hash32
    total_difficulty: int
    genesis_hash: Hash32


class BaseChainDTOPeer(NamedTuple):
    # TODO: Calling code might worker under the assumption these values are live
    # which they aren't for the DTO. Potential footgun?
    uri: str
    head_td: int
    head_hash: Hash32
    head_number: BlockNumber
    max_headers_fetch: int
    perf_metrics: Dict[Type[Command], float]


class BaseChainPeer(BasePeer):
    boot_manager_class = DAOCheckBootManager
    context: ChainContext

    head_td: int = None
    head_hash: Hash32 = None
    head_number: BlockNumber = None

    @property
    @abstractmethod
    def requests(self) -> BaseChainExchangeHandler:
        pass

    @property
    @abstractmethod
    def max_headers_fetch(self) -> int:
        pass

    @property
    def headerdb(self) -> BaseAsyncHeaderDB:
        return self.context.headerdb

    @property
    def network_id(self) -> int:
        return self.context.network_id

    @property
    def vm_configuration(self) -> Tuple[Tuple[int, Type[BaseVM]], ...]:
        return self.context.vm_configuration

    @property
    async def genesis(self) -> BlockHeader:
        genesis_hash = await self.wait(
            self.headerdb.coro_get_canonical_block_hash(BlockNumber(GENESIS_BLOCK_NUMBER)))
        return await self.wait(self.headerdb.coro_get_block_header_by_hash(genesis_hash))

    @property
    async def _local_chain_info(self) -> ChainInfo:
        genesis = await self.genesis
        head = await self.wait(self.headerdb.coro_get_canonical_head())
        total_difficulty = await self.headerdb.coro_get_score(head.hash)
        return ChainInfo(
            block_number=head.block_number,
            block_hash=head.hash,
            total_difficulty=total_difficulty,
            genesis_hash=genesis.hash,
        )

    # We take a snapshot of these metrics whenever a peer is send across as a DTO peer.
    # This means there's no guarantee about the freshness of these values on the DTO side.
    # An alternative would be to have each process do its own performance tracking?
    @to_dict
    def _collect_performance_metrics(self) -> Iterable[Tuple[Type[Command], float]]:
        for exchange in self.requests:
            yield exchange.response_cmd_type, exchange.tracker.items_per_second_ema.value

    def to_dto(self) -> IdentifiablePeer:
        return BaseChainDTOPeer(
            self.remote.uri(),
            self.head_td,
            self.head_hash,
            self.head_number,
            self.max_headers_fetch,
            self._collect_performance_metrics(),
        )

class BaseChainProxyPeer(BaseService):

    # FIXME
    requests: Any

    def __init__(self,
                 dto_peer: BaseChainDTOPeer,
                 event_bus: TrinityEventBusEndpoint,
                 token: CancelToken = None):
        super().__init__(token)
        self.event_bus = event_bus
        self.dto_peer = dto_peer

    # TODO: Wondering if we should only allow one-time read and throw if code
    # tries to read again them again. I think the proper fix may be to just group
    # all of these behind one async API that fetches this info. It just convenient
    # to try to mimic the API for now.
    @property
    def head_td(self) -> int:
        return self.dto_peer.head_td

    @property
    def head_hash(self) -> Hash32:
        return self.dto_peer.head_hash

    @property
    def header_number(self) -> BlockNumber:
        return self.dto_peer.head_number

    @property
    def max_headers_fetch(self) -> int:
        return self.dto_peer.max_headers_fetch

    def __str__(self) -> str:
        return f"{self.__class__.__name__} {self.dto_peer.uri}"

    @property
    def uri(self) -> str:
        return self.dto_peer.uri

    @property
    def perf_metrics(self) -> Dict[Type[Command], float]:
        return self.dto_peer.perf_metrics

    async def _run(self) -> None:
        self.logger.info("Starting Proxy Peer %s", self)
        await self.cancellation()

    async def disconnect(self, reason: DisconnectReason) -> None:
        self._is_operational = False
        self.event_bus.broadcast(
            DisconnectPeerEvent(self.dto_peer, reason),
            TO_NETWORKING_BROADCAST_CONFIG,
        )

    async def get_meta_data(self) -> ChainPeerMetaData:
        response = await self.wait(
            self.event_bus.request(
                GetPeerMetaDataRequest(self.dto_peer),
                TO_NETWORKING_BROADCAST_CONFIG
            ),
            #FIXME
            timeout=10.0
        )

        return response.meta_data


class BaseChainPeerFactory(BasePeerFactory):
    context: ChainContext
    peer_class: Type[BaseChainPeer]


class BaseChainPeerPool(BasePeerPool):
    connected_nodes: Dict[str, BaseChainPeer]  # type: ignore
    peer_factory_class: Type[BaseChainPeerFactory]

    @property
    def highest_td_peer(self) -> BaseChainPeer:
        peers = tuple(self.connected_nodes.values())
        if not peers:
            raise NoConnectedPeers()
        peers_by_td = groupby(operator.attrgetter('head_td'), peers)
        max_td = max(peers_by_td.keys())
        return random.choice(peers_by_td[max_td])

    def get_peers(self, min_td: int) -> List[BaseChainPeer]:
        # TODO: Consider turning this into a method that returns an AsyncIterator, to make it
        # harder for callsites to get a list of peers while making blocking calls, as those peers
        # might disconnect in the meantime.
        peers = tuple(self.connected_nodes.values())
        return [peer for peer in peers if peer.head_td >= min_td]
