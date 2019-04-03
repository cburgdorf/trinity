from abc import abstractmethod
import enum
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
    DataTransferPeer,
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
    GetPeerPerfMetricsRequest,
)


class ChainInfo(NamedTuple):
    block_number: BlockNumber
    block_hash: Hash32
    total_difficulty: int
    genesis_hash: Hash32


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

    @to_dict
    def collect_performance_metrics(self) -> Iterable[Tuple[Type[Command], float]]:
        for exchange in self.requests:
            yield exchange.response_cmd_type, exchange.tracker.items_per_second_ema.value

    def to_dto(self) -> DataTransferPeer:
        return DataTransferPeer(
            self.remote.uri(),
            {
                ProxyPeerCacheKey.META_DATA: ChainPeerMetaData(
                    head_td=self.head_td,
                    head_hash=self.head_hash,
                    head_number=self.head_number,
                    max_headers_fetch=self.max_headers_fetch
                ),
                ProxyPeerCacheKey.PERF_METRICS: self.collect_performance_metrics()
            }
        )


class ProxyPeerCacheKey(enum.Enum):
    META_DATA = enum.auto()
    PERF_METRICS = enum.auto()


class BaseChainProxyPeer(BaseService):

    # FIXME
    requests: Any

    def __init__(self,
                 dto_peer: DataTransferPeer,
                 event_bus: TrinityEventBusEndpoint,
                 token: CancelToken = None):
        super().__init__(token)
        self.event_bus = event_bus
        self.dto_peer = dto_peer
        self._cache: Dict[Any, Any] = {}
        self.update_cache_item(
            ProxyPeerCacheKey.META_DATA, dto_peer.cache[ProxyPeerCacheKey.META_DATA])
        self.update_cache_item(
            ProxyPeerCacheKey.PERF_METRICS, dto_peer.cache[ProxyPeerCacheKey.PERF_METRICS])

    def update_cache_item(self, key: ProxyPeerCacheKey, val: Any) -> None:
        self._cache[key] = val

    def __str__(self) -> str:
        return f"{self.__class__.__name__} {self.dto_peer.uri}"

    @property
    def uri(self) -> str:
        return self.dto_peer.uri

    async def _run(self) -> None:
        self.logger.info("Starting Proxy Peer %s", self)
        await self.cancellation()

    async def disconnect(self, reason: DisconnectReason) -> None:
        self.event_bus.broadcast(
            DisconnectPeerEvent(self.dto_peer, reason),
            TO_NETWORKING_BROADCAST_CONFIG,
        )
        self.cancel_nowait()

    @property
    def cached_perf_metrics(self) -> Dict[Type[Command], float]:
        """
        Return the latest available performance metrics from cache.
        """
        return self._cache[ProxyPeerCacheKey.PERF_METRICS]

    @property
    def cached_meta_data(self) -> ChainPeerMetaData:
        """
        Return the latest available meta data from cache
        """
        return self._cache[ProxyPeerCacheKey.META_DATA]

    async def get_meta_data(self) -> ChainPeerMetaData:
        response = await self.wait(
            self.event_bus.request(
                GetPeerMetaDataRequest(self.dto_peer),
                TO_NETWORKING_BROADCAST_CONFIG
            ),
            # FIXME
            timeout=10.0
        )

        # TODO: Exception handling
        self.update_cache_item(ProxyPeerCacheKey.META_DATA, response.meta_data)

        return response.meta_data

    async def get_perf_metrics(self) -> Dict[Type[Command], float]:
        response = await self.wait(
            self.event_bus.request(
                GetPeerPerfMetricsRequest(self.dto_peer),
                TO_NETWORKING_BROADCAST_CONFIG
            ),
            # FIXME
            timeout=10.0
        )

        # TODO: Exception handling
        self.update_cache_item(ProxyPeerCacheKey.PERF_METRICS, response.metrics)

        return response.metrics


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
