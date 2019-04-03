from typing import (
    List,
    Tuple,
    Type,
    TYPE_CHECKING,
)

from eth.rlp.blocks import BaseBlock
from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt
from eth_typing import (
    BlockIdentifier,
    Hash32,
)
from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)
from p2p.peer import (
    DataTransferPeer,
)

from trinity.protocol.common.events import (
    BasePeerPoolMessageEvent,
)
from trinity.protocol.common.types import (
    BlockBodyBundles,
    NodeDataBundles,
    ReceiptsBundles,
)

if TYPE_CHECKING:
    from trinity.protocol.eth.peer import (
        ETHProxyPeer,
    )
    PeerPoolMessageEvent = BasePeerPoolMessageEvent[ETHProxyPeer]


# RAW PROTOCOL EVENTS

class SendBlockHeadersEvent(BaseEvent):
    """
    An event to carry a *send block header* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self, peer: DataTransferPeer, headers: Tuple[BlockHeader, ...]) -> None:
        self.peer = peer
        self.headers = headers


class SendBlockBodiesEvent(BaseEvent):
    """
    An event to carry a *send block bodies* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self, peer: DataTransferPeer, blocks: List[BaseBlock]) -> None:
        self.peer = peer
        self.blocks = blocks


class SendNodeDataEvent(BaseEvent):
    """
    An event to carry a *send node data* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self, peer: DataTransferPeer, nodes: Tuple[bytes, ...]) -> None:
        self.peer = peer
        self.nodes = nodes


class SendReceiptsEvent(BaseEvent):
    """
    An event to carry a *send receipts* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self, peer: DataTransferPeer, receipts: List[List[Receipt]]) -> None:
        self.peer = peer
        self.receipts = receipts


# EXCHANGE HANDLER REQUEST / RESPONSE PAIRS

class GetBlockHeadersResponse(BaseEvent):

    def __init__(self,
                 headers: Tuple[BlockHeader, ...],
                 exception: Exception = None) -> None:
        self.headers = headers
        self.exception = exception


class GetBlockHeadersRequest(BaseRequestResponseEvent[GetBlockHeadersResponse]):

    def __init__(self,
                 peer: DataTransferPeer,
                 block_number_or_hash: BlockIdentifier,
                 max_headers: int,
                 skip: int,
                 reverse: bool,
                 timeout: float) -> None:
        self.peer = peer
        self.block_number_or_hash = block_number_or_hash
        self.max_headers = max_headers
        self.skip = skip
        self.reverse = reverse
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetBlockHeadersResponse]:
        return GetBlockHeadersResponse


class GetBlockBodiesResponse(BaseEvent):

    def __init__(self,
                 bundles: BlockBodyBundles,
                 exception: Exception = None) -> None:
        self.bundles = bundles
        self.exception = exception


class GetBlockBodiesRequest(BaseRequestResponseEvent[GetBlockBodiesResponse]):

    def __init__(self,
                 peer: DataTransferPeer,
                 headers: Tuple[BlockHeader, ...],
                 timeout: float) -> None:
        self.peer = peer
        self.headers = headers
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetBlockBodiesResponse]:
        return GetBlockBodiesResponse


class GetNodeDataResponse(BaseEvent):

    def __init__(self,
                 bundles: NodeDataBundles,
                 exception: Exception = None) -> None:
        self.bundles = bundles
        self.exception = exception


class GetNodeDataRequest(BaseRequestResponseEvent[GetNodeDataResponse]):

    def __init__(self,
                 peer: DataTransferPeer,
                 node_hashes: Tuple[Hash32, ...],
                 timeout: float) -> None:
        self.peer = peer
        self.node_hashes = node_hashes
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetNodeDataResponse]:
        return GetNodeDataResponse


class GetReceiptsResponse(BaseEvent):

    def __init__(self,
                 bundles: ReceiptsBundles,
                 exception: Exception = None) -> None:
        self.bundles = bundles
        self.exception = exception


class GetReceiptsRequest(BaseRequestResponseEvent[GetReceiptsResponse]):

    def __init__(self,
                 peer: DataTransferPeer,
                 headers: Tuple[BlockHeader, ...],
                 timeout: float) -> None:
        self.peer = peer
        self.headers = headers
        self.timeout = timeout

    @staticmethod
    def expected_response_type() -> Type[GetReceiptsResponse]:
        return GetReceiptsResponse
