from typing import (
    Any,
    Sequence)

from eth.exceptions import (
    HeaderNotFound,
)
from eth_typing import Hash32
from eth_utils import (
    to_hex,
)
from lahja import (
    BroadcastConfig,
    EndpointAPI,
)
from trie.exceptions import (
    MissingTrieNode,
)

from p2p.abc import CommandAPI, SessionAPI

from trinity.db.eth1.chain import BaseAsyncChainDB
from trinity.protocol.common.payloads import BlockHeadersQuery
from trinity.protocol.common.servers import (
    BaseIsolatedRequestServer,
    BasePeerRequestHandler,
)
from trinity.protocol.eth import commands
from trinity.protocol.eth.events import (
    GetBlockHeadersEvent,
    GetBlockBodiesEvent,
    GetNodeDataEvent,
    GetReceiptsEvent,
)
from trinity.protocol.eth.peer import (
    ETHProxyPeer,
)

from eth.rlp.receipts import Receipt
from eth.rlp.transactions import BaseTransactionFields

from trinity.protocol.eth.constants import (
    MAX_BODIES_FETCH,
    MAX_RECEIPTS_FETCH,
    MAX_STATE_FETCH,
)
from trinity.rlp.block_body import BlockBody


class ETHPeerRequestHandler(BasePeerRequestHandler):
    def __init__(self, db: BaseAsyncChainDB) -> None:
        self.db: BaseAsyncChainDB = db

    async def handle_get_block_headers(
            self,
            peer: ETHProxyPeer,
            query: BlockHeadersQuery,
            request_id: int = None) -> None:
        self.logger.debug("%s requested headers: %s", peer, query)
        headers = await self.lookup_headers(query)
        self.logger.debug2("Replying to %s with %d headers", peer, len(headers))
        peer.eth_api.send_block_headers(headers, request_id=request_id)

    async def handle_get_block_bodies(self,
                                      peer: ETHProxyPeer,
                                      query: Sequence[Hash32],
                                      request_id: int = None) -> None:
        block_hashes = query

        self.logger.debug2("%s requested bodies for %d blocks", peer, len(block_hashes))
        bodies = []
        # Only serve up to MAX_BODIES_FETCH items in every request.
        for block_hash in block_hashes[:MAX_BODIES_FETCH]:
            try:
                header = await self.db.coro_get_block_header_by_hash(block_hash)
            except HeaderNotFound:
                self.logger.debug(
                    "%s asked for a block with a header we don't have: %s", peer, to_hex(block_hash)
                )
                continue
            try:
                transactions = await self.db.coro_get_block_transactions(
                    header,
                    BaseTransactionFields,
                )
            except MissingTrieNode as exc:
                self.logger.debug(
                    "%s asked for block transactions we don't have: %s, "
                    "due to %r",
                    peer,
                    to_hex(block_hash),
                    exc,
                )
                continue
            try:
                uncles = await self.db.coro_get_block_uncles(header.uncles_hash)
            except HeaderNotFound as exc:
                self.logger.debug(
                    "%s asked for a block with uncles we don't have: %s", peer, exc
                )
                continue
            bodies.append(BlockBody(transactions, uncles))
        self.logger.debug2("Replying to %s with %d block bodies", peer, len(bodies))
        peer.eth_api.send_block_bodies(bodies, request_id)

    async def handle_get_receipts(self,
                                  peer: ETHProxyPeer,
                                  query: Sequence[Hash32],
                                  request_id: int = None) -> None:
        block_hashes = query

        self.logger.debug2("%s requested receipts for %d blocks", peer, len(block_hashes))
        receipts = []
        # Only serve up to MAX_RECEIPTS_FETCH items in every request.
        for block_hash in block_hashes[:MAX_RECEIPTS_FETCH]:
            try:
                header = await self.db.coro_get_block_header_by_hash(block_hash)
            except HeaderNotFound:
                self.logger.debug(
                    "%s asked receipts for a block we don't have: %s", peer, to_hex(block_hash)
                )
                continue
            try:
                block_receipts = await self.db.coro_get_receipts(header, Receipt)
            except MissingTrieNode as exc:
                self.logger.debug(
                    "%s asked for block receipts we don't have: %s, "
                    "due to %r",
                    peer,
                    to_hex(block_hash),
                    exc,
                )
                continue
            receipts.append(block_receipts)
        self.logger.debug2("Replying to %s with receipts for %d blocks", peer, len(receipts))
        peer.eth_api.send_receipts(receipts, request_id)

    async def handle_get_node_data(self,
                                   peer: ETHProxyPeer,
                                   query: Sequence[Hash32], request_id: int = None) -> None:
        node_hashes = query

        self.logger.debug2("%s requested %d trie nodes", peer, len(node_hashes))
        nodes = []
        missing_node_hashes = []
        # Only serve up to MAX_STATE_FETCH items in every request.
        for node_hash in node_hashes[:MAX_STATE_FETCH]:
            try:
                node = await self.db.coro_get(node_hash)
            except KeyError:
                missing_node_hashes.append(node_hash)
            else:
                nodes.append(node)
        self.logger.debug2("Replying to %s with %d trie nodes", peer, len(nodes))
        if len(missing_node_hashes):
            self.logger.debug(
                "%s asked for %d trie nodes that we don't have, out of request for %d",
                peer,
                len(missing_node_hashes),
                len(node_hashes),
            )
        peer.eth_api.send_node_data(tuple(nodes), request_id)


class ETHRequestServer(BaseIsolatedRequestServer):
    """
    Monitor commands from peers, to identify inbound requests that should receive a response.
    Handle those inbound requests by querying our local database and replying.
    """

    def __init__(
            self,
            event_bus: EndpointAPI,
            broadcast_config: BroadcastConfig,
            db: BaseAsyncChainDB) -> None:
        super().__init__(
            event_bus,
            broadcast_config,
            (GetBlockHeadersEvent, GetBlockBodiesEvent, GetNodeDataEvent, GetReceiptsEvent),
        )
        self._handler = ETHPeerRequestHandler(db)

    async def _handle_msg(self,
                          session: SessionAPI,
                          cmd: CommandAPI[Any]) -> None:

        self.logger.debug2("Peer %s requested %s", session, cmd)
        peer = ETHProxyPeer.from_session(session, self.event_bus, self.broadcast_config)

        if isinstance(cmd, commands.GetBlockHeadersV65):
            await self._handler.handle_get_block_headers(peer, cmd.payload)
        elif isinstance(cmd, commands.GetBlockHeadersV66):
            await self._handler.handle_get_block_headers(
                peer,
                cmd.payload.query,
                cmd.payload.request_id
            )
        elif isinstance(cmd, commands.GetBlockBodiesV65):
            await self._handler.handle_get_block_bodies(peer, cmd.payload)
        elif isinstance(cmd, commands.GetBlockBodiesV66):
            await self._handler.handle_get_block_bodies(
                peer,
                cmd.payload.query,
                cmd.payload.request_id
            )
        elif isinstance(cmd, commands.GetReceiptsV65):
            await self._handler.handle_get_receipts(peer, cmd.payload)
        elif isinstance(cmd, commands.GetReceiptsV66):
            await self._handler.handle_get_receipts(peer, cmd.payload.query, cmd.payload.request_id)
        elif isinstance(cmd, commands.GetNodeDataV65):
            await self._handler.handle_get_node_data(peer, cmd.payload)
        elif isinstance(cmd, commands.GetNodeDataV66):
            await self._handler.handle_get_node_data(
                peer,
                cmd.payload.query,
                cmd.payload.request_id
            )
        else:
            self.logger.debug("%s msg not handled yet, needs to be implemented", cmd)
