import pytest

from p2p.exceptions import PeerConnectionLost
from trinity.protocol.eth.commands import (
    BlockHeaders,
)
from tests.core.integration_test_helpers import (
    FakeAsyncHeaderDB,
    run_proxy_peer_pool,
    run_isolated_request_server,
    make_peer_pool_answer_event_bus_requests,
    make_peer_pool_relay_messages_on_event_bus,
)
from tests.core.peer_helpers import (
    get_directly_linked_peers,
    MockPeerPoolWithConnectedPeers,
)


@pytest.mark.asyncio
async def test_proxy_peer_requests(request,
                                   event_bus,
                                   other_event_bus,
                                   event_loop,
                                   chaindb_fresh,
                                   chaindb_20):

    client_peer, server_peer = await get_directly_linked_peers(
        request,
        event_loop,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db),
    )

    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=other_event_bus)
    client_proxy_peer_pool = await run_proxy_peer_pool(other_event_bus)
    await make_peer_pool_answer_event_bus_requests(other_event_bus, client_peer_pool)
    await make_peer_pool_relay_messages_on_event_bus(other_event_bus, client_peer_pool)

    server_peer_pool = MockPeerPoolWithConnectedPeers([server_peer], event_bus=event_bus)
    server_proxy_peer_pool = await run_proxy_peer_pool(event_bus)
    await make_peer_pool_answer_event_bus_requests(event_bus, server_peer_pool)
    await make_peer_pool_relay_messages_on_event_bus(event_bus, server_peer_pool)

    await run_isolated_request_server(server_proxy_peer_pool, chaindb_20.db)

    proxy_peer = await client_proxy_peer_pool.create_or_update_proxy_peer(client_peer.to_dto())

    assert proxy_peer.perf_metrics[BlockHeaders] == 0

    meta_data = await proxy_peer.get_meta_data()

    assert meta_data.head_td == 105
    assert meta_data.head_hash == b'_\x97\x80\x92\x8e\xffgvI\x15If?\xf7mT_Fsd\xca\xb4\x8eo\x95\xfc\x1bj\x91x\xb4\xbf'
    assert meta_data.max_headers_fetch == 192
    assert meta_data.head_number == None

    headers = await proxy_peer.requests.get_block_headers(0, 1, 0, False)

    assert len(headers) == 1
    block_header = headers[0]
    assert block_header.block_number == 0

    receipts = await proxy_peer.requests.get_receipts(headers)
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt[1][0] == block_header.receipt_root

    block_bundles = await proxy_peer.requests.get_block_bodies(headers)
    assert len(block_bundles) == 1
    first_bundle = block_bundles[0]
    assert first_bundle[1][0] == block_header.transaction_root

    node_data = await proxy_peer.requests.get_node_data((block_header.state_root,))
    assert node_data[0][0] == block_header.state_root


@pytest.mark.asyncio
async def test_proxy_peer_requests_without_server(request,
                                                  event_bus,
                                                  other_event_bus,
                                                  event_loop,
                                                  chaindb_fresh,
                                                  chaindb_20):

    client_peer, server_peer = await get_directly_linked_peers(
        request,
        event_loop,
        alice_headerdb=FakeAsyncHeaderDB(chaindb_fresh.db),
        bob_headerdb=FakeAsyncHeaderDB(chaindb_20.db),
    )

    client_peer_pool = MockPeerPoolWithConnectedPeers([client_peer], event_bus=other_event_bus)
    client_proxy_peer_pool = await run_proxy_peer_pool(other_event_bus)
    await make_peer_pool_answer_event_bus_requests(other_event_bus, client_peer_pool)
    await make_peer_pool_relay_messages_on_event_bus(other_event_bus, client_peer_pool)

    proxy_peer = await client_proxy_peer_pool.create_or_update_proxy_peer(client_peer.to_dto())

    with pytest.raises(PeerConnectionLost):
        await proxy_peer.requests.get_block_headers(0, 1, 0, False)

    with pytest.raises(PeerConnectionLost):
        await proxy_peer.requests.get_receipts(())

    with pytest.raises(PeerConnectionLost):
        await proxy_peer.requests.get_block_bodies(())

    with pytest.raises(PeerConnectionLost):
        await proxy_peer.requests.get_node_data(())
