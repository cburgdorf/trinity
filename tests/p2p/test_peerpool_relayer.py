import asyncio

import pytest

from p2p.events import PeerPoolMessageEvent
from p2p.peer_pool import PeerPoolMessageRelayer

from p2p.tools.paragon import (
    get_directly_linked_peers,
    GetSum,
    ParagonMockPeerPoolWithConnectedPeers,
)


@pytest.mark.asyncio
async def test_relays_pool_events(request, event_loop, event_bus):

    received_events = []
    event_bus.subscribe(PeerPoolMessageEvent, lambda ev: received_events.append(ev))

    alice, bob = await get_directly_linked_peers(
        request,
        event_loop,
    )

    peer_pool = ParagonMockPeerPoolWithConnectedPeers([alice, bob])

    relayer = PeerPoolMessageRelayer(peer_pool, event_bus)
    asyncio.ensure_future(relayer.run())

    def finalizer():
        event_loop.run_until_complete(relayer.cancel())
    request.addfinalizer(finalizer)

    alice.sub_proto.send_get_sum(10, 20)
    bob.sub_proto.send_get_sum(30, 40)

    await asyncio.sleep(0.01)
    assert len(received_events) == 2

    alice_event = received_events[0]
    assert isinstance(alice_event.cmd, GetSum)
    assert alice_event.msg == {'a': 10, 'b': 20}
    # TODO: Why are alice and bob twisted?!
    assert alice_event.peer.uri == bob.to_dto().uri

    bob_event = received_events[1]
    assert isinstance(bob_event.cmd, GetSum)
    assert bob_event.msg == {'a': 30, 'b': 40}
    assert bob_event.peer.uri == alice.to_dto().uri
