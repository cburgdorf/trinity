from typing import (
    Tuple,
    TYPE_CHECKING,
)

from eth.rlp.headers import BlockHeader
from lahja import (
    BaseEvent,
)
from p2p.peer import (
    IdentifiablePeer,
)

if TYPE_CHECKING:
    from trinity.protocol.common.events import (
        BasePeerPoolMessageEvent,
    )
    from trinity.protocol.eth.peer import (
        ETHProxyPeer,
    )
    PeerPoolMessageEvent = BasePeerPoolMessageEvent[ETHProxyPeer]


class SendBlockHeadersEvent(BaseEvent):
    """
    An event to carry a *send block headers* action from the proxy peer to the actual peer
    in the peer pool.
    """
    def __init__(self,
                 peer: IdentifiablePeer,
                 headers: Tuple[BlockHeader, ...],
                 buffer_value: int,
                 request_id: int=None) -> None:
        self.peer = peer
        self.headers = headers
        self.buffer_value = buffer_value
        self.request_id = request_id
