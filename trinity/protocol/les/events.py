from typing import (
    Tuple,
)

from eth.rlp.headers import BlockHeader
from lahja import (
    BaseEvent,
)
from p2p.peer import (
    IdentifiablePeer,
)


class SendBlockHeadersEvent(BaseEvent):

    def __init__(self,
                 dto_peer: IdentifiablePeer,
                 headers: Tuple[BlockHeader, ...],
                 buffer_value: int,
                 request_id: int=None) -> None:
        self.dto_peer = dto_peer
        self.headers = headers
        self.buffer_value = buffer_value
        self.request_id = request_id
