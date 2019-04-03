from lahja import (
    BaseEvent,
)
from p2p.peer import (
    DataTransferPeer,
)


class GetSumRequest(BaseEvent):

    def __init__(self, peer: DataTransferPeer, a: int, b: int) -> None:
        self.peer = peer
        self.a = a
        self.b = b
