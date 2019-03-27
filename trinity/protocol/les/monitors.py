from trinity.protocol.common.monitors import BaseChainTipMonitor
from trinity.protocol.les import commands
from trinity.protocol.les.peer import LESProxyPeer


class LightChainTipMonitor(BaseChainTipMonitor[LESProxyPeer]):
    subscription_msg_types = frozenset({commands.Announce})
