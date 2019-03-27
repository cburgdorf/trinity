from trinity.protocol.common.monitors import BaseChainTipMonitor
from trinity.protocol.eth import commands
from trinity.protocol.eth.peer import ETHProxyPeer


class ETHChainTipMonitor(BaseChainTipMonitor[ETHProxyPeer]):
    subscription_msg_types = frozenset({commands.NewBlock})
