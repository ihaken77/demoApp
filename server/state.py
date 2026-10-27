"""フロントが把握している負荷の希望状態"""

from dataclasses import dataclass

from server.limits import CONNECTIONS_RANGE, RPS_RANGE


@dataclass
class LoadState:
    """通常の通信と負荷の通信の ON/OFF、最大 RPS、最大接続数"""

    baseline_on: bool = False
    load_on: bool = False
    rps: int = RPS_RANGE.default
    connections: int = CONNECTIONS_RANGE.default
