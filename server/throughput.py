"""フロントが受信している通信量の実測（直近の時間窓の平均）"""

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

# 通常の通信（テレメトリの定期送信）と負荷の通信（イベントの取り込み）の受け口。
# 種別は経路で分ける（独自ヘッダを付けると、通常の API と違って見えるため）
BASELINE_PATH = "/api/v1/telemetry/batches"
LOAD_PATH = "/api/v1/ingest/events"
KINDS = ("baseline", "load")
KIND_BY_PATH = {BASELINE_PATH: KINDS[0], LOAD_PATH: KINDS[1]}
# 種別が不明な受信は "other" にまとめる（合計には含める）
OTHER_KIND = "other"
# 集計する時間窓（秒）。短すぎると揺れ、長すぎると変化が遅れて見えるため 1 秒にする
WINDOW_SEC = 1.0


@dataclass(frozen=True)
class _Event:
    """1 回の受信。時刻・種別・受信バイト数・リクエスト数を持つ"""

    at: float
    kind: str
    nbytes: int
    requests: int


class ThroughputMeter:
    """直近 window_sec 秒の受信から、種別ごとの Mbps とリクエスト/秒を出す"""

    def __init__(
        self,
        window_sec: float = WINDOW_SEC,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._window = window_sec
        self._clock = clock
        self._events: deque[_Event] = deque()

    def record(self, kind: str, nbytes: int = 0, requests: int = 0) -> None:
        """受信を 1 件記録する。知らない種別は other にまとめる"""
        if kind not in KINDS:
            kind = OTHER_KIND
        self._events.append(_Event(self._clock(), kind, nbytes, requests))
        self._prune()

    def snapshot(self) -> dict:
        """直近の時間窓での Mbps とリクエスト/秒。合計は全種別の和"""
        self._prune()
        byte_totals = {kind: 0 for kind in (*KINDS, OTHER_KIND)}
        request_totals = {kind: 0 for kind in (*KINDS, OTHER_KIND)}
        for event in self._events:
            byte_totals[event.kind] += event.nbytes
            request_totals[event.kind] += event.requests

        def rates(kinds: tuple[str, ...]) -> dict[str, float]:
            nbytes = sum(byte_totals[kind] for kind in kinds)
            requests = sum(request_totals[kind] for kind in kinds)
            return {
                # Mbps は 1 秒あたりのビット数（10^6 bit）で表す
                "mbps": round(nbytes * 8 / 1_000_000 / self._window, 2),
                "rps": round(requests / self._window, 1),
            }

        return {
            "window_sec": self._window,
            "baseline": rates((KINDS[0],)),
            "load": rates((KINDS[1],)),
            "total": rates((*KINDS, OTHER_KIND)),
        }

    def _prune(self) -> None:
        """時間窓より古い受信は捨てる"""
        cutoff = self._clock() - self._window
        while self._events and self._events[0].at < cutoff:
            self._events.popleft()
