"""送信量を一定に保つためのトークンバケット（バックエンド側）"""

import asyncio
import time


class AsyncTokenBucket:
    """1秒あたり rate 単位まで消費を許し、一度に溜められる量は burst まで"""

    def __init__(self, rate: float, burst: float) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens = burst
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, amount: float) -> None:
        """amount 単位を消費できるまで待つ。burst を超える要求は受け付けない"""
        if amount > self._burst:
            raise ValueError("一度に要求できる量は burst 以下にしてください")
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self._burst, self._tokens + (now - self._updated) * self._rate
                )
                self._updated = now
                if self._tokens >= amount:
                    self._tokens -= amount
                    return
                await asyncio.sleep((amount - self._tokens) / self._rate)
