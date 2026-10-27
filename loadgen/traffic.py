"""バックエンドから AWS フロントへ送る通信の生成（通常の通信と負荷の通信）"""

import asyncio
import logging

import httpx

from loadgen.pacing import AsyncTokenBucket
from server.limits import CONNECTIONS_RANGE, MAX_INJECTED_MBPS
from server.throughput import TRAFFIC_KIND_HEADER

logger = logging.getLogger(__name__)

# 通常の通信（ベースライン）の帯域。約 20Mbps を流す
BASELINE_MBPS = 20
BASELINE_CHUNK_BYTES = 256 * 1024

# 負荷の 1 リクエストのサイズ。15KB × 最大リクエスト/秒 500 ≈ 61Mbps で、注入の上限 60Mbps に届く
REQUEST_BYTES = 15 * 1024

BASELINE_BODY = bytes(BASELINE_CHUNK_BYTES)
REQUEST_BODY = bytes(REQUEST_BYTES)


def mbps_to_bytes_per_sec(mbps: float) -> float:
    return mbps * 1_000_000 / 8


class TrafficRunner:
    """通常の通信と負荷の通信を、フロントの希望状態に合わせて開始・停止する"""

    def __init__(
        self,
        receiver_url: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=receiver_url,
            timeout=10.0,
            transport=transport,
            # 負荷の接続数の上限に合わせる（通常の通信用に 1 本足す）
            limits=httpx.Limits(max_connections=CONNECTIONS_RANGE.maximum + 1),
        )
        self._baseline_task: asyncio.Task | None = None
        self._load_task: asyncio.Task | None = None
        self._load_params: tuple[int, int] | None = None

    @property
    def baseline_running(self) -> bool:
        return self._baseline_task is not None

    @property
    def load_running(self) -> bool:
        return self._load_task is not None

    def set_baseline(self, on: bool) -> None:
        if on and not self.baseline_running:
            bucket = AsyncTokenBucket(
                rate=mbps_to_bytes_per_sec(BASELINE_MBPS), burst=BASELINE_CHUNK_BYTES
            )
            self._baseline_task = asyncio.create_task(self._baseline_loop(bucket))
            logger.info("通常の通信を開始しました")
        elif not on and self.baseline_running:
            self._baseline_task.cancel()
            self._baseline_task = None
            logger.info("通常の通信を停止しました")

    def set_load(self, on: bool, rps: int, connections: int) -> None:
        if not on:
            if self.load_running:
                self._stop_load()
                logger.info("負荷の通信を停止しました")
            return
        if self.load_running and self._load_params == (rps, connections):
            return
        # 設定が変わったら、いったん止めて新しい設定で始める
        self._stop_load()
        # 注入負荷の帯域は、この値を超えないようコードで固定する
        byte_bucket = AsyncTokenBucket(
            rate=mbps_to_bytes_per_sec(MAX_INJECTED_MBPS), burst=REQUEST_BYTES
        )
        # 1 件ずつ待つと sleep の誤差でリクエスト数が目標より下がるため、リクエスト/秒の 1/50 件ずつまとめて渡す
        request_bucket = AsyncTokenBucket(rate=rps, burst=max(1, rps // 50))
        self._load_task = asyncio.create_task(
            self._load_supervisor(connections, request_bucket, byte_bucket)
        )
        self._load_params = (rps, connections)
        logger.info(
            "負荷の通信を開始しました（リクエスト/秒 %d / 接続数 %d）", rps, connections
        )

    async def aclose(self) -> None:
        if self._baseline_task is not None:
            self._baseline_task.cancel()
            self._baseline_task = None
        if self._load_task is not None:
            self._stop_load()
        await self._client.aclose()

    def _stop_load(self) -> None:
        if self._load_task is not None:
            self._load_task.cancel()
            self._load_task = None
        self._load_params = None

    async def _load_supervisor(
        self,
        connections: int,
        request_bucket: AsyncTokenBucket,
        byte_bucket: AsyncTokenBucket,
    ) -> None:
        # 接続数の分だけ同時に送る。リクエスト数と帯域の上限は全員で共有する
        workers = [
            asyncio.create_task(self._load_worker(request_bucket, byte_bucket))
            for _ in range(connections)
        ]
        try:
            await asyncio.gather(*workers)
        finally:
            for worker in workers:
                worker.cancel()

    async def _load_worker(
        self, request_bucket: AsyncTokenBucket, byte_bucket: AsyncTokenBucket
    ) -> None:
        while True:
            await request_bucket.acquire(1)
            await byte_bucket.acquire(REQUEST_BYTES)
            await self._post(REQUEST_BODY, "load")

    async def _baseline_loop(self, bucket: AsyncTokenBucket) -> None:
        while True:
            await bucket.acquire(BASELINE_CHUNK_BYTES)
            await self._post(BASELINE_BODY, "baseline")

    async def _post(self, body: bytes, kind: str) -> None:
        try:
            # 種別を付けて送る。フロントが通常の通信と負荷の通信を分けて実測できるようにする
            await self._client.post(
                "/receive", content=body, headers={TRAFFIC_KIND_HEADER: kind}
            )
        except httpx.HTTPError as error:
            # 一時的な失敗では止めず、次の送信で続ける
            logger.warning("フロントへの送信に失敗しました: %s", type(error).__name__)
            await asyncio.sleep(0.5)
