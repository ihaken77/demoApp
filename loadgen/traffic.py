"""バックエンドから AWS フロントへ送る通信の生成（通常の通信と負荷の通信）

実際のテレメトリ送信・イベント取り込みに近い見た目にするため、JSON の中身（レコード）、
送信のばらつき、サービスとしての User-Agent を使う。
"""

import asyncio
import json
import logging
import random
import uuid
from datetime import UTC, datetime, timedelta

import httpx

from loadgen.pacing import AsyncTokenBucket
from server.limits import CONNECTIONS_RANGE, MAX_INJECTED_MBPS
from server.throughput import BASELINE_PATH, LOAD_PATH

logger = logging.getLogger(__name__)

# 通常の通信（ベースライン）の帯域。約 20Mbps を流す
BASELINE_MBPS = 20
BASELINE_CHUNK_BYTES = 256 * 1024

# 負荷の 1 リクエストの平均サイズ。15KB × 最大リクエスト/秒 500 ≈ 61Mbps で、注入の上限 60Mbps に届く
REQUEST_BYTES = 15 * 1024
# 1 件あたりのサイズのばらつき（±この値）。毎回同じサイズだと機械的に見えるため
REQUEST_JITTER_BYTES = 1024
# 同じ大きさの本文を何通り用意しておくか（送るたびに組み立て直すと CPU を使うため、先に作っておく）
PAYLOAD_POOL_SIZE = 32
# 送信前の小さなばらつき（秒）。実際のクライアントの送信間隔に近づける
SEND_JITTER_SEC = 0.02
# 失敗・拒否のあとに待つ時間（秒）
RETRY_WAIT_SEC = 0.5

# 送信元のクライアント識別子。既定の httpx の User-Agent は付けない
USER_AGENT = "ingest-client/1.4.2"

# レコードに入れる値の候補（本文らしさのため）
SERVICES = ("checkout", "catalog", "inventory", "auth", "payment", "search")
ENDPOINTS = (
    "/v1/orders",
    "/v1/items",
    "/v1/stock",
    "/v1/session",
    "/v1/charges",
    "/v1/query",
)
METHODS = ("GET", "POST", "PUT")
STATUSES = (200, 200, 200, 201, 204, 400, 404, 500)
REGIONS = ("ap-northeast-1", "ap-northeast-3")


def mbps_to_bytes_per_sec(mbps: float) -> float:
    return mbps * 1_000_000 / 8


def make_payload(target_bytes: int, rng: random.Random) -> bytes:
    """target_bytes 前後の JSON 本文（テレメトリのバッチ）を作る"""
    now = datetime.now(UTC)
    records: list[dict] = []
    # 外枠のサイズを測ってから、レコードの大きさを足していく（毎回直列化すると遅いため）
    frame = {
        "batch_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "sent_at": now.isoformat(),
        "records": [],
    }
    size = len(json.dumps(frame, separators=(",", ":")))
    # 目標サイズに届いた時点で止める（1 件分の超過は許す）
    while size < target_bytes:
        record = {
            "trace_id": f"{rng.getrandbits(128):032x}",
            "timestamp": (
                now - timedelta(milliseconds=rng.randint(0, 60_000))
            ).isoformat(),
            "service": rng.choice(SERVICES),
            "endpoint": rng.choice(ENDPOINTS),
            "method": rng.choice(METHODS),
            "status": rng.choice(STATUSES),
            "latency_ms": round(rng.lognormvariate(3.2, 0.6), 2),
            "bytes_out": rng.randint(200, 20_000),
            "region": rng.choice(REGIONS),
        }
        records.append(record)
        size += len(json.dumps(record, separators=(",", ":"))) + 1
    frame["records"] = records
    return json.dumps(frame, separators=(",", ":")).encode()


def make_pool(target_bytes: int, jitter_bytes: int, rng: random.Random) -> list[bytes]:
    """サイズにばらつきを持たせた本文の一覧を作る"""
    return [
        make_payload(target_bytes + rng.randint(-jitter_bytes, jitter_bytes), rng)
        for _ in range(PAYLOAD_POOL_SIZE)
    ]


class TrafficRunner:
    """通常の通信と負荷の通信を、フロントの希望状態に合わせて開始・停止する"""

    def __init__(
        self,
        receiver_url: str,
        transport: httpx.AsyncBaseTransport | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=receiver_url,
            timeout=10.0,
            transport=transport,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            # 負荷の接続数の上限に合わせる（通常の通信用に 1 本足す）
            limits=httpx.Limits(max_connections=CONNECTIONS_RANGE.maximum + 1),
        )
        self._rng = rng or random.Random()
        self._baseline_pool = make_pool(BASELINE_CHUNK_BYTES, 16 * 1024, self._rng)
        self._load_pool = make_pool(REQUEST_BYTES, REQUEST_JITTER_BYTES, self._rng)
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
                rate=mbps_to_bytes_per_sec(BASELINE_MBPS),
                burst=max(len(body) for body in self._baseline_pool),
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
            rate=mbps_to_bytes_per_sec(MAX_INJECTED_MBPS),
            burst=max(len(body) for body in self._load_pool),
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
            body = self._rng.choice(self._load_pool)
            await byte_bucket.acquire(len(body))
            await asyncio.sleep(self._rng.uniform(0, SEND_JITTER_SEC))
            await self._post(LOAD_PATH, body)

    async def _baseline_loop(self, bucket: AsyncTokenBucket) -> None:
        while True:
            body = self._rng.choice(self._baseline_pool)
            await bucket.acquire(len(body))
            await self._post(BASELINE_PATH, body)

    async def _post(self, path: str, body: bytes) -> None:
        try:
            response = await self._client.post(
                path, content=body, headers={"Content-Type": "application/json"}
            )
        except httpx.HTTPError as error:
            # 一時的な失敗では止めず、次の送信で続ける
            logger.warning("フロントへの送信に失敗しました: %s", type(error).__name__)
            await asyncio.sleep(RETRY_WAIT_SEC)
            return
        if response.status_code >= 400:
            # フロントが受け付けられなかった（混雑など）場合は少し待ってから続ける
            logger.warning(
                "フロントが送信を受け付けませんでした: HTTP %d", response.status_code
            )
            await asyncio.sleep(RETRY_WAIT_SEC)
