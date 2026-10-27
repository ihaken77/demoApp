import asyncio
import time

from loadgen.pacing import AsyncTokenBucket


def test_burst_の範囲内は待たずに通る() -> None:
    async def run() -> float:
        bucket = AsyncTokenBucket(rate=1000, burst=100)
        start = time.monotonic()
        await bucket.acquire(100)
        return time.monotonic() - start

    assert asyncio.run(run()) < 0.05


def test_rate_を超える消費は待たされる() -> None:
    async def run() -> float:
        bucket = AsyncTokenBucket(rate=1000, burst=200)
        await bucket.acquire(200)
        start = time.monotonic()
        # 1000 単位/秒なので、溜まった分がなくなった後の 200 単位は約 0.2 秒かかる
        await bucket.acquire(200)
        return time.monotonic() - start

    assert asyncio.run(run()) >= 0.15


def test_burst_を超える要求は拒否する() -> None:
    async def run() -> None:
        bucket = AsyncTokenBucket(rate=1000, burst=100)
        await bucket.acquire(101)

    try:
        asyncio.run(run())
    except ValueError:
        return
    raise AssertionError("ValueError が出るべき")
