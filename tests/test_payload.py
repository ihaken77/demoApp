"""送信する本文（テレメトリのバッチ風 JSON）の検証"""

import json
import random

from loadgen.traffic import REQUEST_BYTES, make_payload, make_pool


def test_本文は目標サイズ付近の有効なJSONになる() -> None:
    body = make_payload(REQUEST_BYTES, random.Random(1))

    data = json.loads(body)
    assert len(body) >= REQUEST_BYTES
    # 1 件分（数百バイト）を超えて大きくならないこと
    assert len(body) < REQUEST_BYTES + 1024
    assert data["records"]


def test_本文はゼロ埋めではなく内容を持つ() -> None:
    body = make_payload(REQUEST_BYTES, random.Random(2))

    assert body.count(b"\x00") == 0
    assert len({record["trace_id"] for record in json.loads(body)["records"]}) > 1


def test_サイズにばらつきがあり目標の前後に収まる() -> None:
    pool = make_pool(REQUEST_BYTES, 1024, random.Random(3))
    sizes = {len(body) for body in pool}

    assert len(sizes) > 1
    assert all(
        REQUEST_BYTES - 1024 <= size < REQUEST_BYTES + 1024 + 1024 for size in sizes
    )
