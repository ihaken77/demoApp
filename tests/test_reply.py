import json
import random

from fastapi.testclient import TestClient

from server.backend_client import BackendClient
from server.main import create_app
from server.reply import REPLY_RATIO, ReplyBuilder
from server.throughput import BASELINE_PATH, LOAD_PATH


def test_応答は受信量の約5分の1でJSONとして読める() -> None:
    builder = ReplyBuilder(rng=random.Random(0))
    for received in (15 * 1024, 256 * 1024):
        body = builder.build(received)
        data = json.loads(body)
        target = received // REPLY_RATIO
        # 1 件分の超過は許す
        assert target <= len(body) <= target + 200
        assert data["status"] == "accepted"
        assert data["received_bytes"] == received
        assert data["results"]


def test_受信が小さくても応答はJSONとして読める() -> None:
    body = ReplyBuilder(rng=random.Random(0)).build(0)
    assert json.loads(body)["results"] == []


def test_受信サーバは応答を返し_応答量を実測に出す() -> None:
    app = create_app(BackendClient("http://backend.test"), "azure")
    with TestClient(app) as client:
        for path, size in ((LOAD_PATH, 15 * 1024), (BASELINE_PATH, 256 * 1024)):
            response = client.post(path, content=b"x" * size)
            assert response.status_code == 202
            assert len(response.content) >= size // REPLY_RATIO

        data = client.get("/api/traffic").json()

    received_mbps = data["total"]["mbps"]
    assert data["reply"]["rps"] == 2.0
    # 受信 : 応答 ≈ 5 : 1
    assert 4.5 <= received_mbps / data["reply"]["mbps"] <= 5.5
