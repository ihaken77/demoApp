import json

import httpx
from fastapi.testclient import TestClient

from server.backend_client import BackendClient
from server.main import create_app

# バックエンドの代わりに、受け取った指示を記録する
sent: list[dict] = []
backend_ok = True


def fake_backend(request: httpx.Request) -> httpx.Response:
    if not backend_ok:
        return httpx.Response(500)
    sent.append(json.loads(request.content))
    return httpx.Response(200, json={"ok": True})


def make_client() -> TestClient:
    backend = BackendClient(
        "http://backend.test", transport=httpx.MockTransport(fake_backend)
    )
    return TestClient(create_app(backend, "azure"))


def setup_function() -> None:
    global backend_ok
    sent.clear()
    backend_ok = True


def test_初期状態と上下限を返す() -> None:
    with make_client() as client:
        data = client.get("/api/state").json()

    assert data["backend"] == "azure"
    assert data["state"] == {
        "baseline_on": False,
        "load_on": False,
        "rps": 100,
        "connections": 10,
    }
    assert data["limits"]["rps"] == {
        "minimum": 10,
        "maximum": 500,
        "step": 10,
        "default": 100,
    }
    assert data["limits"]["connections"]["maximum"] == 100
    assert data["limits"]["max_injected_mbps"] == 50


def test_通常の通信と負荷のON_OFFを独立して切り替える() -> None:
    with make_client() as client:
        client.post("/api/control", json={"baseline_on": True})
        response = client.post("/api/control", json={"load_on": True})

    assert response.json()["state"]["baseline_on"] is True
    assert response.json()["state"]["load_on"] is True
    # 送る指示は毎回、希望状態の全体
    assert sent[-1] == {
        "baseline_on": True,
        "load_on": True,
        "rps": 100,
        "connections": 10,
    }


def test_最大RPSと最大接続数を変更できる() -> None:
    with make_client() as client:
        response = client.post("/api/control", json={"rps": 250, "connections": 40})

    assert response.json()["state"]["rps"] == 250
    assert response.json()["state"]["connections"] == 40


def test_範囲外の値は422で拒否し_バックエンドには送らない() -> None:
    with make_client() as client:
        response = client.post("/api/control", json={"rps": 9999})
        state = client.get("/api/state").json()["state"]

    assert response.status_code == 422
    assert sent == []
    assert state["rps"] == 100


def test_刻みに合わない値は422で拒否する() -> None:
    with make_client() as client:
        response = client.post("/api/control", json={"rps": 15})

    assert response.status_code == 422


def test_バックエンドへの送信に失敗したら状態を変えない() -> None:
    global backend_ok
    backend_ok = False
    with make_client() as client:
        response = client.post("/api/control", json={"load_on": True})
        state = client.get("/api/state").json()["state"]

    assert response.status_code == 502
    assert state["load_on"] is False


def test_受信した通信量を種別ごとに実測として返す() -> None:
    with make_client() as client:
        for _ in range(2):
            client.post(
                "/receive",
                content=bytes(10 * 1024),
                headers={"X-Traffic-Kind": "load"},
            )
        data = client.get("/api/traffic").json()

    assert data["load"]["rps"] == 2.0
    assert data["load"]["mbps"] > 0
    assert data["baseline"]["mbps"] == 0.0
    assert data["total"]["mbps"] == data["load"]["mbps"]
