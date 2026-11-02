import time

import httpx
from fastapi.testclient import TestClient

from loadgen.control import create_control_app
from loadgen.traffic import TrafficRunner
from server.backend_client import BackendClient
from server.main import create_app
from server.throughput import BASELINE_PATH, LOAD_PATH

received_paths: list[str] = []


def fake_receiver(request: httpx.Request) -> httpx.Response:
    received_paths.append(request.url.path)
    return httpx.Response(202)


def make_client() -> TestClient:
    runner = TrafficRunner(
        "http://receiver.test", transport=httpx.MockTransport(fake_receiver)
    )
    return TestClient(create_control_app(runner))


def setup_function() -> None:
    received_paths.clear()


def apply(client: TestClient, **body: object) -> httpx.Response:
    payload = {"baseline_on": False, "load_on": False, "rps": 100, "connections": 10}
    payload.update(body)
    return client.post("/control/apply", json=payload)


def test_範囲外の値は422で拒否する() -> None:
    with make_client() as client:
        response = apply(client, rps=9999)

    assert response.status_code == 422


def test_通常の通信のON_OFFを切り替える() -> None:
    with make_client() as client:
        assert apply(client, baseline_on=True).json()["baseline_on"] is True
        time.sleep(0.1)
        assert apply(client, baseline_on=False).json()["baseline_on"] is False

    assert received_paths and set(received_paths) == {BASELINE_PATH}


def test_負荷の通信のON_OFFを切り替える() -> None:
    with make_client() as client:
        assert (
            apply(client, load_on=True, rps=10, connections=1).json()["load_on"] is True
        )
        time.sleep(0.1)
        assert apply(client, load_on=False).json()["load_on"] is False

    assert received_paths


def test_フロントの受け口まで負荷が届く() -> None:
    # バックエンドの送信先をフロントのアプリ（受け口）にしてつなぐ
    front = create_app(
        BackendClient(
            "http://backend.test",
            transport=httpx.MockTransport(lambda r: httpx.Response(200)),
        ),
        "azure",
    )
    runner = TrafficRunner(
        "http://front.test", transport=httpx.ASGITransport(app=front)
    )
    with TestClient(create_control_app(runner)) as client:
        assert apply(client, load_on=True, rps=10, connections=1).status_code == 200
        time.sleep(0.3)
        apply(client, load_on=False)


def test_通常の通信と負荷の通信は別の経路に送る() -> None:
    paths: set[str] = set()

    def record_path(request: httpx.Request) -> httpx.Response:
        paths.add(request.url.path)
        # 本文は JSON として届くこと（既定の httpx の User-Agent ではないこと）も確かめる
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["User-Agent"].startswith("ingest-client/")
        return httpx.Response(202)

    runner = TrafficRunner(
        "http://receiver.test", transport=httpx.MockTransport(record_path)
    )
    with TestClient(create_control_app(runner)) as client:
        apply(client, baseline_on=True)
        apply(client, load_on=True, rps=10, connections=1)
        time.sleep(0.2)

    assert paths == {BASELINE_PATH, LOAD_PATH}
