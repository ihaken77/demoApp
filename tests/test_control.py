import time

import httpx
from fastapi.testclient import TestClient

from loadgen.control import create_control_app
from loadgen.traffic import TrafficRunner
from server.backend_client import BackendClient
from server.main import create_app

received_paths: list[str] = []


def fake_receiver(request: httpx.Request) -> httpx.Response:
    received_paths.append(request.url.path)
    return httpx.Response(204)


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

    assert received_paths and set(received_paths) == {"/receive"}


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
