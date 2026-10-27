"""フロント（AWS）の制御画面と API"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel

from server.backend_client import BackendClient
from server.config import load_backend_url
from server.limits import CONNECTIONS_RANGE, MAX_INJECTED_MBPS, RPS_RANGE
from server.state import LoadState

WEB_DIR = Path(__file__).resolve().parents[1] / "web"

logger = logging.getLogger(__name__)


class ControlUpdate(BaseModel):
    """画面から変更された項目だけを含む。含まれない項目は現在の値のまま"""

    baseline_on: bool | None = None
    load_on: bool | None = None
    rps: int | None = None
    connections: int | None = None


def create_app(backend: BackendClient, backend_name: str) -> FastAPI:
    state = LoadState()
    # 同時に来た操作が互いに上書きしないよう、1件ずつ処理する（単一ワーカー前提）
    lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        await backend.aclose()

    app = FastAPI(lifespan=lifespan)

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(WEB_DIR / "index.html")

    @app.get("/api/state")
    async def get_state() -> dict:
        return {
            "backend": backend_name,
            "state": asdict(state),
            "limits": {
                "rps": asdict(RPS_RANGE),
                "connections": asdict(CONNECTIONS_RANGE),
                "max_injected_mbps": MAX_INJECTED_MBPS,
            },
        }

    @app.post("/api/control")
    async def update_control(body: ControlUpdate) -> dict:
        nonlocal state
        changes = {
            key: value for key, value in body.model_dump().items() if value is not None
        }
        async with lock:
            candidate = replace(state, **changes)
            try:
                candidate.rps = RPS_RANGE.validate(candidate.rps)
                candidate.connections = CONNECTIONS_RANGE.validate(
                    candidate.connections
                )
            except ValueError as error:
                raise HTTPException(status_code=422, detail=str(error)) from error

            try:
                await backend.apply(candidate)
            except httpx.HTTPError as error:
                # 送れなかった場合は画面の状態を変えない（実際の負荷と表示をずらさないため）
                logger.error(
                    "バックエンドへの送信に失敗しました: %s", type(error).__name__
                )
                raise HTTPException(
                    status_code=502,
                    detail="バックエンドへの送信に失敗しました。状態は変更していません",
                ) from error

            state = candidate
        return {"state": asdict(state)}

    @app.post("/receive", status_code=204)
    async def receive(request: Request) -> Response:
        # バックエンドからの負荷の通信を受けて捨てる（帯域を消費させるのが目的）
        async for _ in request.stream():
            pass
        return Response(status_code=204)

    return app


def create_default_app() -> FastAPI:
    """環境変数から接続先を決めて起動する。例: DEMO_BACKEND=azure uvicorn server.main:create_default_app --factory"""
    backend_name = os.environ["DEMO_BACKEND"]
    config_path = Path(os.environ.get("DEMO_CONFIG", "config/targets.yaml"))
    url = load_backend_url(backend_name, config_path)
    return create_app(BackendClient(url), backend_name)
