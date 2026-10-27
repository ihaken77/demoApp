"""バックエンド（Azure / OCI）の制御エンドポイント。フロントからの希望状態を受けて負荷を切り替える"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from loadgen.traffic import TrafficRunner
from server.limits import CONNECTIONS_RANGE, RPS_RANGE

logger = logging.getLogger(__name__)


class ApplyBody(BaseModel):
    """フロントが持つ希望状態の全体（毎回すべての項目を送る）"""

    baseline_on: bool
    load_on: bool
    rps: int
    connections: int


def create_control_app(runner: TrafficRunner) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        # 停止時は通信を止めてから終わる
        await runner.aclose()

    app = FastAPI(lifespan=lifespan)

    @app.post("/control/apply")
    async def apply(body: ApplyBody) -> dict:
        try:
            rps = RPS_RANGE.validate(body.rps)
            connections = CONNECTIONS_RANGE.validate(body.connections)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

        # 状態の切り替えは同期処理なので、同時の指示が混ざることはない
        runner.set_baseline(body.baseline_on)
        runner.set_load(body.load_on, rps, connections)
        return {
            "baseline_on": runner.baseline_running,
            "load_on": runner.load_running,
        }

    return app


def create_default_app() -> FastAPI:
    """環境変数 RECEIVER_URL（フロントの受信サーバ）に送る制御エンドポイントを作る"""
    receiver_url = os.environ["RECEIVER_URL"]
    return create_control_app(TrafficRunner(receiver_url))
