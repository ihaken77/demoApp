"""バックエンドの制御エンドポイントへ、負荷の希望状態を送る（AWS フロント側）"""

import logging
from dataclasses import asdict

import httpx

from server.state import LoadState

logger = logging.getLogger(__name__)


class BackendClient:
    """制御エンドポイントへの送信。送るたびに希望状態の全体を渡す（同じ指示を繰り返しても結果が変わらない）"""

    def __init__(
        self,
        control_url: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=control_url, timeout=5.0, transport=transport
        )

    async def apply(self, state: LoadState) -> None:
        response = await self._client.post("/control/apply", json=asdict(state))
        response.raise_for_status()
        # 接続先の IP はログに出さない
        logger.info("バックエンドに負荷の状態を送信しました")

    async def aclose(self) -> None:
        await self._client.aclose()
