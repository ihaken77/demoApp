"""受信した通信への応答本文の生成（フロント → バックエンドの向きの通信）

バックエンドから受けた本文の 1/5 程度の大きさの応答を返し、
バックエンド → フロント と フロント → バックエンド の通信量を約 5:1 にする。
応答はバックエンドが張った接続の上で返すため、新しいポートや経路の設定はいらない。
"""

import json
import random
import uuid

# 受信量 : 応答量 の比。受信の 1/5 の大きさの応答を返す
REPLY_RATIO = 5
# 応答に並べる受付結果の候補を何件用意しておくか（毎回組み立てると CPU を使うため、先に作っておく）
ACK_POOL_SIZE = 512
ACK_STATUSES = ("stored", "stored", "stored", "deduplicated")


def make_ack_pool(rng: random.Random, size: int = ACK_POOL_SIZE) -> list[bytes]:
    """受付結果 1 件分の JSON を、直列化済みで用意する"""
    return [
        json.dumps(
            {
                "trace_id": f"{rng.getrandbits(128):032x}",
                "status": rng.choice(ACK_STATUSES),
                "partition": rng.randint(0, 11),
                "offset": rng.randint(1_000_000, 99_999_999),
            },
            separators=(",", ":"),
        ).encode()
        for _ in range(size)
    ]


class ReplyBuilder:
    """受信したバイト数から、その 1/REPLY_RATIO 前後の大きさの応答本文を作る"""

    def __init__(self, rng: random.Random | None = None) -> None:
        self._rng = rng or random.Random()
        self._pool = make_ack_pool(self._rng)

    def build(self, received_bytes: int) -> bytes:
        target = received_bytes // REPLY_RATIO
        head = json.dumps(
            {
                "status": "accepted",
                "request_id": str(uuid.UUID(int=self._rng.getrandbits(128))),
                "received_bytes": received_bytes,
            },
            separators=(",", ":"),
        ).encode()[:-1]  # 末尾の "}" を外して results を足す
        # 候補を途中から順に並べ、目標の大きさに届いた時点で止める（1 件分の超過は許す）
        items: list[bytes] = []
        size = len(head) + len(b',"results":[]}')
        start = self._rng.randrange(len(self._pool))
        while size < target:
            item = self._pool[(start + len(items)) % len(self._pool)]
            items.append(item)
            size += len(item) + 1
        return head + b',"results":[' + b",".join(items) + b"]}"
