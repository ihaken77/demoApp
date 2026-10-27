"""負荷パラメータの上限・下限と検証（AWS フロント側）"""

from dataclasses import dataclass

# 注入負荷の帯域上限（コード定数。設定ファイルや引数では上げられない）
MAX_INJECTED_MBPS = 60


@dataclass(frozen=True)
class ParamRange:
    """画面のスライダーに使う範囲。最小・最大・刻みを持つ"""

    minimum: int
    maximum: int
    step: int
    default: int

    def validate(self, value: int) -> int:
        """範囲と刻みを確認し、問題なければそのまま返す"""
        if not self.minimum <= value <= self.maximum:
            raise ValueError(f"{self.minimum}〜{self.maximum} の範囲で指定してください")
        if (value - self.minimum) % self.step != 0:
            raise ValueError(f"{self.step} 単位で指定してください")
        return value


# 最大リクエスト/秒: 10〜500 を 10 刻み（50 段階）。最大リクエスト/秒の 15KB で約 61Mbps となり、注入の上限 60Mbps に届く
RPS_RANGE = ParamRange(minimum=10, maximum=500, step=10, default=500)

# 最大接続数: 1〜100 を 1 刻み。100 接続あれば帯域上限まで届き、グラフの変化も読み取りやすい
CONNECTIONS_RANGE = ParamRange(minimum=1, maximum=100, step=1, default=100)
