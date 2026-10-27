from server.throughput import ThroughputMeter


class FakeClock:
    """テスト用の時計。秒を手で進める"""

    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_直近1秒のバイト数からMbpsを出す() -> None:
    clock = FakeClock()
    meter = ThroughputMeter(clock=clock)
    # 125,000 バイト = 1,000,000 ビット。1 秒の窓なら 1Mbps
    meter.record("load", nbytes=125_000, requests=1)

    data = meter.snapshot()

    assert data["load"] == {"mbps": 1.0, "rps": 1.0}
    assert data["total"] == {"mbps": 1.0, "rps": 1.0}
    assert data["baseline"] == {"mbps": 0.0, "rps": 0.0}


def test_通常の通信と負荷の通信を分けて集計する() -> None:
    meter = ThroughputMeter(clock=FakeClock())
    meter.record("baseline", nbytes=250_000)
    meter.record("load", nbytes=125_000, requests=5)

    data = meter.snapshot()

    assert data["baseline"]["mbps"] == 2.0
    assert data["load"] == {"mbps": 1.0, "rps": 5.0}
    assert data["total"]["mbps"] == 3.0


def test_窓より古い受信は集計から外れる() -> None:
    clock = FakeClock()
    meter = ThroughputMeter(clock=clock)
    meter.record("load", nbytes=125_000)
    clock.now += 1.5

    assert meter.snapshot()["total"]["mbps"] == 0.0


def test_知らない種別はotherにまとめ_合計には含める() -> None:
    meter = ThroughputMeter(clock=FakeClock())
    meter.record("", nbytes=125_000)

    data = meter.snapshot()

    assert data["baseline"]["mbps"] == 0.0
    assert data["load"]["mbps"] == 0.0
    assert data["total"]["mbps"] == 1.0
