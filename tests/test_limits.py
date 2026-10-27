import pytest

from server.limits import CONNECTIONS_RANGE, RPS_RANGE


def test_rps_の範囲内で刻みに合う値は通る() -> None:
    assert RPS_RANGE.validate(10) == 10
    assert RPS_RANGE.validate(500) == 500


@pytest.mark.parametrize("value", [0, 9, 505, 1000])
def test_rps_の範囲外は拒否する(value: int) -> None:
    with pytest.raises(ValueError):
        RPS_RANGE.validate(value)


def test_rps_の刻みに合わない値は拒否する() -> None:
    with pytest.raises(ValueError):
        RPS_RANGE.validate(15)


def test_接続数は1刻みで1から100まで通る() -> None:
    assert CONNECTIONS_RANGE.validate(1) == 1
    assert CONNECTIONS_RANGE.validate(37) == 37
    assert CONNECTIONS_RANGE.validate(100) == 100


@pytest.mark.parametrize("value", [0, 101])
def test_接続数の範囲外は拒否する(value: int) -> None:
    with pytest.raises(ValueError):
        CONNECTIONS_RANGE.validate(value)
