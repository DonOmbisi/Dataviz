import pandas as pd

from forecasting_engine import ForecastingEngine


def test_prepare_timeseries_sorts_and_dedupes():
    fe = ForecastingEngine()
    df = pd.DataFrame(
        {
            "d": ["2024-01-03", "2024-01-01", "2024-01-02", "2024-01-01"],
            "v": [30, 10, 20, 99],
        }
    )
    ts = fe.prepare_timeseries(df, "d", "v")
    assert ts is not None
    assert len(ts) == 3
    assert ts.index.is_monotonic_increasing


def test_check_stationarity_returns_dict():
    fe = ForecastingEngine()
    s = pd.Series([1.0, 1.1, 0.9, 1.05, 0.95] * 20)
    out = fe.check_stationarity(s)
    assert isinstance(out, dict)
    assert "stationary" in out or "error" in out
