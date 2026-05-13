import pandas as pd

from ab_testing import ABTestingFramework


def test_create_experiment_and_t_test():
    f = ABTestingFramework()
    a = pd.Series([1.0, 2.0, 3.0, 4.0])
    b = pd.Series([1.5, 2.5, 3.5, 4.5])
    created = f.create_experiment("exp_a", a, b, "revenue", hypothesis="B > A")
    assert created["success"] is True

    result = f.t_test_analysis("exp_a", alpha=0.05)
    assert result["success"] is True
    assert "p_value" in result
    assert result["variant_a_mean"] == 2.5
    assert result["variant_b_mean"] == 3.0


def test_t_test_missing_experiment():
    f = ABTestingFramework()
    out = f.t_test_analysis("missing")
    assert out["success"] is False
    assert "not found" in out["error"].lower()
