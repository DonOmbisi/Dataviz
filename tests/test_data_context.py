import pandas as pd

from data_context import (
    build_data_digest,
    detect_coordinate_columns,
    detect_location_text_columns,
)


def test_latest_column_not_detected_as_latitude():
    df = pd.DataFrame({"Latest": [1, 2, 3], "latitude": [10.0, 11.0, 12.0], "lon_x": [-70.0, -71.0, -72.0]})
    lat, lon = detect_coordinate_columns(df)
    assert "Latest" not in lat
    assert "latitude" in lat
    assert "lon_x" in lon


def test_city_column_detected_for_geocoding():
    df = pd.DataFrame({"City": ["Phoenix", "Chicago"], "Sales": [1, 2]})
    loc = detect_location_text_columns(df)
    assert "City" in loc


def test_digest_bullets_for_city_data():
    df = pd.DataFrame({"City": ["Phoenix", "Chicago"], "Sales": [1, 2]})
    d = build_data_digest(df)
    assert d["shape"] == (2, 2)
    assert d["has_coords"] is False
    assert "City" in d["location_columns"]
    texts = " ".join(d["bullets"])
    assert "Geocode" in texts or "place" in texts
