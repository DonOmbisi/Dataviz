"""
Adaptive data understanding: infer column roles and optional geocoding hints.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

GEO_DV_LAT = "dv_latitude"
GEO_DV_LON = "dv_longitude"

LOCATION_HINTS = (
    "city",
    "town",
    "municipality",
    "village",
    "metro",
    "state",
    "province",
    "region",
    "country",
    "nation",
    "location",
    "address",
    "street",
    "place",
    "borough",
    "county",
    "zip",
    "postal",
)


def _norm_col(name: str) -> str:
    return str(name).lower().replace(" ", "_").replace("-", "_")


def _is_lat_column_name(nl: str) -> bool:
    if nl == _norm_col(GEO_DV_LAT):
        return True
    if "latitude" in nl:
        return True
    if nl == "lat" or nl.endswith("_lat") or nl.startswith("lat_"):
        return True
    return False


def _is_lon_column_name(nl: str) -> bool:
    if nl == _norm_col(GEO_DV_LON):
        return True
    if "longitude" in nl:
        return True
    if nl in ("lon", "lng") or nl.endswith("_lon") or nl.startswith("lon_"):
        return True
    if nl.endswith("_lng") or nl.startswith("lng_"):
        return True
    return False


def _is_location_column_name(nl: str) -> bool:
    for h in LOCATION_HINTS:
        if nl == h or nl.startswith(f"{h}_") or nl.endswith(f"_{h}") or f"_{h}_" in nl:
            return True
    return False


def detect_coordinate_columns(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    lat_cols: List[str] = []
    lon_cols: List[str] = []
    for c in df.columns:
        nl = _norm_col(c)
        if _is_lat_column_name(nl) and pd.api.types.is_numeric_dtype(df[c]):
            lat_cols.append(c)
        if _is_lon_column_name(nl) and pd.api.types.is_numeric_dtype(df[c]):
            lon_cols.append(c)
    return lat_cols, lon_cols


def detect_location_text_columns(df: pd.DataFrame) -> List[str]:
    lat_set = set(detect_coordinate_columns(df)[0])
    lon_set = set(detect_coordinate_columns(df)[1])
    out: List[str] = []
    for c in df.columns:
        if c in lat_set or c in lon_set:
            continue
        nl = _norm_col(c)
        if not _is_location_column_name(nl):
            continue
        if not pd.api.types.is_object_dtype(df[c]) and not pd.api.types.is_string_dtype(
            df[c]
        ):
            if not (nl == "zip" or nl.startswith("zip_") or nl.endswith("_zip") or "postal" in nl):
                continue
        out.append(c)
    return out


def _guess_role(df: pd.DataFrame, col: str) -> Tuple[str, str]:
    s = df[col]
    name = _norm_col(col)
    nunique = int(s.nunique(dropna=True))
    n = len(df)

    if _is_lat_column_name(name) and pd.api.types.is_numeric_dtype(s):
        return "latitude", "Numeric column name suggests latitude."
    if _is_lon_column_name(name) and pd.api.types.is_numeric_dtype(s):
        return "longitude", "Numeric column name suggests longitude."

    if pd.api.types.is_datetime64_any_dtype(s):
        return "datetime", "Parsed or native datetime values."

    if pd.api.types.is_bool_dtype(s):
        return "boolean", "True/false flags."

    if pd.api.types.is_numeric_dtype(s):
        if nunique <= 12 and n < 5000:
            return "numeric_code", "Few distinct numeric values — could be codes or categories."
        return "numeric_measure", "Continuous or count-like numeric measure."

    # object / string
    if any(h in name for h in ("id", "uuid", "sku", "key")) and nunique >= max(3, n * 0.85):
        return "identifier", "High cardinality text — likely an ID, not a dimension."

    if _is_location_column_name(name):
        return "location_text", "Name suggests addresses or place names — good for maps after geocoding."

    str_s = s.dropna().astype(str)
    if len(str_s) == 0:
        return "mostly_empty", "All or nearly all values are missing."

    avg_len = float(str_s.map(len).mean())
    if avg_len > 48 and nunique > min(50, n * 0.2):
        return "free_text", "Long varied strings — notes, descriptions, or compound text."

    if nunique <= min(24, max(2, n // 10)):
        return "category", f"~{nunique} distinct labels — chart as dimension or color."

    return "text_or_category", f"~{nunique} distinct values — may be categories or short labels."


def build_data_digest(df: pd.DataFrame) -> Dict[str, Any]:
    lat_cols, lon_cols = detect_coordinate_columns(df)
    loc_cols = detect_location_text_columns(df)
    roles = []
    for col in df.columns:
        role, hint = _guess_role(df, col)
        roles.append({"column": col, "role": role, "hint": hint})

    bullets: List[str] = []
    num_measures = sum(1 for r in roles if r["role"] == "numeric_measure")
    cats = sum(1 for r in roles if r["role"] == "category")
    dts = sum(1 for r in roles if r["role"] == "datetime")
    if num_measures:
        bullets.append(
            f"Found **{num_measures}** numeric measure column(s) — use bar/line/scatter and correlation in **Visualizations** and **Advanced**."
        )
    if cats:
        bullets.append(
            f"Found **{cats}** low-cardinality text column(s) — good for breakdowns, color, or treemap segments."
        )
    if dts:
        bullets.append(
            f"Found **{dts}** date/time column(s) — ideal for trends and time-based charts or forecasting."
        )
    if lat_cols and lon_cols:
        bullets.append(
            "You already have **latitude/longitude** — open **Geographic Maps** to plot points."
        )
    elif loc_cols:
        bullets.append(
            f"Found possible place column(s): **{', '.join(loc_cols[:5])}** — use **Geocode to coordinates** on the map tab."
        )
    else:
        bullets.append(
            "No obvious place or coordinate columns — maps need city/address columns or lat/lon."
        )

    return {
        "column_roles": roles,
        "bullets": bullets,
        "has_coords": bool(lat_cols and lon_cols),
        "lat_columns": lat_cols,
        "lon_columns": lon_cols,
        "location_columns": loc_cols,
        "shape": (int(len(df)), int(len(df.columns))),
    }


def geocode_place(query: str, user_agent: str = "dataviz-pro/1.0 (local analytics)") -> Tuple[Optional[float], Optional[float]]:
    """
    Resolve a single free-text place to (lat, lon) using OpenStreetMap Nominatim.
    Respect OSM usage policy: call infrequently (caller should throttle batch jobs).
    """
    q = (query or "").strip()
    if not q:
        return None, None
    try:
        from geopy.exc import GeocoderServiceError, GeocoderTimedOut, GeocoderUnavailable
        from geopy.geocoders import Nominatim
    except ImportError:
        return None, None

    geolocator = Nominatim(user_agent=user_agent, timeout=12)
    try:
        loc = geolocator.geocode(q, exactly_one=True)
        if loc is None:
            return None, None
        return float(loc.latitude), float(loc.longitude)
    except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError, OSError):
        return None, None


def apply_geocode_columns(
    df: pd.DataFrame,
    place_col: str,
    mapping: Dict[str, Tuple[Optional[float], Optional[float]]],
    lat_name: str = GEO_DV_LAT,
    lon_name: str = GEO_DV_LON,
) -> pd.DataFrame:
    """Add latitude/longitude columns from a mapping of string place -> (lat, lon)."""
    out = df.copy()

    def _lookup(v: Any) -> Tuple[Optional[float], Optional[float]]:
        if pd.isna(v):
            return None, None
        key = str(v).strip()
        return mapping.get(key, (None, None))

    pairs = out[place_col].map(_lookup)
    out[lat_name] = pairs.map(lambda t: t[0])
    out[lon_name] = pairs.map(lambda t: t[1])
    return out
