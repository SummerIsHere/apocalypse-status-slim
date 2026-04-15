from __future__ import annotations

from pathlib import Path

import pandas as pd

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.transform.models import build_annual_avg_msl


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        paths=AppPaths(
            data_root=tmp_path / "downloaded_data",
            raw_dir=tmp_path / "downloaded_data" / "raw",
            tidy_dir=tmp_path / "downloaded_data" / "tidy",
            artifacts_dir=tmp_path / "artifacts",
        ),
        sources={},
        quality={},
        report={},
    )


def test_build_annual_avg_msl_station_normalized_avoids_station_offset_jump(tmp_path: Path) -> None:
    tidal_master = pd.DataFrame(
        [
            {"Year": 1974, "Month": 1, "Datum": "STND", "MSL": 2.0, "nwlon_station_id": "A"},
            {"Year": 1975, "Month": 1, "Datum": "STND", "MSL": 2.1, "nwlon_station_id": "A"},
            {"Year": 1976, "Month": 1, "Datum": "STND", "MSL": 2.2, "nwlon_station_id": "A"},
            {"Year": 2000, "Month": 1, "Datum": "STND", "MSL": 2.3, "nwlon_station_id": "A"},
            # Station B joins later with a much higher absolute level offset.
            {"Year": 1975, "Month": 1, "Datum": "STND", "MSL": 10.1, "nwlon_station_id": "B"},
            {"Year": 1976, "Month": 1, "Datum": "STND", "MSL": 10.2, "nwlon_station_id": "B"},
            {"Year": 2000, "Month": 1, "Datum": "STND", "MSL": 10.3, "nwlon_station_id": "B"},
        ]
    )

    annual, _ = build_annual_avg_msl(_config(tmp_path), tidal_master)
    annual = annual.set_index("Year")

    assert abs(float(annual.loc[1974, "Change in Mean Sea Level since 2000 (in)"]) - (-11.81103)) < 1e-4
    assert abs(float(annual.loc[1975, "Change in Mean Sea Level since 2000 (in)"]) - (-7.87402)) < 1e-4
    assert abs(float(annual.loc[1976, "Change in Mean Sea Level since 2000 (in)"]) - (-3.93701)) < 1e-4


def test_build_annual_avg_msl_falls_back_without_station_id(tmp_path: Path) -> None:
    tidal_master = pd.DataFrame(
        [
            {"Year": 1999, "Month": 1, "Datum": "STND", "MSL": 1.0},
            {"Year": 2000, "Month": 1, "Datum": "STND", "MSL": 1.5},
            {"Year": 2001, "Month": 1, "Datum": "STND", "MSL": 2.0},
        ]
    )

    annual, _ = build_annual_avg_msl(_config(tmp_path), tidal_master)
    annual = annual.set_index("Year")
    assert float(annual.loc[2000, "Change in Mean Sea Level since 2000 (in)"]) == 0.0
    assert abs(float(annual.loc[2001, "Change in Mean Sea Level since 2000 (in)"]) - 19.68505) < 1e-4
