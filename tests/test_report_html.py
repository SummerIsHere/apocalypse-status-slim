from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.report.html import (
    _dataset_max_date,
    _load_snotel_region_groups,
    _source_refresh_panel_html,
    generate_dashboard_html,
)


def _config(tmp_path: Path) -> AppConfig:
    paths = AppPaths(
        data_root=tmp_path / "downloaded_data",
        raw_dir=tmp_path / "downloaded_data" / "raw",
        tidy_dir=tmp_path / "downloaded_data" / "tidy",
        artifacts_dir=tmp_path / "artifacts",
    )
    return AppConfig(paths=paths, sources={}, quality={}, report={"title": "Test Dashboard"})


def test_generate_dashboard_html(tmp_path: Path) -> None:
    config = _config(tmp_path)
    datasets = {
        "combined_co2": pd.DataFrame({"Year": [2000, 2001], "CO2 ppm": [370, 372], "Source": ["A", "A"]}),
        "global_temp": pd.DataFrame(
            {
                "Date": pd.to_datetime(["2000-01-01", "2001-01-01"]),
                "Median Global Temp C Deviation": [0.5, 0.6],
            }
        ),
        "grain_per_capita": pd.DataFrame(
            {
                "Date": pd.to_datetime(["2000-01-01", "2001-01-01"]),
                "Year": [2000, 2001],
                "kg Grain Per Capita": [420, 418],
            }
        ),
        "streamflow_master": pd.DataFrame(
            {
                "year_nu": [2026],
                "month_nu": [1],
                "monthly_cfs": [100.0],
                "data_status": ["provisional"],
                "site_no": ["123"],
                "River Name": ["Test River"],
                "River Type": ["Test"],
                "month_completeness": [0.7],
            }
        ),
        "annual_avg_msl": pd.DataFrame(
            {
                "Year": [2000, 2001],
                "Year Date": pd.to_datetime(["2000-01-01", "2001-01-01"]),
                "Change in Mean Sea Level since 2000 (in)": [0.0, 0.2],
            }
        ),
        "co2": pd.DataFrame(
            {
                "Date": pd.to_datetime(["2000-01-01"]),
                "avg CO2 ppm": [370.0],
            }
        ),
        "population_census_idb": pd.DataFrame(
            {
                "Year": [2000, 2001],
                "Value": [6_143_493_823, 6_222_626_524],
                "Source": ["US Census IDB", "US Census IDB"],
            }
        ),
    }

    output = tmp_path / "artifacts" / "dashboard.html"
    result = generate_dashboard_html(config, datasets, output)

    html = result.read_text(encoding="utf-8")
    assert "River Streamflows" in html
    assert "provisional" in html.lower()
    assert "Test Dashboard" in html
    assert "Data Source Refresh" in html
    assert "Data comes from" in html
    assert "population_census_idb" in html


def test_generate_dashboard_html_with_snotel_section(tmp_path: Path) -> None:
    config = _config(tmp_path)
    dates = pd.date_range("1980-10-01", "1983-09-01", freq="MS")
    swe_values = [10.0 + (idx % 12) * 0.4 for idx in range(len(dates))]
    datasets = {
        "snotel_master": pd.DataFrame(
            {
                "Date": dates,
                "Snow Water Equivalent (in) Start of Day Values": swe_values,
            }
        )
    }

    output = tmp_path / "artifacts" / "dashboard_snotel.html"
    result = generate_dashboard_html(config, datasets, output)

    html = result.read_text(encoding="utf-8")
    assert "Puget Sound Snowpack Levels" in html
    assert "Puget Sound Snowpack Rolling Means" in html
    assert "Puget Sound Snowpack Seasonal Profile" in html
    assert "Puget Sound Snowpack Annual Peak SWE" in html
    assert "PS Snowpack Overall" not in html
    assert "PS Snowpack Yr" not in html
    assert "PS Snowpack 5 Yr Avg" not in html
    assert "PS Snowpack 30 Yr Avg" not in html


def test_generate_dashboard_html_snotel_uses_legacy_region_grouping(tmp_path: Path) -> None:
    paths = AppPaths(
        data_root=tmp_path / "downloaded_data",
        raw_dir=tmp_path / "downloaded_data" / "raw",
        tidy_dir=tmp_path / "downloaded_data" / "tidy",
        artifacts_dir=tmp_path / "artifacts",
    )
    stations_csv = tmp_path / "downloaded_data" / "snotel" / "WA_SNOTEL_STATION_LIST.csv"
    stations_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "snotel_station_id": [1011, 2001],
            "legacy_regions": ["NORTH PUGET SOUND", "LOWER COLUMBIA"],
        }
    ).to_csv(stations_csv, index=False)

    config = AppConfig(
        paths=paths,
        sources={"snotel": {"stations_csv": str(stations_csv)}},
        quality={},
        report={"title": "Test Dashboard"},
    )

    dates = pd.date_range("1980-10-01", "1983-09-01", freq="MS")
    datasets = {
        "snotel_master": pd.DataFrame(
            {
                "Date": list(dates) + list(dates),
                "snotel_station_id": [1011] * len(dates) + [2001] * len(dates),
                "Snow Water Equivalent (in) Start of Day Values": [10.0 + (idx % 12) * 0.4 for idx in range(len(dates))]
                + [6.0 + (idx % 12) * 0.3 for idx in range(len(dates))],
            }
        )
    }

    output = tmp_path / "artifacts" / "dashboard_snotel_regions.html"
    result = generate_dashboard_html(config, datasets, output)
    html = result.read_text(encoding="utf-8")

    assert "Puget Sound vs Other WA" in html
    assert "Other WA" in html


def test_load_snotel_region_groups_prefers_wria_id(tmp_path: Path) -> None:
    paths = AppPaths(
        data_root=tmp_path / "downloaded_data",
        raw_dir=tmp_path / "downloaded_data" / "raw",
        tidy_dir=tmp_path / "downloaded_data" / "tidy",
        artifacts_dir=tmp_path / "artifacts",
    )
    stations_csv = tmp_path / "downloaded_data" / "snotel" / "WA_SNOTEL_STATION_LIST.csv"
    stations_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "snotel_station_id": [1011, 2001, 3001, 4001],
            "wria_id": [10, "", "", ""],
            "region_group": ["Other WA", "Puget Sound", "", ""],
            "legacy_regions": ["LOWER COLUMBIA", "LOWER COLUMBIA", "NORTH PUGET SOUND", "LOWER COLUMBIA"],
        }
    ).to_csv(stations_csv, index=False)

    config = AppConfig(
        paths=paths,
        sources={"snotel": {"stations_csv": str(stations_csv)}},
        quality={},
        report={"title": "Test Dashboard"},
    )

    grouped = _load_snotel_region_groups(config).set_index("snotel_station_id_norm")["snotel_region_group"].to_dict()
    assert grouped["1011"] == "Puget Sound"
    assert grouped["2001"] == "Puget Sound"
    assert grouped["3001"] == "Puget Sound"
    assert grouped["4001"] == "Other WA"


def test_generate_dashboard_html_streamflow_seasonality_section(tmp_path: Path) -> None:
    config = _config(tmp_path)
    rows: list[dict[str, object]] = []
    for year in range(2000, 2016):
        for month in range(1, 13):
            rows.append(
                {
                    "site_no": "12150800",
                    "year_nu": year,
                    "month_nu": month,
                    "monthly_cfs": float(100 + month * 10 + (year - 2000)),
                    "data_status": "approved",
                    "River Name": "Skagit",
                    "River Type": "Snow dominant",
                    "month_completeness": 1.0,
                }
            )
            rows.append(
                {
                    "site_no": "12201500",
                    "year_nu": year,
                    "month_nu": month,
                    "monthly_cfs": float(70 + month * 8 + (year - 2000)),
                    "data_status": "approved",
                    "River Name": "Chehalis",
                    "River Type": "Rain dominant",
                    "month_completeness": 1.0,
                }
            )

    datasets = {"streamflow_master": pd.DataFrame(rows)}
    output = tmp_path / "artifacts" / "dashboard_streamflow.html"
    result = generate_dashboard_html(config, datasets, output)

    html = result.read_text(encoding="utf-8")
    assert "Seasonal Hydrograph Shape by river type." in html
    assert "River Type: Snow dominant" in html
    assert "River Type: Rain dominant" in html
    assert "One-page 2x2 layout" not in html
    assert "streamflow-grid" in html


def test_generate_dashboard_html_streamflow_per_type_fallback_sections(tmp_path: Path) -> None:
    config = _config(tmp_path)
    rows: list[dict[str, object]] = []
    for month in range(1, 5):
        rows.append(
            {
                "site_no": "12150800",
                "year_nu": 2026,
                "month_nu": month,
                "monthly_cfs": float(100 + month * 10),
                "data_status": "provisional",
                "River Name": "Skagit",
                "River Type": "Snow dominant",
                "month_completeness": 1.0,
            }
        )
        rows.append(
            {
                "site_no": "12201500",
                "year_nu": 2026,
                "month_nu": month,
                "monthly_cfs": float(60 + month * 7),
                "data_status": "approved",
                "River Name": "Chehalis",
                "River Type": "Rain dominant",
                "month_completeness": 1.0,
            }
        )

    datasets = {"streamflow_master": pd.DataFrame(rows)}
    output = tmp_path / "artifacts" / "dashboard_streamflow_fallback.html"
    result = generate_dashboard_html(config, datasets, output)

    html = result.read_text(encoding="utf-8")
    assert "River Streamflows" in html
    assert "River Type: Snow dominant" in html
    assert "River Type: Rain dominant" in html
    assert "Seasonal Hydrograph Shape: unavailable" in html
    assert "One-page 2x2 layout" not in html
    assert "streamflow-grid" in html


def test_generate_dashboard_html_precip_deviation_explains_fixed_baseline(tmp_path: Path) -> None:
    config = _config(tmp_path)
    years = pd.date_range("1900-01-01", "1940-01-01", freq="YS")
    data = {
        "Precip YearDate": years,
    }
    for season in ("Spring", "Summer", "Fall", "Winter"):
        data[f"{season} Precip % Dev"] = [0.01 * ((idx % 5) - 2) for idx in range(len(years))]
        data[f"{season} Precipitation Inches"] = [10.0 + (idx % 6) for idx in range(len(years))]

    datasets = {"wa03_precip": pd.DataFrame(data)}
    output = tmp_path / "artifacts" / "dashboard_precip.html"
    result = generate_dashboard_html(config, datasets, output)

    html = result.read_text(encoding="utf-8")
    assert "Puget Sound Precipitation Deviation (5Y Avg)" in html
    assert "Puget Sound Precipitation Deviation (30Y Avg)" in html
    assert "fixed 1951-1980 baseline" in html
    assert "full 5-year windows" in html
    assert "full 30-year windows" in html


def test_dataset_max_date_handles_nclimdiv_and_grain_shapes() -> None:
    nclim = pd.DataFrame({"YEAR": ["2024", "2025"], "Month": ["11", "12"], "value": [1.0, 2.0]})
    grain = pd.DataFrame({"Market_Year": ["2020", "2023", "2025"], "Value": [1, 2, 3]})

    nclim_max = _dataset_max_date(nclim)
    grain_max = _dataset_max_date(grain)

    assert nclim_max is not None
    assert grain_max is not None
    assert pd.Timestamp(nclim_max).strftime("%Y-%m-%d") == "2025-12-01"
    assert pd.Timestamp(grain_max).strftime("%Y-%m-%d") == "2025-01-01"


def test_dataset_max_date_ignores_nclimdiv_months_with_missing_values() -> None:
    nclim = pd.DataFrame(
        {
            "YEAR": ["2026", "2026", "2025"],
            "Month": ["12", "01", "12"],
            "value": [pd.NA, 1.2, 0.8],
        }
    )
    nclim_max = _dataset_max_date(nclim)
    assert nclim_max is not None
    assert pd.Timestamp(nclim_max).strftime("%Y-%m-%d") == "2026-01-01"


def test_source_refresh_uses_entry_specific_latest_and_na_for_legacy_entry(tmp_path: Path) -> None:
    config = _config(tmp_path)
    datasets = {
        "co2": pd.DataFrame({"Date": pd.to_datetime(["2026-01-01"]), "avg CO2 ppm": [420.0]}),
        "world_bank": pd.DataFrame({"Year": [2022], "Value": [8_000_000_000]}),
        "population_combined": pd.DataFrame({"Year": [2026], "Value": [8_200_000_000]}),
        "nclimdiv_tmpcdv": pd.DataFrame(
            {
                "YEAR": ["2026", "2026", "2025"],
                "Month": ["12", "01", "12"],
                "value": [pd.NA, 1.1, 0.9],
            }
        ),
    }

    html = _source_refresh_panel_html(config, datasets)

    assert re.search(
        r"EPA Historical CO2 Composite \(legacy ice-core context\).*?<td>n/a</td><td>n/a</td>",
        html,
        flags=re.S,
    )
    assert re.search(
        r"World Bank Indicator API.*?<td>2022-01-01</td>",
        html,
        flags=re.S,
    )
    assert re.search(
        r"NOAA nClimDiv.*?<td>2026-01-01</td>",
        html,
        flags=re.S,
    )
