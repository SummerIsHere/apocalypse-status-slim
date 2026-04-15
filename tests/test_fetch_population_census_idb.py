from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.extract.sources import fetch_population_census_idb


class _FakeHttp:
    def __init__(self, payload: list[list[str]]) -> None:
        self._payload = payload

    def get_text(self, url: str) -> str:
        del url
        return json.dumps(self._payload)


def _make_config(tmp_path: Path) -> AppConfig:
    data_root = tmp_path / "downloaded_data"
    return AppConfig(
        paths=AppPaths(
            data_root=data_root,
            raw_dir=data_root / "raw",
            tidy_dir=data_root / "tidy",
            artifacts_dir=tmp_path / "artifacts",
        ),
        sources={
            "population_census_idb": {
                "url": "https://api.census.gov/data/timeseries/idb/5year",
                "raw_filename": "census_idb_world_population.csv",
                "start_year": 2000,
                "end_year": 2100,
                "for_geography": "genc standard countries and areas:*",
            }
        },
        quality={},
        report={},
    )


def test_fetch_population_census_idb_handles_duplicate_year_columns(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    payload = [
        ["NAME", "GENC", "YR", "POP", "YR", "genc standard countries and areas"],
        ["Alpha", "AA", "2024", "10", "2024", "AA"],
        ["Beta", "BB", "2024", "20", "2024", "BB"],
        ["Alpha", "AA", "2025", "11", "2025", "AA"],
        ["Beta", "BB", "2025", "21", "2025", "BB"],
    ]
    result = fetch_population_census_idb(config, _FakeHttp(payload))

    assert result.success is True
    assert len(result.raw_files) == 1
    output = result.raw_files[0]
    assert output.exists()

    frame = pd.read_csv(output)
    assert frame["Year"].tolist() == [2024, 2025]
    assert frame["Value"].tolist() == [30, 32]
    assert frame["Source"].unique().tolist() == ["US Census IDB"]


def test_fetch_population_census_idb_excludes_future_projection_rows_by_default(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    payload = [
        ["NAME", "GENC", "YR", "POP", "YR", "genc standard countries and areas"],
        ["Alpha", "AA", "2025", "10", "2025", "AA"],
        ["Beta", "BB", "2025", "20", "2025", "BB"],
        ["Alpha", "AA", "9999", "11", "9999", "AA"],
        ["Beta", "BB", "9999", "21", "9999", "BB"],
    ]
    result = fetch_population_census_idb(config, _FakeHttp(payload))

    frame = pd.read_csv(result.raw_files[0])
    assert frame["Year"].tolist() == [2025]
    assert frame["Value"].tolist() == [30]
