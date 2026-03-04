from __future__ import annotations

from pathlib import Path

import pandas as pd

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.transform.models import build_population_series


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


def test_build_population_series_extends_world_bank_with_census(tmp_path: Path) -> None:
    wb = pd.DataFrame({"Year": [2022, 2023, 2024], "Value": [1.0, 2.0, 3.0]})
    census = pd.DataFrame({"Year": [2024, 2025, 2026], "Value": [3.1, 4.0, 5.0]})

    combined, _ = build_population_series(_config(tmp_path), wb, census)

    years = combined["Year"].astype(int).tolist()
    values = combined["Value"].astype(float).tolist()
    assert years == [2022, 2023, 2024, 2025, 2026]
    assert values == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_build_population_series_writes_overlap_diagnostics(tmp_path: Path) -> None:
    wb = pd.DataFrame({"Year": [2022, 2023, 2024], "Value": [100.0, 110.0, 120.0]})
    census = pd.DataFrame({"Year": [2023, 2024, 2025], "Value": [111.1, 118.8, 130.0]})

    _, outputs = build_population_series(_config(tmp_path), wb, census)

    diag_path = outputs["population_overlap_diagnostics"]
    assert diag_path.exists()

    diag = pd.read_csv(diag_path)
    assert diag["Year"].astype(int).tolist() == [2023, 2024]
    assert diag["world_bank_value"].astype(float).tolist() == [110.0, 120.0]
    assert diag["census_idb_value"].astype(float).tolist() == [111.1, 118.8]
