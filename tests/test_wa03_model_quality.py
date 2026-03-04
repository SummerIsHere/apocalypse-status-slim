from __future__ import annotations

from pathlib import Path

import pandas as pd

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.transform.models import build_wa03_annual_temp


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


def test_build_wa03_annual_temp_excludes_incomplete_year(tmp_path: Path) -> None:
    config = _config(tmp_path)

    rows: list[dict[str, object]] = []
    for month in range(1, 13):
        rows.append(
            {
                "STATE-CODE": "45",
                "DIVISION-NUMBER": "03",
                "YEAR": 2025,
                "Month": month,
                "value": 50.0,
            }
        )
    rows.append(
        {
            "STATE-CODE": "45",
            "DIVISION-NUMBER": "03",
            "YEAR": 2026,
            "Month": 1,
            "value": 43.1,
        }
    )

    frame = pd.DataFrame(rows)
    annual, _ = build_wa03_annual_temp(config, frame)

    years = annual["YEAR"].astype(int).tolist()
    assert years == [2025]
