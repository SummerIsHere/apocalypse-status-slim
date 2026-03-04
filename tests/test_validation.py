from __future__ import annotations

from pathlib import Path

import pandas as pd

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.quality.validation import validate_datasets


def _config(tmp_path: Path) -> AppConfig:
    paths = AppPaths(
        data_root=tmp_path / "downloaded_data",
        raw_dir=tmp_path / "downloaded_data" / "raw",
        tidy_dir=tmp_path / "downloaded_data" / "tidy",
        artifacts_dir=tmp_path / "artifacts",
    )
    return AppConfig(
        paths=paths,
        sources={},
        quality={
            "stale_days_default": 36500,
            "duplicate_key_checks": [
                {
                    "dataset": "tidy_data_master_usgs_streamflow",
                    "keys": ["site_no", "year_nu", "month_nu"],
                }
            ],
        },
        report={},
    )


def test_validation_duplicate_and_null_flags(tmp_path: Path) -> None:
    config = _config(tmp_path)
    datasets = {
        "streamflow_monthly": pd.DataFrame(
            {
                "site_no": ["1", "1"],
                "year_nu": [2026, 2026],
                "month_nu": [1, 1],
                "mean_va": [10.0, None],
            }
        )
    }

    issues, _ = validate_datasets(config, datasets)
    messages = [issue.message for issue in issues]

    assert any("duplicate" in msg.lower() for msg in messages)
    assert any("null fraction" in msg.lower() for msg in messages)
