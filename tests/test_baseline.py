from __future__ import annotations

from pathlib import Path

import pandas as pd

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.pipeline import capture_baseline_metrics


def test_capture_baseline_metrics(tmp_path: Path) -> None:
    data_root = tmp_path / "downloaded_data"
    data_root.mkdir(parents=True, exist_ok=True)

    frame = pd.DataFrame({"Date": ["2020-01-01", "2020-02-01"], "Value": [1, 2]})
    frame.to_csv(data_root / "example.csv", index=False)

    config = AppConfig(
        paths=AppPaths(
            data_root=data_root,
            raw_dir=data_root / "raw",
            tidy_dir=data_root / "tidy",
            artifacts_dir=tmp_path / "artifacts",
        ),
        sources={},
        quality={},
        report={},
    )

    output = capture_baseline_metrics(config)
    text = output.read_text(encoding="utf-8")

    assert output.exists()
    assert "example.csv" in text
