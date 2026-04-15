from __future__ import annotations

from pathlib import Path

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.transform.models import transform_global_temp


def _config(tmp_path: Path, raw_filename: str) -> AppConfig:
    return AppConfig(
        paths=AppPaths(
            data_root=tmp_path / "downloaded_data",
            raw_dir=tmp_path / "downloaded_data" / "raw",
            tidy_dir=tmp_path / "downloaded_data" / "tidy",
            artifacts_dir=tmp_path / "artifacts",
        ),
        sources={"global_temp": {"raw_filename": raw_filename}},
        quality={},
        report={},
    )


def test_transform_global_temp_parses_hadcrut5_csv(tmp_path: Path) -> None:
    raw = tmp_path / "downloaded_data" / "raw" / "global_temp"
    raw.mkdir(parents=True, exist_ok=True)
    payload = raw / "HadCRUT.5.1.0.0.analysis.summary_series.global.monthly.csv"
    payload.write_text(
        "Time,Anomaly (deg C),Lower confidence limit (2.5%),Upper confidence limit (97.5%)\n"
        "2024-11,1.25,1.12,1.37\n"
        "2024-12,1.31,1.18,1.43\n",
        encoding="utf-8",
    )

    frame, _ = transform_global_temp(_config(tmp_path, payload.name))
    assert len(frame.index) == 2
    assert str(frame.iloc[-1]["Date"].date()) == "2024-12-01"
    assert float(frame.iloc[-1]["Median Global Temp C Deviation"]) == 1.31


def test_transform_global_temp_parses_hadcrut4_text(tmp_path: Path) -> None:
    raw = tmp_path / "downloaded_data" / "raw" / "global_temp"
    raw.mkdir(parents=True, exist_ok=True)
    payload = raw / "HadCRUT.4.6.0.0.monthly_ns_avg.txt"
    payload.write_text(
        "2021/11 0.711 0.665 0.752 0.679 0.743 0.597 0.825 0.655 0.761 0.582 0.835\n"
        "2021/12 0.651 0.597 0.691 0.617 0.684 0.524 0.777 0.588 0.703 0.509 0.787\n",
        encoding="utf-8",
    )

    frame, _ = transform_global_temp(_config(tmp_path, payload.name))
    assert len(frame.index) == 2
    assert str(frame.iloc[-1]["Date"].date()) == "2021-12-01"
