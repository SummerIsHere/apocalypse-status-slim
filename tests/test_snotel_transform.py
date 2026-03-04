from __future__ import annotations

from pathlib import Path

import pytest

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.transform.models import transform_snotel


def _config(tmp_path: Path) -> AppConfig:
    data_root = tmp_path / "downloaded_data"
    raw_dir = data_root / "raw"
    tidy_dir = data_root / "tidy"
    artifacts_dir = tmp_path / "artifacts"
    for path in [data_root / "snotel", raw_dir / "snotel", tidy_dir / "snotel", artifacts_dir]:
        path.mkdir(parents=True, exist_ok=True)

    return AppConfig(
        paths=AppPaths(
            data_root=data_root,
            raw_dir=raw_dir,
            tidy_dir=tidy_dir,
            artifacts_dir=artifacts_dir,
        ),
        sources={},
        quality={},
        report={},
    )


def test_transform_snotel_mixed_files_succeeds(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    config = _config(tmp_path)
    raw_dir = config.paths.raw_dir / "snotel"

    valid = raw_dir / "snotel_station_id_376_swe_hx.csv"
    valid.write_text(
        "\n".join(
            [
                "# comment",
                "Date,Snow Water Equivalent (in) Start of Day Values,Snow Water Equivalent QC Flag Start of Day Values",
                "2026-01-01,11.1,E",
                "2026-01-02,10.9,E",
            ]
        ),
        encoding="utf-8",
    )

    metadata_only = raw_dir / "snotel_station_id_530_swe_hx.csv"
    metadata_only.write_text(
        "# warning\n# metadata only\n",
        encoding="utf-8",
    )

    master, outputs = transform_snotel(config)

    assert not master.empty
    assert "snotel_station_id" in master.columns
    assert int(master["snotel_station_id"].iloc[0]) == 376
    assert "snotel_master_tidy" in outputs
    assert "snotel_master_compat" in outputs
    assert any("Skipping SNOTEL station 530" in record.message for record in caplog.records)


def test_transform_snotel_all_metadata_raises_clear_error(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw_dir = config.paths.raw_dir / "snotel"

    metadata_only = raw_dir / "snotel_station_id_530_swe_hx.csv"
    metadata_only.write_text(
        "# warning\n# metadata only\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="No valid SNOTEL station files were parsed"):
        transform_snotel(config)
