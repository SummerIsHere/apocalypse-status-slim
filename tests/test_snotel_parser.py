from __future__ import annotations

from pathlib import Path

from apocalypse_status.transform.models import _read_snotel_raw


def test_read_snotel_raw_metadata_only_returns_no_header(tmp_path: Path) -> None:
    raw = tmp_path / "snotel_station_id_530_swe_hx.csv"
    raw.write_text(
        "# warning\n# metadata only\n# As of: 2026-03-03\n",
        encoding="utf-8",
    )

    result = _read_snotel_raw(raw)

    assert result.status == "no_header"
    assert result.frame.empty
    assert result.message is not None


def test_read_snotel_raw_valid_file_parses(tmp_path: Path) -> None:
    raw = tmp_path / "snotel_station_id_376_swe_hx.csv"
    raw.write_text(
        "\n".join(
            [
                "# comment",
                "# another comment",
                "Date,Snow Water Equivalent (in) Start of Day Values,Snow Water Equivalent QC Flag Start of Day Values",
                "2026-01-01,12.3,E",
                "2026-01-02,12.0,E",
            ]
        ),
        encoding="utf-8",
    )

    result = _read_snotel_raw(raw)

    assert result.status == "ok"
    assert not result.frame.empty
    assert "Date" in result.frame.columns


def test_read_snotel_raw_header_only_returns_empty_csv(tmp_path: Path) -> None:
    raw = tmp_path / "snotel_station_id_600_swe_hx.csv"
    raw.write_text(
        "\n".join(
            [
                "# comment",
                "Date,Snow Water Equivalent (in) Start of Day Values,Snow Water Equivalent QC Flag Start of Day Values",
            ]
        ),
        encoding="utf-8",
    )

    result = _read_snotel_raw(raw)

    assert result.status == "empty_csv"
    assert result.frame.empty
