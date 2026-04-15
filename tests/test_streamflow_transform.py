from __future__ import annotations

from pathlib import Path

from apocalypse_status.transform.models import _parse_rdb, _transform_streamflow_from_rdb


def test_streamflow_mixed_status_aggregation(tmp_path: Path) -> None:
    raw = tmp_path / "usgs_streamflow_site_no_12345678_dv.rdb"
    raw.write_text(
        "\n".join(
            [
                "# comment",
                "agency_cd\tsite_no\tdatetime\t00060_00003\t00060_00003_cd",
                "5s\t15s\t10d\t14n\t10s",
                "USGS\t12345678\t2026-01-01\t10\tA",
                "USGS\t12345678\t2026-01-02\t14\tP",
                "USGS\t12345678\t2026-01-03\t16\tA",
            ]
        ),
        encoding="utf-8",
    )

    parsed = _parse_rdb(raw)
    monthly = _transform_streamflow_from_rdb(parsed, site_no="12345678", approved_only=False)

    assert len(monthly.index) == 1
    row = monthly.iloc[0]
    assert row["data_status"] == "mixed"
    assert row["provisional_day_count"] == 1
    assert row["approved_day_count"] == 2
    assert bool(row["is_provisional_month"]) is True


def test_streamflow_approved_only_filter(tmp_path: Path) -> None:
    raw = tmp_path / "usgs_streamflow_site_no_12345678_dv.rdb"
    raw.write_text(
        "\n".join(
            [
                "# comment",
                "agency_cd\tsite_no\tdatetime\t00060_00003\t00060_00003_cd",
                "5s\t15s\t10d\t14n\t10s",
                "USGS\t12345678\t2026-01-01\t10\tA",
                "USGS\t12345678\t2026-01-02\t20\tP",
            ]
        ),
        encoding="utf-8",
    )

    parsed = _parse_rdb(raw)
    monthly = _transform_streamflow_from_rdb(parsed, site_no="12345678", approved_only=True)

    assert len(monthly.index) == 1
    row = monthly.iloc[0]
    assert float(row["mean_va"]) == 10.0
    assert row["data_status"] == "approved"


def test_streamflow_three_part_value_column_parses(tmp_path: Path) -> None:
    raw = tmp_path / "usgs_streamflow_site_no_12150800_dv.rdb"
    raw.write_text(
        "\n".join(
            [
                "# comment",
                "agency_cd\tsite_no\tdatetime\t149182_00060_00003\t149182_00060_00003_cd",
                "5s\t15s\t10d\t14n\t10s",
                "USGS\t12150800\t2026-01-01\t100\tA",
                "USGS\t12150800\t2026-01-02\t200\tP",
            ]
        ),
        encoding="utf-8",
    )

    parsed = _parse_rdb(raw)
    monthly = _transform_streamflow_from_rdb(parsed, site_no="12150800", approved_only=False)

    assert len(monthly.index) == 1
    row = monthly.iloc[0]
    assert float(row["mean_va"]) == 150.0
    assert row["data_status"] == "mixed"
