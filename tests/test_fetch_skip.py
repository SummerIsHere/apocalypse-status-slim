from __future__ import annotations

from pathlib import Path

import pytest

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.extract import sources as extract_sources
from apocalypse_status.types import SourceResult


def _make_config(tmp_path: Path, sources: dict[str, dict[str, object]]) -> AppConfig:
    data_root = tmp_path / "downloaded_data"
    return AppConfig(
        paths=AppPaths(
            data_root=data_root,
            raw_dir=data_root / "raw",
            tidy_dir=data_root / "tidy",
            artifacts_dir=tmp_path / "artifacts",
        ),
        sources=sources,
        quality={},
        report={},
    )


def test_fetch_skips_when_source_already_succeeded_today(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _make_config(tmp_path, {"co2": {"enabled": True}})
    calls = {"count": 0}

    def fake_fetch_co2(config: AppConfig, http: object) -> SourceResult:
        del http
        calls["count"] += 1
        output = config.paths.raw_dir / "co2" / "co2_mm_mlo.txt"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("co2", encoding="utf-8")
        return SourceResult(source="co2", success=True, raw_files=[output])

    monkeypatch.setattr(extract_sources, "fetch_co2", fake_fetch_co2)

    first = extract_sources.fetch_all_sources(config, selected_sources=["co2"])
    second = extract_sources.fetch_all_sources(config, selected_sources=["co2"])

    assert calls["count"] == 1
    assert first[0].details.get("skipped") is None
    assert second[0].details.get("skipped") is True
    assert "already succeeded today" in (second[0].warning or "")


def test_fetch_force_overrides_daily_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path, {"co2": {"enabled": True}})
    calls = {"count": 0}

    def fake_fetch_co2(config: AppConfig, http: object) -> SourceResult:
        del http
        calls["count"] += 1
        output = config.paths.raw_dir / "co2" / "co2_mm_mlo.txt"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(f"co2-{calls['count']}", encoding="utf-8")
        return SourceResult(source="co2", success=True, raw_files=[output])

    monkeypatch.setattr(extract_sources, "fetch_co2", fake_fetch_co2)

    extract_sources.fetch_all_sources(config, selected_sources=["co2"])
    forced = extract_sources.fetch_all_sources(config, selected_sources=["co2"], force=True)

    assert calls["count"] == 2
    assert forced[0].details.get("skipped") is None


def test_streamflow_mode_change_does_not_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path, {"streamflow": {"enabled": True}})
    calls: list[bool] = []

    def fake_fetch_streamflow(
        config: AppConfig, http: object, approved_only: bool = False
    ) -> SourceResult:
        del http
        calls.append(approved_only)
        output = config.paths.raw_dir / "streamflow" / "site.rdb"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("streamflow", encoding="utf-8")
        return SourceResult(source="streamflow", success=True, raw_files=[output])

    monkeypatch.setattr(extract_sources, "fetch_streamflow", fake_fetch_streamflow)

    extract_sources.fetch_all_sources(
        config,
        selected_sources=["streamflow"],
        streamflow_approved_only=False,
    )
    second = extract_sources.fetch_all_sources(
        config,
        selected_sources=["streamflow"],
        streamflow_approved_only=True,
    )

    assert calls == [False, True]
    assert second[0].details.get("skipped") is None
