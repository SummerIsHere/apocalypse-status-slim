from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.extract import sources as extract_sources


def _make_config(
    tmp_path: Path, stations_csv: Path, snotel_overrides: dict[str, object] | None = None
) -> AppConfig:
    data_root = tmp_path / "downloaded_data"
    snotel_cfg: dict[str, object] = {
        "enabled": True,
        "stations_csv": str(stations_csv),
        "validate_ids_against_awdb": False,
        "awdb_state_cd": "WA",
        "awdb_network_cd": "SNTL",
    }
    if snotel_overrides:
        snotel_cfg.update(snotel_overrides)
    return AppConfig(
        paths=AppPaths(
            data_root=data_root,
            raw_dir=data_root / "raw",
            tidy_dir=data_root / "tidy",
            artifacts_dir=tmp_path / "artifacts",
        ),
        sources={"snotel": snotel_cfg},
        quality={},
        report={},
    )


def test_snotel_station_level_skip_retries_only_failed_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stations_csv = tmp_path / "snotel_stations.csv"
    pd.DataFrame({"snotel_station_id": [644, 645]}).to_csv(stations_csv, index=False)
    config = _make_config(tmp_path, stations_csv)

    attempts: dict[str, int] = {"644": 0, "645": 0}

    class FakeHttpClient:
        def get_text(self, url: str) -> str:
            match = re.search(r"/daily/(\d+):WA:SNTL", url)
            assert match is not None
            station = match.group(1)
            attempts[station] += 1
            if station == "645" and attempts[station] == 1:
                raise RuntimeError("simulated timeout")
            return "Date,SWE\n2026-03-01,10\n"

    monkeypatch.setattr(extract_sources, "HttpClient", FakeHttpClient)

    first = extract_sources.fetch_all_sources(config, selected_sources=["snotel"])
    second = extract_sources.fetch_all_sources(config, selected_sources=["snotel"])
    third = extract_sources.fetch_all_sources(config, selected_sources=["snotel"])

    assert first[0].details["stations_fetched"] == 1
    assert first[0].details["stations_skipped"] == 0
    assert first[0].details["stations_failed"] == 1

    assert second[0].details["stations_fetched"] == 1
    assert second[0].details["stations_skipped"] == 1
    assert second[0].details["stations_failed"] == 0

    assert attempts == {"644": 1, "645": 2}
    assert third[0].details.get("skipped") is True


def test_snotel_force_refetches_station_cached_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stations_csv = tmp_path / "snotel_stations.csv"
    pd.DataFrame({"snotel_station_id": [644, 645]}).to_csv(stations_csv, index=False)
    config = _make_config(tmp_path, stations_csv)

    attempts: dict[str, int] = {"644": 0, "645": 0}

    class FakeHttpClient:
        def get_text(self, url: str) -> str:
            match = re.search(r"/daily/(\d+):WA:SNTL", url)
            assert match is not None
            station = match.group(1)
            attempts[station] += 1
            return "Date,SWE\n2026-03-01,10\n"

    monkeypatch.setattr(extract_sources, "HttpClient", FakeHttpClient)

    extract_sources.fetch_all_sources(config, selected_sources=["snotel"])
    extract_sources.fetch_all_sources(config, selected_sources=["snotel"], force=True)

    assert attempts == {"644": 2, "645": 2}


def test_snotel_uses_mirror_first_when_enabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stations_csv = tmp_path / "snotel_stations.csv"
    pd.DataFrame({"snotel_station_id": [644]}).to_csv(stations_csv, index=False)

    mirror_dir = tmp_path / "mirror"
    mirror_dir.mkdir(parents=True, exist_ok=True)
    mirror_latest = mirror_dir / "snotel_station_id_644_latest.csv"
    mirror_latest.write_text("Date,SWE\n2026-03-01,10\n", encoding="utf-8")

    config = _make_config(
        tmp_path,
        stations_csv,
        {
            "mirror_dir": str(mirror_dir),
            "mirror_prefer_first": True,
            "mirror_max_age_days": 7,
        },
    )

    class FailIfCalledHttp:
        def get_text(self, url: str) -> str:
            raise AssertionError(f"Network should not be called when mirror-first is used: {url}")

    monkeypatch.setattr(extract_sources, "HttpClient", FailIfCalledHttp)

    result = extract_sources.fetch_all_sources(config, selected_sources=["snotel"])[0]
    assert result.success is True
    assert result.details["stations_mirror_primary"] == 1
    output = config.paths.raw_dir / "snotel" / "snotel_station_id_644_swe_hx.csv"
    assert output.exists()


def test_snotel_falls_back_to_mirror_on_network_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stations_csv = tmp_path / "snotel_stations.csv"
    pd.DataFrame({"snotel_station_id": [644]}).to_csv(stations_csv, index=False)

    mirror_dir = tmp_path / "mirror"
    mirror_dir.mkdir(parents=True, exist_ok=True)
    mirror_latest = mirror_dir / "snotel_station_id_644_latest.csv"
    mirror_latest.write_text("Date,SWE\n2026-03-01,10\n", encoding="utf-8")

    config = _make_config(
        tmp_path,
        stations_csv,
        {
            "mirror_dir": str(mirror_dir),
            "mirror_prefer_first": False,
        },
    )

    class AlwaysFailHttp:
        def get_text(self, url: str) -> str:
            raise RuntimeError(f"simulated failure for {url}")

    monkeypatch.setattr(extract_sources, "HttpClient", AlwaysFailHttp)

    result = extract_sources.fetch_all_sources(config, selected_sources=["snotel"])[0]
    assert result.success is True
    assert result.details["stations_failed"] == 0
    assert result.details["stations_mirror_fallback"] == 1
    assert "mirror fallback" in (result.warning or "").lower()


def test_snotel_validation_excludes_non_matching_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stations_csv = tmp_path / "snotel_stations.csv"
    pd.DataFrame({"snotel_station_id": [644, 645]}).to_csv(stations_csv, index=False)
    config = _make_config(
        tmp_path,
        stations_csv,
        {
            "validate_ids_against_awdb": True,
            "awdb_state_cd": "WA",
            "awdb_network_cd": "SNTL",
        },
    )

    attempted_triplets: list[str] = []

    class FakeHttpClient:
        def get_text(self, url: str) -> str:
            match = re.search(r"/daily/([^|]+)\|", url)
            assert match is not None
            attempted_triplets.append(match.group(1))
            return "Date,SWE\n2026-03-01,10\n"

    class FakeResponse:
        text = (
            '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
            "<soap:Body>"
            '<ns2:getStationsResponse xmlns:ns2="http://www.wcc.nrcs.usda.gov/ns/awdbWebService">'
            "<return>644:WA:SNTL</return>"
            "</ns2:getStationsResponse>"
            "</soap:Body>"
            "</soap:Envelope>"
        )

        @staticmethod
        def raise_for_status() -> None:
            return

    def fake_post(url: str, data: bytes, headers: dict[str, str], timeout: int) -> FakeResponse:
        assert "awdbWebService/services" in url
        assert b"<logicalAnd>true</logicalAnd>" in data
        return FakeResponse()

    monkeypatch.setattr(extract_sources, "HttpClient", FakeHttpClient)
    monkeypatch.setattr(extract_sources.requests, "post", fake_post)

    result = extract_sources.fetch_all_sources(config, selected_sources=["snotel"])[0]
    assert result.success is True
    assert result.details["stations_excluded"] == 1
    assert attempted_triplets == ["644:WA:SNTL"]
    assert (config.paths.raw_dir / "snotel" / "snotel_station_id_645_swe_hx.csv").exists() is False
