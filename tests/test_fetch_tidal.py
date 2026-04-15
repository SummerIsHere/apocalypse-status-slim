from __future__ import annotations

from pathlib import Path

import requests

from apocalypse_status.config import AppConfig, AppPaths
from apocalypse_status.extract.sources import fetch_tidal


class _FakeResponse:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text


class _FakeHttp:
    def get_text(self, url: str, *, suppress_non_retryable_log: bool = False) -> str:
        if "datum=STND" in url:
            return "Year,Month,MSL\n2020,1,1.0\n"
        if "datum=NAVD" in url:
            response = _FakeResponse(
                status_code=400,
                text="The supported Datum values are: MHHW, MHW, MTL, MSL, MLW, MLLW, LWI, HWI",
            )
            raise RuntimeError("Request failed for navd url") from requests.HTTPError("400 error", response=response)
        raise RuntimeError("Unexpected URL")


class _FakeHttpOtherFailure:
    def get_text(self, url: str, *, suppress_non_retryable_log: bool = False) -> str:
        if "datum=STND" in url:
            return "Year,Month,MSL\n2020,1,1.0\n"
        if "datum=NAVD" in url:
            response = _FakeResponse(status_code=400, text="generic bad request")
            raise RuntimeError("Request failed for navd url") from requests.HTTPError("400 error", response=response)
        raise RuntimeError("Unexpected URL")


def _make_config(tmp_path: Path) -> AppConfig:
    data_root = tmp_path / "downloaded_data"
    stations_csv = data_root / "tide_gauge" / "tidal_stations.csv"
    stations_csv.parent.mkdir(parents=True, exist_ok=True)
    stations_csv.write_text(
        "NWLON Station ID,Tide Station Name\n9444900,Port Townsend\n",
        encoding="utf-8",
    )
    return AppConfig(
        paths=AppPaths(
            data_root=data_root,
            raw_dir=data_root / "raw",
            tidy_dir=data_root / "tidy",
            artifacts_dir=tmp_path / "artifacts",
        ),
        sources={"tidal": {"stations_csv": str(stations_csv)}},
        quality={},
        report={},
    )


def test_fetch_tidal_skips_unsupported_navd_without_warning(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    result = fetch_tidal(config, _FakeHttp())

    assert result.success is True
    assert result.warning is None
    assert len(result.raw_files) == 1
    assert result.raw_files[0].name.endswith("_datum_STND.csv")
    assert (result.details or {}).get("navd_skipped_unsupported") == 1


def test_fetch_tidal_counts_non_unsupported_navd_as_failure(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    result = fetch_tidal(config, _FakeHttpOtherFailure())

    assert result.success is True
    assert result.warning == "Failed 1 station/datum downloads"
    assert len(result.raw_files) == 1
    assert (result.details or {}).get("navd_skipped_unsupported") == 0
