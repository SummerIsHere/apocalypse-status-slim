from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_validator_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "validate_snotel_station_list.py"
    spec = spec_from_file_location("validate_snotel_station_list", module_path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


VALIDATOR = _load_validator_module()


def _metadata_row(triplet: str, *, lat: float, lon: float) -> dict[str, str]:
    station_id = triplet.split(":")[0]
    return {
        "stationTriplet": triplet,
        "name": f"Station {station_id}",
        "actonId": f"{station_id}A",
        "latitude": str(lat),
        "longitude": str(lon),
        "elevation": "5000",
        "countyName": "King",
        "huc": "171100130101",
        "beginDate": "2000-10-01 00:00:00",
        "endDate": "2100-01-01 00:00:00",
        "shefId": f"S{station_id}",
    }


def test_validator_outputs_snake_case_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(VALIDATOR, "_get_valid_triplets", lambda *_args, **_kwargs: ["1011:WA:SNTL"])
    monkeypatch.setattr(
        VALIDATOR,
        "_get_metadata_rows",
        lambda _triplets: [_metadata_row("1011:WA:SNTL", lat=48.8, lon=-121.9)],
    )
    monkeypatch.setattr(VALIDATOR, "_resolve_wria", lambda _lon, _lat: (10, "Nooksack"))

    stations = pd.DataFrame(
        {
            "snotel_station_id": [1011],
            "station_triplet": ["1011:WA:SNTL"],
            "state_sub-basin_name": ["NORTH PUGET SOUND"],
        }
    )
    out = VALIDATOR.build_canonical_station_frame(stations, state_cd="WA", network_cd="SNTL")

    assert list(out.columns) == VALIDATOR.CANONICAL_COLUMNS
    assert all(column == column.lower() and " " not in column for column in out.columns)


def test_validator_reuses_existing_wria_for_unchanged_station(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(VALIDATOR, "_get_valid_triplets", lambda *_args, **_kwargs: ["1011:WA:SNTL"])
    monkeypatch.setattr(
        VALIDATOR,
        "_get_metadata_rows",
        lambda _triplets: [_metadata_row("1011:WA:SNTL", lat=48.8, lon=-121.9)],
    )

    calls: list[tuple[float, float]] = []

    def _unexpected_lookup(lon: float, lat: float) -> tuple[int | None, str | None]:
        calls.append((lon, lat))
        return 10, "Nooksack"

    monkeypatch.setattr(VALIDATOR, "_resolve_wria", _unexpected_lookup)

    stations = pd.DataFrame(
        {
            "station_triplet": ["1011:WA:SNTL"],
            "snotel_station_id": [1011],
            "latitude": [48.8],
            "longitude": [-121.9],
            "wria_id": [10],
            "wria_name": ["Nooksack"],
            "legacy_regions": ["LOWER COLUMBIA"],
        }
    )
    out = VALIDATOR.build_canonical_station_frame(stations, state_cd="WA", network_cd="SNTL")

    assert calls == []
    assert out.loc[0, "wria_id"] == 10
    assert out.loc[0, "wria_name"] == "Nooksack"


def test_validator_queries_wria_for_new_station(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(VALIDATOR, "_get_valid_triplets", lambda *_args, **_kwargs: ["2001:WA:SNTL"])
    monkeypatch.setattr(
        VALIDATOR,
        "_get_metadata_rows",
        lambda _triplets: [_metadata_row("2001:WA:SNTL", lat=47.5, lon=-121.5)],
    )

    calls: list[tuple[float, float]] = []

    def _lookup(lon: float, lat: float) -> tuple[int | None, str | None]:
        calls.append((lon, lat))
        return 12, "Skagit"

    monkeypatch.setattr(VALIDATOR, "_resolve_wria", _lookup)

    stations = pd.DataFrame(
        {
            "station_triplet": ["2001:WA:SNTL"],
            "snotel_station_id": [2001],
            "latitude": [47.5],
            "longitude": [-121.5],
            "legacy_regions": ["LOWER COLUMBIA"],
        }
    )
    out = VALIDATOR.build_canonical_station_frame(stations, state_cd="WA", network_cd="SNTL")

    assert calls == [(-121.5, 47.5)]
    assert out.loc[0, "wria_id"] == 12
    assert out.loc[0, "wria_name"] == "Skagit"


def test_validator_ignores_legacy_wria_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(VALIDATOR, "_get_valid_triplets", lambda *_args, **_kwargs: ["1011:WA:SNTL"])
    monkeypatch.setattr(
        VALIDATOR,
        "_get_metadata_rows",
        lambda _triplets: [_metadata_row("1011:WA:SNTL", lat=48.8, lon=-121.9)],
    )

    calls: list[tuple[float, float]] = []

    def _lookup(lon: float, lat: float) -> tuple[int | None, str | None]:
        calls.append((lon, lat))
        return 10, "Nooksack"

    monkeypatch.setattr(VALIDATOR, "_resolve_wria", _lookup)

    stations = pd.DataFrame(
        {
            "station_triplet": ["1011:WA:SNTL"],
            "snotel_station_id": [1011],
            "latitude": [48.8],
            "longitude": [-121.9],
            "WRIA id": [10],
            "WRIA name": ["Nooksack"],
            "legacy_regions": ["LOWER COLUMBIA"],
        }
    )
    out = VALIDATOR.build_canonical_station_frame(stations, state_cd="WA", network_cd="SNTL")

    assert calls == [(-121.9, 48.8)]
    assert out.loc[0, "wria_id"] == 10
    assert out.loc[0, "wria_name"] == "Nooksack"


def test_validator_queries_wria_when_coords_changed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(VALIDATOR, "_get_valid_triplets", lambda *_args, **_kwargs: ["1011:WA:SNTL"])
    monkeypatch.setattr(
        VALIDATOR,
        "_get_metadata_rows",
        lambda _triplets: [_metadata_row("1011:WA:SNTL", lat=48.81, lon=-121.91)],
    )

    calls: list[tuple[float, float]] = []

    def _lookup(lon: float, lat: float) -> tuple[int | None, str | None]:
        calls.append((lon, lat))
        return 10, "Nooksack"

    monkeypatch.setattr(VALIDATOR, "_resolve_wria", _lookup)

    stations = pd.DataFrame(
        {
            "station_triplet": ["1011:WA:SNTL"],
            "snotel_station_id": [1011],
            "latitude": [48.8],
            "longitude": [-121.9],
            "wria_id": [10],
            "wria_name": ["Nooksack"],
            "legacy_regions": ["NORTH PUGET SOUND"],
        }
    )
    VALIDATOR.build_canonical_station_frame(stations, state_cd="WA", network_cd="SNTL")
    assert calls == [(-121.91, 48.81)]


def test_region_group_wria_first_then_legacy_fallback() -> None:
    assert VALIDATOR._derive_region_group("LOWER COLUMBIA", 10) == "Puget Sound"
    assert VALIDATOR._derive_region_group("NORTH PUGET SOUND", pd.NA) == "Puget Sound"
    assert VALIDATOR._derive_region_group("LOWER COLUMBIA", None) == "Other WA"
