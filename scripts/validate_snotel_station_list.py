#!/usr/bin/env python3
"""Validate/refactor the WA SNOTEL station list against AWDB metadata."""

from __future__ import annotations

import argparse
import math
import sys
import time
from pathlib import Path
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

import pandas as pd
import requests


SOAP_URL = "https://wcc.sc.egov.usda.gov/awdbWebService/services"
SOAP_NS = "http://www.wcc.nrcs.usda.gov/ns/awdbWebService"
USER_AGENT = "apocalypse-status/0.1 (+station-list-validator)"
WRIA_QUERY_URL = "https://geodataservices.wdfw.wa.gov/arcgis/rest/services/ApplicationServices/SS2_WRIA/MapServer/0/query"
WRIA_COORD_TOLERANCE = 1e-5
CANONICAL_COLUMNS = [
    "station_triplet",
    "snotel_station_id",
    "state_cd",
    "network_cd",
    "station_name",
    "snow_survey_program_station_code",
    "latitude",
    "longitude",
    "elevation_ft",
    "county_name",
    "huc",
    "begin_date",
    "end_date",
    "shef_id",
    "legacy_regions",
    "wria_id",
    "wria_name",
    "region_group",
]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def _normalize_station_id(value: object) -> str:
    token = str(value).strip()
    if token.endswith(".0"):
        token = token[:-2]
    return token


def _request_json(
    url: str,
    *,
    params: dict[str, object],
    timeout_seconds: int = 45,
    retries: int = 3,
    backoff_seconds: float = 1.5,
) -> dict[str, object]:
    headers = {"User-Agent": USER_AGENT}
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout_seconds, headers=headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("Unexpected JSON response shape")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == retries:
                break
            time.sleep(backoff_seconds * attempt)
    assert last_exc is not None
    raise RuntimeError(f"JSON request failed: {url}") from last_exc


def _soap_call(
    body_xml: str,
    *,
    timeout_seconds: int = 60,
    retries: int = 3,
    backoff_seconds: float = 1.5,
) -> ET.Element:
    payload = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        f'xmlns:awdb="{SOAP_NS}">'
        f"<soapenv:Body>{body_xml}</soapenv:Body>"
        "</soapenv:Envelope>"
    )
    headers = {"Content-Type": "text/xml; charset=utf-8", "User-Agent": USER_AGENT}

    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                SOAP_URL,
                data=payload.encode("utf-8"),
                headers=headers,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            root = ET.fromstring(response.text)
            fault = next((node for node in root.iter() if node.tag.endswith("Fault")), None)
            if fault is not None:
                raise RuntimeError(ET.tostring(fault, encoding="unicode"))
            return root
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == retries:
                break
            time.sleep(backoff_seconds * attempt)

    assert last_exc is not None
    raise RuntimeError("AWDB SOAP call failed") from last_exc


def _extract_candidate_ids(stations: pd.DataFrame) -> list[str]:
    ids: list[str] = []
    if "snotel_station_id" in stations.columns:
        ids.extend(
            stations["snotel_station_id"]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0+$", "", regex=True)
            .tolist()
        )
    elif "station_triplet" in stations.columns:
        ids.extend(
            stations["station_triplet"]
            .astype(str)
            .str.strip()
            .str.split(":")
            .str[0]
            .tolist()
        )
    else:
        raise RuntimeError("Station CSV must include snotel_station_id or station_triplet")

    return list(dict.fromkeys([station_id for station_id in ids if station_id and station_id.lower() != "nan"]))


def _get_valid_triplets(candidate_ids: list[str], state_cd: str, network_cd: str) -> list[str]:
    valid: set[str] = set()
    for chunk in _chunks(candidate_ids, 200):
        ids_xml = "".join(f"<stationIds>{escape(station_id)}</stationIds>" for station_id in chunk)
        root = _soap_call(
            "<awdb:getStations>"
            f"{ids_xml}"
            f"<stateCds>{escape(state_cd)}</stateCds>"
            f"<networkCds>{escape(network_cd)}</networkCds>"
            "<logicalAnd>true</logicalAnd>"
            "</awdb:getStations>"
        )
        for node in root.iter():
            if not node.tag.endswith("return"):
                continue
            value = (node.text or "").strip()
            parts = value.split(":")
            if len(parts) != 3:
                continue
            if parts[1] == state_cd and parts[2] == network_cd:
                valid.add(value)
    return sorted(valid, key=lambda triplet: int(triplet.split(":", maxsplit=1)[0]))


def _get_metadata_rows(triplets: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for chunk in _chunks(triplets, 100):
        triplet_xml = "".join(f"<stationTriplets>{escape(triplet)}</stationTriplets>" for triplet in chunk)
        root = _soap_call(f"<awdb:getStationMetadataMultiple>{triplet_xml}</awdb:getStationMetadataMultiple>")
        for node in root.iter():
            if not node.tag.endswith("return"):
                continue
            row: dict[str, str] = {}
            for child in list(node):
                key = child.tag.split("}", maxsplit=1)[-1]
                row[key] = (child.text or "").strip()
            if row.get("stationTriplet"):
                rows.append(row)
    return rows


def _legacy_regions_map(stations: pd.DataFrame) -> pd.Series:
    if "state_sub-basin_name" in stations.columns:
        grouped = stations.copy()
        if "snotel_station_id" in grouped.columns:
            grouped["snotel_station_id"] = (
                grouped["snotel_station_id"].astype(str).str.strip().str.replace(r"\.0+$", "", regex=True)
            )
        return grouped.groupby("snotel_station_id")["state_sub-basin_name"].apply(
            lambda s: "; ".join(sorted({str(v).strip() for v in s.dropna().tolist() if str(v).strip()}))
        )
    if "legacy_regions" in stations.columns and "snotel_station_id" in stations.columns:
        grouped = stations.copy()
        grouped["snotel_station_id"] = (
            grouped["snotel_station_id"].astype(str).str.strip().str.replace(r"\.0+$", "", regex=True)
        )
        return grouped.groupby("snotel_station_id")["legacy_regions"].first()
    return pd.Series(dtype="object")


def _resolve_wria(lon: float, lat: float) -> tuple[int | None, str | None]:
    payload = _request_json(
        WRIA_QUERY_URL,
        params={
            "f": "pjson",
            "geometry": f"{lon},{lat}",
            "geometryType": "esriGeometryPoint",
            "inSR": 4326,
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "WRIA_NR,WRIA_NM",
            "returnGeometry": "false",
        },
    )
    features = payload.get("features")
    if not isinstance(features, list) or not features:
        return None, None
    first = features[0]
    if not isinstance(first, dict):
        return None, None
    attrs = first.get("attributes")
    if not isinstance(attrs, dict):
        return None, None
    raw_id = attrs.get("WRIA_NR")
    raw_name = attrs.get("WRIA_NM")
    wria_id: int | None
    try:
        wria_id = int(raw_id) if raw_id is not None else None
    except Exception:  # noqa: BLE001
        wria_id = None
    wria_name = str(raw_name).strip() if raw_name is not None else None
    return wria_id, wria_name


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
        if math.isnan(parsed):
            return None
        return parsed
    except Exception:  # noqa: BLE001
        return None


def _coords_unchanged(lat_a: float | None, lon_a: float | None, lat_b: float | None, lon_b: float | None) -> bool:
    if lat_a is None or lon_a is None or lat_b is None or lon_b is None:
        return False
    return abs(lat_a - lat_b) <= WRIA_COORD_TOLERANCE and abs(lon_a - lon_b) <= WRIA_COORD_TOLERANCE


def _derive_region_group(legacy_regions: str, wria_id: object) -> str:
    parsed_wria = _float_or_none(wria_id)
    if parsed_wria is not None:
        wria_int = int(parsed_wria)
        return "Puget Sound" if 1 <= wria_int <= 19 else "Other WA"
    return "Puget Sound" if "PUGET SOUND" in legacy_regions.upper() else "Other WA"


def _prior_wria_cache(stations: pd.DataFrame) -> dict[str, dict[str, object]]:
    cache: dict[str, dict[str, object]] = {}
    if "station_triplet" not in stations.columns:
        return cache

    wria_id_col = "wria_id" if "wria_id" in stations.columns else None
    wria_name_col = "wria_name" if "wria_name" in stations.columns else None
    for _, row in stations.iterrows():
        triplet = str(row.get("station_triplet", "")).strip()
        if not triplet:
            continue
        wria_id = row.get(wria_id_col) if wria_id_col else None
        wria_name = row.get(wria_name_col) if wria_name_col else None
        cache[triplet] = {
            "wria_id": wria_id,
            "wria_name": wria_name,
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
        }
    return cache


def build_canonical_station_frame(
    stations: pd.DataFrame,
    state_cd: str,
    network_cd: str,
    *,
    refresh_wria: bool = False,
) -> pd.DataFrame:
    candidate_ids = _extract_candidate_ids(stations)
    valid_triplets = _get_valid_triplets(candidate_ids, state_cd=state_cd, network_cd=network_cd)
    metadata_rows = _get_metadata_rows(valid_triplets)
    if not metadata_rows:
        raise RuntimeError("No AWDB metadata rows were returned.")

    metadata = pd.DataFrame(metadata_rows)
    metadata["station_triplet"] = metadata["stationTriplet"]
    metadata["snotel_station_id"] = metadata["station_triplet"].str.split(":").str[0]
    metadata["state_cd"] = metadata["station_triplet"].str.split(":").str[1]
    metadata["network_cd"] = metadata["station_triplet"].str.split(":").str[2]
    metadata["latitude"] = pd.to_numeric(metadata.get("latitude"), errors="coerce")
    metadata["longitude"] = pd.to_numeric(metadata.get("longitude"), errors="coerce")
    metadata["elevation_ft"] = pd.to_numeric(metadata.get("elevation"), errors="coerce").round().astype("Int64")

    canonical = (
        metadata[
            [
                "station_triplet",
                "snotel_station_id",
                "state_cd",
                "network_cd",
                "name",
                "actonId",
                "latitude",
                "longitude",
                "elevation_ft",
                "countyName",
                "huc",
                "beginDate",
                "endDate",
                "shefId",
            ]
        ]
        .rename(
            columns={
                "name": "station_name",
                "actonId": "snow_survey_program_station_code",
                "countyName": "county_name",
                "beginDate": "begin_date",
                "endDate": "end_date",
                "shefId": "shef_id",
            }
        )
        .drop_duplicates(subset=["station_triplet"])
        .sort_values("station_triplet")
        .reset_index(drop=True)
    )

    regions = _legacy_regions_map(stations)
    if not regions.empty:
        canonical = canonical.merge(regions.rename("legacy_regions"), on="snotel_station_id", how="left")
    else:
        canonical["legacy_regions"] = ""
    canonical["legacy_regions"] = canonical["legacy_regions"].fillna("").astype(str)

    prior_cache = {} if refresh_wria else _prior_wria_cache(stations)
    canonical["wria_id"] = pd.NA
    canonical["wria_name"] = pd.NA

    lookup_count = 0
    reused_count = 0
    for idx, row in canonical.iterrows():
        triplet = str(row["station_triplet"]).strip()
        cached = prior_cache.get(triplet)
        if cached:
            cached_wria_id = cached.get("wria_id")
            cached_wria_name = cached.get("wria_name")
            parsed_cached_wria = _float_or_none(cached_wria_id)
            cached_wria_name_text = "" if pd.isna(cached_wria_name) else str(cached_wria_name).strip()
            if parsed_cached_wria is not None and cached_wria_name_text:
                if _coords_unchanged(
                    _float_or_none(cached.get("latitude")),
                    _float_or_none(cached.get("longitude")),
                    _float_or_none(row.get("latitude")),
                    _float_or_none(row.get("longitude")),
                ):
                    canonical.at[idx, "wria_id"] = int(parsed_cached_wria)
                    canonical.at[idx, "wria_name"] = cached_wria_name_text
                    reused_count += 1
                    continue

        lon = _float_or_none(row.get("longitude"))
        lat = _float_or_none(row.get("latitude"))
        if lon is None or lat is None:
            continue
        wria_id, wria_name = _resolve_wria(lon, lat)
        if wria_id is not None:
            canonical.at[idx, "wria_id"] = wria_id
        if wria_name:
            canonical.at[idx, "wria_name"] = wria_name
        lookup_count += 1

    canonical["wria_id"] = pd.to_numeric(canonical["wria_id"], errors="coerce").astype("Int64")
    canonical["wria_name"] = canonical["wria_name"].fillna("")
    canonical["region_group"] = [
        _derive_region_group(legacy, wria_id)
        for legacy, wria_id in zip(canonical["legacy_regions"], canonical["wria_id"], strict=False)
    ]
    canonical = canonical[CANONICAL_COLUMNS]
    canonical.attrs["wria_lookup_count"] = lookup_count
    canonical.attrs["wria_reused_count"] = reused_count

    return canonical


def _normalized(df: pd.DataFrame) -> pd.DataFrame:
    norm = df.copy().fillna("")
    for column in norm.columns:
        norm[column] = norm[column].astype(str)
    return norm.sort_values("station_triplet").reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and refactor WA SNOTEL station list from AWDB.")
    parser.add_argument(
        "--stations-csv",
        default="downloaded_data/snotel/WA_SNOTEL_STATION_LIST.csv",
        help="Path to SNOTEL station CSV to validate/refactor.",
    )
    parser.add_argument("--state-cd", default="WA", help="AWDB state code filter.")
    parser.add_argument("--network-cd", default="SNTL", help="AWDB network code filter.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check only; exit non-zero if the file differs from AWDB canonical output.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write canonical output to --stations-csv (default action when --check is not used).",
    )
    parser.add_argument(
        "--refresh-wria",
        action="store_true",
        help="Force WRIA re-query for all stations (default reuses unchanged prior classifications).",
    )
    args = parser.parse_args()

    stations_path = Path(args.stations_csv)
    stations = pd.read_csv(stations_path)
    canonical = build_canonical_station_frame(
        stations,
        state_cd=args.state_cd.upper(),
        network_cd=args.network_cd.upper(),
        refresh_wria=args.refresh_wria,
    )

    print(
        f"AWDB canonical stations: {len(canonical)} "
        f"(state={args.state_cd.upper()} network={args.network_cd.upper()})"
    )
    print(
        "WRIA classification: "
        f"reused={canonical.attrs.get('wria_reused_count', 0)} "
        f"queried={canonical.attrs.get('wria_lookup_count', 0)}"
    )

    if args.check:
        current = _normalized(stations) if "station_triplet" in stations.columns else None
        target = _normalized(canonical)
        if current is not None and set(current.columns) == set(target.columns):
            current = current[target.columns]
            if current.equals(target):
                print("Station CSV is already canonical.")
                return 0
        print("Station CSV differs from AWDB canonical output.")
        return 1

    canonical.to_csv(stations_path, index=False)
    print(f"Wrote canonical station list to {stations_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
