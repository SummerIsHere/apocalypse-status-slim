"""Source extraction layer."""

from __future__ import annotations

import ftplib
import io
import json
import logging
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

import pandas as pd
import requests

from apocalypse_status.config import AppConfig
from apocalypse_status.extract.http import HttpClient, HttpConfig
from apocalypse_status.types import SourceResult


LOGGER = logging.getLogger(__name__)
FETCH_STATE_FILENAME = "fetch_state.json"
AWDB_SOAP_ENDPOINT = "https://wcc.sc.egov.usda.gov/awdbWebService/services"
AWDB_SOAP_NS = "http://www.wcc.nrcs.usda.gov/ns/awdbWebService"
SNOTEL_TRIPLET_COLUMNS = (
    "station_triplet",
    "stationTriplet",
    "awdb_station_triplet",
    "triplet",
)


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _fetch_state_path(config: AppConfig) -> Path:
    return config.paths.artifacts_dir / FETCH_STATE_FILENAME


def _load_fetch_state(config: AppConfig) -> dict[str, Any]:
    path = _fetch_state_path(config)
    if not path.exists():
        return {"version": 1, "sources": {}}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("Ignoring invalid fetch state file: %s", path)
        return {"version": 1, "sources": {}}

    sources = payload.get("sources")
    if not isinstance(sources, dict):
        return {"version": 1, "sources": {}}
    return {"version": 1, "sources": sources}


def _write_fetch_state(config: AppConfig, state: dict[str, Any]) -> None:
    path = _fetch_state_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _signature_for_source(name: str, streamflow_approved_only: bool) -> dict[str, Any]:
    if name != "streamflow":
        return {}
    return {"streamflow_approved_only": streamflow_approved_only}


def _is_complete_success(result: SourceResult) -> bool:
    if not result.success or result.warning:
        return False
    if not result.raw_files:
        return False
    return all(path.exists() for path in result.raw_files)


def _result_for_skip(name: str, entry: dict[str, Any]) -> SourceResult:
    raw_files = [Path(path) for path in entry.get("raw_files", [])]
    return SourceResult(
        source=name,
        success=True,
        raw_files=raw_files,
        warning="Skipped fetch: already succeeded today; use --force to refresh.",
        details={
            "skipped": True,
            "skip_reason": "already_succeeded_today",
            "last_success_at": entry.get("last_success_at"),
        },
    )


def _parse_station_start_date(legacy_url: str, default_start: str) -> str:
    # legacy URL example contains: ...,1943-08,2100-01
    match = re.search(r",(\d{4}-\d{2}),\d{4}-\d{2}", legacy_url)
    if match:
        return f"{match.group(1)}-01"
    return default_start


def _resolve_optional_path(raw_path: str | None, default_path: Path) -> Path:
    if not raw_path:
        return default_path
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return (Path.cwd() / path).resolve()


def _snotel_mirror_paths(mirror_dir: Path, station_key: str) -> tuple[Path, Path]:
    latest = mirror_dir / f"snotel_station_id_{station_key}_latest.csv"
    snapshot = (
        mirror_dir
        / "history"
        / f"snotel_station_id_{station_key}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
    )
    return latest, snapshot


def _is_navd_unsupported_error(exc: Exception) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, requests.HTTPError):
            response = current.response
            status = response.status_code if response is not None else None
            body = (response.text if response is not None and response.text is not None else "").lower()
            if status == 400 and "supported datum values" in body:
                return True
        current = current.__cause__ or current.__context__

    message = str(exc).lower()
    return "supported datum values" in message and "datum" in message


def _looks_like_snotel_payload(payload: str) -> bool:
    for line in payload.splitlines():
        token = line.strip()
        if not token:
            continue
        if token.startswith("#"):
            continue
        return token.startswith("Date,")
    return False


def _is_mirror_fresh(path: Path, today_token: str, max_age_days: int) -> bool:
    if max_age_days < 0:
        return True
    file_day = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).date()
    today_day = date.fromisoformat(today_token)
    age_days = (today_day - file_day).days
    return age_days <= max_age_days


def _try_use_mirror_payload(
    mirror_latest: Path,
    output_path: Path,
    *,
    today_token: str,
    allow_stale: bool,
    mirror_max_age_days: int,
) -> bool:
    if not mirror_latest.exists():
        return False
    if not allow_stale and not _is_mirror_fresh(mirror_latest, today_token, mirror_max_age_days):
        return False

    payload = mirror_latest.read_text(encoding="utf-8", errors="ignore")
    if not _looks_like_snotel_payload(payload):
        return False
    output_path.write_text(payload, encoding="utf-8")
    return True


def _write_snotel_mirror(mirror_latest: Path, mirror_snapshot: Path, payload: str) -> None:
    mirror_latest.parent.mkdir(parents=True, exist_ok=True)
    mirror_snapshot.parent.mkdir(parents=True, exist_ok=True)
    mirror_latest.write_text(payload, encoding="utf-8")
    mirror_snapshot.write_text(payload, encoding="utf-8")


def _normalize_station_token(value: Any) -> str | None:
    if pd.isna(value):
        return None
    token = str(value).strip()
    if not token or token.lower() in {"nan", "none"}:
        return None
    if re.fullmatch(r"\d+\.0+", token):
        token = token.split(".", maxsplit=1)[0]
    return token


def _normalize_triplet(value: Any) -> str | None:
    token = _normalize_station_token(value)
    if token is None:
        return None
    parts = [part.strip() for part in token.split(":")]
    if len(parts) != 3 or any(not part for part in parts):
        return None
    return f"{parts[0]}:{parts[1].upper()}:{parts[2].upper()}"


def _build_awdb_get_stations_envelope(station_ids: list[str], state_cd: str, network_cd: str) -> str:
    station_id_xml = "".join(f"<stationIds>{escape(station_id)}</stationIds>" for station_id in station_ids)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        f'xmlns:awdb="{AWDB_SOAP_NS}">'
        "<soapenv:Body>"
        "<awdb:getStations>"
        f"{station_id_xml}"
        f"<stateCds>{escape(state_cd)}</stateCds>"
        f"<networkCds>{escape(network_cd)}</networkCds>"
        "<logicalAnd>true</logicalAnd>"
        "</awdb:getStations>"
        "</soapenv:Body>"
        "</soapenv:Envelope>"
    )


def _awdb_post_get_stations(
    http: HttpClient,
    station_ids: list[str],
    state_cd: str,
    network_cd: str,
) -> list[str]:
    http_cfg = getattr(http, "config", None) or HttpConfig()
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "User-Agent": http_cfg.user_agent,
    }
    payload = _build_awdb_get_stations_envelope(station_ids, state_cd=state_cd, network_cd=network_cd)
    last_exc: Exception | None = None
    for attempt in range(1, http_cfg.retries + 1):
        try:
            response = requests.post(
                AWDB_SOAP_ENDPOINT,
                data=payload.encode("utf-8"),
                headers=headers,
                timeout=http_cfg.timeout_seconds,
            )
            response.raise_for_status()
            root = ET.fromstring(response.text)
            fault = next((node for node in root.iter() if node.tag.endswith("Fault")), None)
            if fault is not None:
                raise RuntimeError(ET.tostring(fault, encoding="unicode"))
            return [
                (node.text or "").strip()
                for node in root.iter()
                if node.tag.endswith("return") and (node.text or "").strip()
            ]
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == http_cfg.retries:
                break
    assert last_exc is not None
    raise RuntimeError("AWDB station lookup failed") from last_exc


def _resolve_snotel_fetch_targets(
    stations: pd.DataFrame,
    source_cfg: dict[str, Any],
    http: HttpClient,
) -> tuple[list[tuple[str, str]], list[str]]:
    state_cd = str(source_cfg.get("awdb_state_cd", "WA")).upper()
    network_cd = str(source_cfg.get("awdb_network_cd", "SNTL")).upper()
    validate_ids = bool(source_cfg.get("validate_ids_against_awdb", True))

    triplet_column = next((col for col in SNOTEL_TRIPLET_COLUMNS if col in stations.columns), None)
    has_station_id_column = "snotel_station_id" in stations.columns

    targets: list[tuple[str, str]] = []
    excluded: list[str] = []
    seen_station_keys: set[str] = set()
    unresolved_ids: list[str] = []

    for row in stations.to_dict(orient="records"):
        station_id = _normalize_station_token(row.get("snotel_station_id")) if has_station_id_column else None
        triplet = _normalize_triplet(row.get(triplet_column)) if triplet_column else None
        if triplet:
            triplet_id, triplet_state, triplet_network = triplet.split(":")
            station_key = station_id or triplet_id
            if validate_ids and (triplet_state != state_cd or triplet_network != network_cd):
                excluded.append(station_key)
                continue
            if station_key not in seen_station_keys:
                targets.append((station_key, triplet))
                seen_station_keys.add(station_key)
            continue
        if station_id is None:
            continue
        unresolved_ids.append(station_id)

    unresolved_ids = list(dict.fromkeys(unresolved_ids))
    if not unresolved_ids:
        return targets, excluded

    if validate_ids:
        lookup_triplets = _awdb_post_get_stations(
            http,
            unresolved_ids,
            state_cd=state_cd,
            network_cd=network_cd,
        )
        lookup_map: dict[str, str] = {}
        for triplet in lookup_triplets:
            parts = triplet.split(":")
            if len(parts) != 3:
                continue
            station_key, triplet_state, triplet_network = parts
            if triplet_state != state_cd or triplet_network != network_cd:
                continue
            if station_key in unresolved_ids:
                lookup_map[station_key] = triplet
        for station_id in unresolved_ids:
            triplet = lookup_map.get(station_id)
            if triplet is None:
                excluded.append(station_id)
                continue
            if station_id not in seen_station_keys:
                targets.append((station_id, triplet))
                seen_station_keys.add(station_id)
        return targets, excluded

    for station_id in unresolved_ids:
        if station_id in seen_station_keys:
            continue
        targets.append((station_id, f"{station_id}:{state_cd}:{network_cd}"))
        seen_station_keys.add(station_id)

    return targets, excluded


def fetch_co2(config: AppConfig, http: HttpClient) -> SourceResult:
    source_cfg = config.sources["co2"]
    out_dir = _ensure_dir(config.paths.raw_dir / "co2")
    output = out_dir / source_cfg["raw_filename"]
    output.write_text(http.get_text(source_cfg["url"]), encoding="utf-8")
    return SourceResult(source="co2", success=True, raw_files=[output])


def fetch_global_temp(config: AppConfig, http: HttpClient) -> SourceResult:
    source_cfg = config.sources["global_temp"]
    out_dir = _ensure_dir(config.paths.raw_dir / "global_temp")
    output = out_dir / source_cfg["raw_filename"]
    output.write_text(http.get_text(source_cfg["url"]), encoding="utf-8")
    return SourceResult(source="global_temp", success=True, raw_files=[output])


def fetch_nclimdiv(config: AppConfig) -> SourceResult:
    source_cfg = config.sources["nclimdiv"]
    out_dir = _ensure_dir(config.paths.raw_dir / "nclimdiv")
    ftp = ftplib.FTP(source_cfg["ftp_host"])
    ftp.login()
    ftp.cwd(source_cfg["ftp_dir"])
    listing = ftp.nlst()

    raw_files: list[Path] = []
    for metric in source_cfg["metrics"]:
        matching = sorted([entry for entry in listing if metric in entry], reverse=True)
        if not matching:
            raise RuntimeError(f"Could not find nClimDiv metric file for {metric}")
        filename = matching[0]
        output = out_dir / f"{filename}.txt"
        with output.open("wb") as handle:
            ftp.retrbinary(f"RETR {filename}", handle.write)
        raw_files.append(output)

    ftp.quit()
    return SourceResult(source="nclimdiv", success=True, raw_files=raw_files)


def fetch_snotel(
    config: AppConfig,
    http: HttpClient,
    *,
    force: bool = False,
    today: str | None = None,
    station_state: dict[str, Any] | None = None,
) -> SourceResult:
    source_cfg = config.sources["snotel"]
    stations = pd.read_csv(Path(source_cfg["stations_csv"]))
    try:
        fetch_targets, excluded_station_ids = _resolve_snotel_fetch_targets(stations, source_cfg, http)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("SNOTEL station validation failed; falling back to station IDs with default triplets: %s", exc)
        fallback_cfg = {
            "validate_ids_against_awdb": False,
            "awdb_state_cd": source_cfg.get("awdb_state_cd", "WA"),
            "awdb_network_cd": source_cfg.get("awdb_network_cd", "SNTL"),
        }
        fetch_targets, excluded_station_ids = _resolve_snotel_fetch_targets(stations, fallback_cfg, http)
    out_dir = _ensure_dir(config.paths.raw_dir / "snotel")
    mirror_dir = _ensure_dir(
        _resolve_optional_path(source_cfg.get("mirror_dir"), config.paths.raw_dir / "snotel_mirror")
    )
    mirror_prefer_first = bool(source_cfg.get("mirror_prefer_first", False))
    mirror_max_age_days = int(source_cfg.get("mirror_max_age_days", 0))
    progress_log_every = max(1, int(source_cfg.get("progress_log_every", 10)))
    today_token = today or date.today().isoformat()
    known_stations = station_state if station_state is not None else {}

    raw_files: list[Path] = []
    failed_stations: list[str] = []
    fetched_count = 0
    skipped_count = 0
    mirror_primary_count = 0
    mirror_fallback_count = 0
    total_stations = int(len(fetch_targets))
    LOGGER.info(
        "SNOTEL fetch started: total=%s excluded=%s progress_log_every=%s force=%s",
        total_stations,
        len(excluded_station_ids),
        progress_log_every,
        force,
    )

    for idx, (station_key, station_triplet) in enumerate(fetch_targets, start=1):
        output = out_dir / f"snotel_station_id_{station_key}_swe_hx.csv"
        mirror_latest, mirror_snapshot = _snotel_mirror_paths(mirror_dir, station_key)
        existing = known_stations.get(station_key, {})
        if (
            not force
            and isinstance(existing, dict)
            and existing.get("last_success_date") == today_token
            and output.exists()
        ):
            raw_files.append(output)
            skipped_count += 1
            if idx % progress_log_every == 0 or idx == total_stations:
                LOGGER.info(
                    "SNOTEL fetch progress: processed=%s/%s fetched=%s skipped=%s mirror_primary=%s mirror_fallback=%s failed=%s",
                    idx,
                    total_stations,
                    fetched_count,
                    skipped_count,
                    mirror_primary_count,
                    mirror_fallback_count,
                    len(failed_stations),
                )
            continue

        if (
            not force
            and mirror_prefer_first
            and _try_use_mirror_payload(
                mirror_latest,
                output,
                today_token=today_token,
                allow_stale=False,
                mirror_max_age_days=mirror_max_age_days,
            )
        ):
            raw_files.append(output)
            mirror_primary_count += 1
            known_stations[station_key] = {
                "last_success_date": today_token,
                "last_success_at": datetime.now(timezone.utc).isoformat(),
                "raw_file": str(output),
                "last_success_source": "mirror",
            }
            if idx % progress_log_every == 0 or idx == total_stations:
                LOGGER.info(
                    "SNOTEL fetch progress: processed=%s/%s fetched=%s skipped=%s mirror_primary=%s mirror_fallback=%s failed=%s",
                    idx,
                    total_stations,
                    fetched_count,
                    skipped_count,
                    mirror_primary_count,
                    mirror_fallback_count,
                    len(failed_stations),
                )
            continue

        prefix = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/"
        suffix = (
            '|id=%22%22|name/POR_BEGIN,POR_END/'
            'WTEQ::value,WTEQ::qcFlag,WTEQ::qaFlag,SNDN::value,SNDN::qcFlag,SNDN::qaFlag?fitToScreen=false'
        )
        url = f"{prefix}{station_triplet}{suffix}"
        try:
            payload = http.get_text(url)
            if _looks_like_snotel_payload(payload):
                output.write_text(payload, encoding="utf-8")
                _write_snotel_mirror(mirror_latest, mirror_snapshot, payload)
            else:
                raise RuntimeError("SNOTEL payload missing expected data header")
            raw_files.append(output)
            fetched_count += 1
            known_stations[station_key] = {
                "last_success_date": today_token,
                "last_success_at": datetime.now(timezone.utc).isoformat(),
                "raw_file": str(output),
                "last_success_source": "network",
            }
        except Exception as exc:  # noqa: BLE001
            if _try_use_mirror_payload(
                mirror_latest,
                output,
                today_token=today_token,
                allow_stale=True,
                mirror_max_age_days=mirror_max_age_days,
            ):
                raw_files.append(output)
                mirror_fallback_count += 1
                known_stations[station_key] = {
                    "last_success_date": today_token,
                    "last_success_at": datetime.now(timezone.utc).isoformat(),
                    "raw_file": str(output),
                    "last_success_source": "mirror_fallback",
                }
                LOGGER.warning(
                    "SNOTEL station=%s triplet=%s network failed, used mirror fallback: %s",
                    station_key,
                    station_triplet,
                    exc,
                )
                continue
            failed_stations.append(station_key)
            LOGGER.warning("Failed SNOTEL download station=%s triplet=%s: %s", station_key, station_triplet, exc)

        if idx % progress_log_every == 0 or idx == total_stations:
            LOGGER.info(
                "SNOTEL fetch progress: processed=%s/%s fetched=%s skipped=%s mirror_primary=%s mirror_fallback=%s failed=%s",
                idx,
                total_stations,
                fetched_count,
                skipped_count,
                mirror_primary_count,
                mirror_fallback_count,
                len(failed_stations),
            )

    success = len(failed_stations) == 0 and (bool(raw_files) or total_stations == 0)
    warning = None
    if failed_stations:
        sample = ", ".join(failed_stations[:5])
        warning = (
            f"Failed {len(failed_stations)} SNOTEL stations "
            f"(sample: {sample})"
        )
    elif mirror_fallback_count > 0:
        warning = f"Used mirror fallback for {mirror_fallback_count} SNOTEL stations"
    LOGGER.info(
        "SNOTEL fetch summary: total=%s excluded=%s fetched=%s skipped=%s mirror_primary=%s mirror_fallback=%s failed=%s",
        total_stations,
        len(excluded_station_ids),
        fetched_count,
        skipped_count,
        mirror_primary_count,
        mirror_fallback_count,
        len(failed_stations),
    )

    serialized_station_state = {
        str(key): value for key, value in known_stations.items() if isinstance(value, dict)
    }

    return SourceResult(
        source="snotel",
        success=success,
        raw_files=raw_files,
        warning=warning,
        details={
            "stations_total": int(len(stations.index)),
            "stations_excluded": len(excluded_station_ids),
            "excluded_station_ids_sample": excluded_station_ids[:10],
            "stations_fetched": fetched_count,
            "stations_skipped": skipped_count,
            "stations_mirror_primary": mirror_primary_count,
            "stations_mirror_fallback": mirror_fallback_count,
            "stations_failed": len(failed_stations),
            "station_state": serialized_station_state,
        },
    )


def fetch_tidal(config: AppConfig, http: HttpClient) -> SourceResult:
    source_cfg = config.sources["tidal"]
    stations = pd.read_csv(Path(source_cfg["stations_csv"]))
    out_dir = _ensure_dir(config.paths.raw_dir / "tide_gauge")

    today = date.today()
    last_day_prev_month = today.replace(day=1) - timedelta(days=1)
    end_date = last_day_prev_month.strftime("%Y%m%d")

    raw_files: list[Path] = []
    failed_downloads = 0
    skipped_navd_downloads = 0
    for station_id in stations["NWLON Station ID"].tolist():
        for datum in ("STND", "NAVD"):
            url = (
                "https://tidesandcurrents.noaa.gov/api/datagetter?"
                f"product=monthly_mean&application=NOS.COOPS.TAC.WL&begin_date=19500101&end_date={end_date}"
                f"&time_zone=GMT&units=metric&format=csv&datum={datum}&station={station_id}"
            )
            output = out_dir / f"nwlon_station_id_{station_id}_datum_{datum}.csv"
            try:
                output.write_text(
                    http.get_text(url, suppress_non_retryable_log=(datum == "NAVD")),
                    encoding="utf-8",
                )
                raw_files.append(output)
            except Exception as exc:  # noqa: BLE001
                if datum == "NAVD" and _is_navd_unsupported_error(exc):
                    skipped_navd_downloads += 1
                    LOGGER.info(
                        "Skipping tidal download station=%s datum=%s (unsupported by NOAA for this station/product)",
                        station_id,
                        datum,
                    )
                    continue
                failed_downloads += 1
                LOGGER.warning("Failed tidal download station=%s datum=%s: %s", station_id, datum, exc)

    warning = None
    if failed_downloads:
        warning = f"Failed {failed_downloads} station/datum downloads"
    return SourceResult(
        source="tidal",
        success=True,
        raw_files=raw_files,
        warning=warning,
        details={"navd_skipped_unsupported": skipped_navd_downloads},
    )


def fetch_streamflow(
    config: AppConfig,
    http: HttpClient,
    approved_only: bool = False,
) -> SourceResult:
    source_cfg = config.sources["streamflow"]
    stations = pd.read_csv(Path(source_cfg["stations_csv"]))
    out_dir = _ensure_dir(config.paths.raw_dir / "streamflow")

    raw_files: list[Path] = []
    today = date.today().strftime("%Y-%m-%d")
    default_start = source_cfg["default_start_date"]
    parameter_cd = source_cfg["parameter_cd"]

    for _, row in stations.iterrows():
        site_no = str(row["USGS Site Number"])
        legacy_url = str(row.get("URL", ""))
        start_date = _parse_station_start_date(legacy_url, default_start)
        site_status = "active" if approved_only else "all"
        url = (
            "https://waterservices.usgs.gov/nwis/dv/?format=rdb"
            f"&sites={site_no}&startDT={start_date}&endDT={today}"
            f"&parameterCd={parameter_cd}&statCd=00003&siteStatus={site_status}"
        )
        output = out_dir / f"usgs_streamflow_site_no_{site_no}_dv.rdb"
        output.write_text(http.get_text(url), encoding="utf-8")
        raw_files.append(output)

    return SourceResult(
        source="streamflow",
        success=True,
        raw_files=raw_files,
        details={
            "mode": "approved_only" if approved_only else "approved_plus_provisional",
            "parameter_cd": parameter_cd,
        },
    )


def fetch_world_bank(config: AppConfig, http: HttpClient) -> SourceResult:
    source_cfg = config.sources["world_bank"]
    out_dir = _ensure_dir(config.paths.raw_dir / "world_bank")

    country = source_cfg["country"].lower()
    indicator = source_cfg["indicator_id"]
    url = (
        "https://api.worldbank.org/v2/country/"
        f"{country}/indicator/{indicator}?format=json&per_page=20000"
    )
    output = out_dir / f"world_bank_{indicator}.json"
    data = json.loads(http.get_text(url))
    output.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return SourceResult(source="world_bank", success=True, raw_files=[output])


def fetch_population_census_idb(config: AppConfig, http: HttpClient) -> SourceResult:
    source_cfg = config.sources["population_census_idb"]
    out_dir = _ensure_dir(config.paths.raw_dir / "population")

    params: dict[str, Any] = {
        "get": "NAME,GENC,YR,POP",
        "YR": f"{int(source_cfg.get('start_year', 2000))}:{int(source_cfg.get('end_year', 2100))}",
        "for": source_cfg.get("for_geography", "genc standard countries and areas:*"),
    }
    api_key = source_cfg.get("api_key")
    if api_key:
        params["key"] = api_key

    query = urlencode(params, safe=":*")
    url = f"{source_cfg['url']}?{query}"
    payload = json.loads(http.get_text(url))
    if not isinstance(payload, list) or len(payload) < 2:
        raise RuntimeError("Unexpected Census IDB response shape")

    header = [str(col).strip() for col in payload[0]]
    rows = payload[1:]
    frame = pd.DataFrame(rows, columns=header)
    # Census IDB can return duplicate YR columns when YR is both selected and filtered.
    frame = frame.loc[:, ~frame.columns.duplicated(keep="first")]

    year_col = "YR" if "YR" in frame.columns else ("YEAR" if "YEAR" in frame.columns else None)
    if year_col is None or "POP" not in frame.columns:
        raise RuntimeError("Census IDB response missing YR/YEAR or POP columns")

    frame[year_col] = pd.to_numeric(frame[year_col], errors="coerce")
    frame["POP"] = pd.to_numeric(frame["POP"], errors="coerce")
    frame = frame.dropna(subset=[year_col, "POP"])
    include_projections = bool(source_cfg.get("include_projections", False))
    if not include_projections:
        current_year = datetime.now(timezone.utc).year
        frame = frame[frame[year_col] <= current_year]
    if frame.empty:
        raise RuntimeError("Census IDB response produced no population rows after filtering")

    world = (
        frame.groupby(year_col, as_index=False)["POP"]
        .sum()
        .rename(columns={year_col: "Year", "POP": "Value"})
        .sort_values("Year")
        .reset_index(drop=True)
    )
    world["Indicator ID"] = "CENSUS.IDB.POP"
    world["Indicator"] = "Population, total (US Census IDB aggregate)"
    world["Country"] = "World"
    world["Source"] = "US Census IDB"

    output = out_dir / source_cfg.get("raw_filename", "census_idb_world_population.csv")
    world.to_csv(output, index=False, encoding="utf-8")
    return SourceResult(source="population_census_idb", success=True, raw_files=[output])


def fetch_grain(config: AppConfig, http: HttpClient) -> SourceResult:
    source_cfg = config.sources["grain"]
    out_dir = _ensure_dir(config.paths.raw_dir / "grain")

    zip_path = out_dir / "psd_grains_pulses_csv.zip"
    zip_path.write_bytes(http.get_bytes(source_cfg["url"]))

    with zipfile.ZipFile(zip_path) as bundle:
        member_name = source_cfg["zip_member"]
        payload = bundle.read(member_name)

    csv_path = out_dir / source_cfg["zip_member"]
    csv_path.write_bytes(payload)
    return SourceResult(source="grain", success=True, raw_files=[zip_path, csv_path])


def fetch_all_sources(
    config: AppConfig,
    selected_sources: list[str] | None = None,
    streamflow_approved_only: bool = False,
    force: bool = False,
) -> list[SourceResult]:
    http = HttpClient()

    available: dict[str, Any] = {
        "co2": lambda: fetch_co2(config, http),
        "global_temp": lambda: fetch_global_temp(config, http),
        "nclimdiv": lambda: fetch_nclimdiv(config),
        "tidal": lambda: fetch_tidal(config, http),
        "streamflow": lambda: fetch_streamflow(config, http, approved_only=streamflow_approved_only),
        "world_bank": lambda: fetch_world_bank(config, http),
        "population_census_idb": lambda: fetch_population_census_idb(config, http),
        "grain": lambda: fetch_grain(config, http),
    }
    available_names = set(available) | {"snotel"}

    if selected_sources:
        unknown = sorted(set(selected_sources) - available_names)
        if unknown:
            raise ValueError(f"Unknown sources: {', '.join(unknown)}")
        source_names = selected_sources
    else:
        source_names = [name for name, cfg in config.sources.items() if cfg.get("enabled", True)]

    today = date.today().isoformat()
    state = _load_fetch_state(config)
    sources_state: dict[str, Any] = state["sources"]

    results: list[SourceResult] = []
    for name in source_names:
        signature = _signature_for_source(name, streamflow_approved_only)
        signature_key = json.dumps(signature, sort_keys=True)
        existing = sources_state.get(name)

        if (
            not force
            and isinstance(existing, dict)
            and existing.get("last_success_date") == today
            and existing.get("run_signature") == signature_key
        ):
            raw_files = [Path(path) for path in existing.get("raw_files", [])]
            if raw_files and all(path.exists() for path in raw_files):
                LOGGER.info("Skipping source %s; already fetched successfully today", name)
                results.append(_result_for_skip(name, existing))
                continue

        LOGGER.info("Fetching source: %s", name)
        try:
            if name == "snotel":
                stations_state: dict[str, Any] = {}
                if isinstance(existing, dict):
                    existing_station_state = existing.get("stations")
                    if isinstance(existing_station_state, dict):
                        stations_state = existing_station_state
                result = fetch_snotel(
                    config,
                    http,
                    force=force,
                    today=today,
                    station_state=stations_state,
                )
            else:
                result = available[name]()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Source failed: %s", name)
            result = SourceResult(source=name, success=False, warning=str(exc))

        if name == "snotel":
            snotel_state = sources_state.get(name)
            if not isinstance(snotel_state, dict):
                snotel_state = {}
                sources_state[name] = snotel_state
            station_entries = result.details.get("station_state", {})
            if isinstance(station_entries, dict):
                snotel_state["stations"] = station_entries

        if _is_complete_success(result):
            sources_state[name] = {
                "last_success_date": today,
                "last_success_at": datetime.now(timezone.utc).isoformat(),
                "run_signature": signature_key,
                "raw_files": [str(path) for path in result.raw_files],
            }
            if name == "snotel":
                station_entries = result.details.get("station_state", {})
                if isinstance(station_entries, dict):
                    sources_state[name]["stations"] = station_entries

        results.append(result)

    _write_fetch_state(config, state)

    return results
