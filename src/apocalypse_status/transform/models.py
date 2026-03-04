"""Transformation and modeled dataset builders."""

from __future__ import annotations

import io
import json
import logging
import re
from calendar import monthrange
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from apocalypse_status.config import AppConfig


LOGGER = logging.getLogger(__name__)


@dataclass
class TransformResult:
    datasets: dict[str, pd.DataFrame]
    outputs: dict[str, Path]


@dataclass
class SnotelParseResult:
    frame: pd.DataFrame
    status: str
    message: str | None = None


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.strip().str.replace(".", "", regex=False), errors="coerce")


def _safe_write_csv(frame: pd.DataFrame, output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False, encoding="utf-8")
    return output


def _first_existing(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"No candidate file found: {[str(c) for c in candidates]}")


def _baseline_mean(series: pd.Series, years: pd.Series) -> float:
    mask = years.between(1951, 1980)
    subset = series[mask]
    if subset.empty:
        return float(series.dropna().head(30).mean())
    return float(subset.mean())


def transform_co2(config: AppConfig) -> tuple[pd.DataFrame, dict[str, Path]]:
    raw = _first_existing(
        config.paths.raw_dir / "co2" / "co2_mm_mlo.csv",
        config.paths.data_root / "co2_mm_mlo.csv",
    )
    frame = pd.read_csv(raw, comment="#")
    frame.columns = [col.strip() for col in frame.columns]

    if "average" not in frame.columns and "average_ppm_co2" in frame.columns:
        frame["average"] = frame["average_ppm_co2"]

    frame = frame.rename(columns={"average": "avg CO2 ppm"})
    frame["Date"] = pd.to_datetime(
        frame[["year", "month"]].assign(day=1),
        errors="coerce",
    )
    frame["avg CO2 ppm"] = pd.to_numeric(frame["avg CO2 ppm"], errors="coerce")
    frame = frame[["Date", "year", "month", "avg CO2 ppm"]].dropna(subset=["Date"])

    tidy = config.paths.tidy_dir / "tidied_data_CO2_ppm.csv"
    compat = config.paths.data_root / "tidied_data_CO2_ppm.csv"
    outputs = {
        "co2_tidy": _safe_write_csv(frame, tidy),
        "co2_compat": _safe_write_csv(frame, compat),
    }
    return frame, outputs


def transform_global_temp(config: AppConfig) -> tuple[pd.DataFrame, dict[str, Path]]:
    source_cfg = config.sources.get("global_temp", {})
    configured_name = source_cfg.get("raw_filename", "HadCRUT.5.1.0.0.analysis.summary_series.global.monthly.csv")
    raw = _first_existing(
        config.paths.raw_dir / "global_temp" / str(configured_name),
        config.paths.raw_dir / "global_temp" / "HadCRUT.5.1.0.0.analysis.summary_series.global.monthly.csv",
        config.paths.raw_dir / "global_temp" / "HadCRUT.4.6.0.0.monthly_ns_avg.txt",
        config.paths.data_root / "HadCRUT.5.1.0.0.analysis.summary_series.global.monthly.csv",
        config.paths.data_root / "HadCRUT.4.6.0.0.monthly_ns_avg.txt",
    )

    cols = [
        "DateText",
        "Median Global Temp C Deviation",
        "Bias Uncertainty (Lower 95 CI)",
        "Bias Uncertainty (Upper 95 CI)",
        "Measurement and Sampling Uncertainty (Lower 95 CI)",
        "Measurement and Sampling Uncertainty (Upper 95 CI)",
        "Coverage Uncertainty (Lower 95 CI)",
        "Coverage Uncertainty (Upper 95 CI)",
        "Measurement, Sampling, and Bias Uncertainty (Lower 95 CI)",
        "Measurement, Sampling, and Bias Uncertainty (Upper 95 CI)",
        "Combined Uncertainty (Lower 95 CI)",
        "Combined Uncertainty (Upper 95 CI)",
    ]
    first_payload_line = ""
    for line in raw.read_text(encoding="utf-8", errors="ignore").splitlines():
        token = line.strip()
        if token and not token.startswith("#"):
            first_payload_line = token
            break

    if "," in first_payload_line:
        parsed = pd.read_csv(raw, comment="#")
        parsed.columns = [str(col).strip() for col in parsed.columns]
        date_col = next(
            (col for col in parsed.columns if col.lower() in {"time", "date", "datetext"}),
            None,
        )
        if date_col is None:
            date_col = parsed.columns[0]

        anomaly_col = next(
            (
                col
                for col in parsed.columns
                if "anomaly" in col.lower() and "uncert" not in col.lower() and "bound" not in col.lower()
            ),
            None,
        )
        if anomaly_col is None:
            numeric_cols = [col for col in parsed.columns if pd.api.types.is_numeric_dtype(parsed[col])]
            anomaly_col = numeric_cols[0] if numeric_cols else parsed.columns[1]

        frame = pd.DataFrame(
            {
                "DateText": parsed[date_col].astype(str),
                "Median Global Temp C Deviation": pd.to_numeric(parsed[anomaly_col], errors="coerce"),
            }
        )
    else:
        frame = pd.read_csv(raw, delimiter=r"\s+", header=None, names=cols)
        frame["DateText"] = frame["DateText"].astype(str)
        frame["Median Global Temp C Deviation"] = pd.to_numeric(
            frame["Median Global Temp C Deviation"], errors="coerce"
        )

    frame["Date"] = pd.to_datetime(
        frame["DateText"].str.replace("/", "-", regex=False),
        errors="coerce",
    )
    frame = frame.dropna(subset=["Date", "Median Global Temp C Deviation"]).sort_values("Date").reset_index(drop=True)

    tidy = config.paths.tidy_dir / "tidied_data_HadCRUT_global_temperature_anomalies.csv"
    compat = config.paths.data_root / "tidied_data_HadCRUT_global_temperature_anomalies.csv"
    outputs = {
        "global_temp_tidy": _safe_write_csv(frame, tidy),
        "global_temp_compat": _safe_write_csv(frame, compat),
    }
    return frame, outputs


def transform_nclimdiv(config: AppConfig) -> tuple[dict[str, pd.DataFrame], dict[str, Path]]:
    raw_dir = config.paths.raw_dir / "nclimdiv"
    if not raw_dir.exists():
        raw_dir = config.paths.data_root / "nclimdiv" / "raw_data"

    metric_to_file = {
        "tmpcdv": "climdiv-tmpcdv",
        "pcpndv": "climdiv-pcpndv",
        "hddcdv": "climdiv-hddcdv",
        "cddcdv": "climdiv-cddcdv",
    }

    outputs: dict[str, Path] = {}
    frames: dict[str, pd.DataFrame] = {}

    missing_sentinels = {
        "tmpcdv": -99.9,
        "pcpndv": -9.99,
        "hddcdv": -9999.0,
        "cddcdv": -9999.0,
    }

    for short, prefix in metric_to_file.items():
        matches = sorted(raw_dir.glob(f"{prefix}*.txt"), reverse=True)
        if not matches:
            legacy = config.paths.data_root / "nclimdiv" / f"tidied_data_nclimdiv-{short}.csv"
            frame = pd.read_csv(legacy, dtype=str)
        else:
            raw = matches[0]
            frame = pd.read_csv(
                raw,
                delimiter=r"\s+",
                header=None,
                dtype=str,
                names=["StateDivisionElementYear", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
            )
            frame["STATE-CODE"] = frame["StateDivisionElementYear"].str.slice(0, 2)
            frame["DIVISION-NUMBER"] = frame["StateDivisionElementYear"].str.slice(2, 4)
            frame["ELEMENT"] = frame["StateDivisionElementYear"].str.slice(4, 6)
            frame["YEAR"] = frame["StateDivisionElementYear"].str.slice(6)
            frame = frame.melt(
                id_vars=["StateDivisionElementYear", "STATE-CODE", "DIVISION-NUMBER", "ELEMENT", "YEAR"],
                var_name="Month",
                value_name="value",
            )

        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        sentinel = missing_sentinels[short]
        frame.loc[frame["value"] == sentinel, "value"] = pd.NA

        frames[short] = frame
        tidy = config.paths.tidy_dir / "nclimdiv" / f"tidied_data_nclimdiv-{short}.csv"
        compat = config.paths.data_root / "nclimdiv" / f"tidied_data_nclimdiv-{short}.csv"
        outputs[f"nclimdiv_{short}_tidy"] = _safe_write_csv(frame, tidy)
        outputs[f"nclimdiv_{short}_compat"] = _safe_write_csv(frame, compat)

    return frames, outputs


def _read_snotel_raw(raw_path: Path) -> SnotelParseResult:
    lines = raw_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    header_index: int | None = None
    for idx, line in enumerate(lines):
        if line.startswith("Date,"):
            header_index = idx
            break

    if header_index is None:
        return SnotelParseResult(
            frame=pd.DataFrame(),
            status="no_header",
            message="metadata-only file (no Date header)",
        )

    try:
        frame = pd.read_csv(raw_path, skiprows=header_index)
    except pd.errors.EmptyDataError:
        return SnotelParseResult(
            frame=pd.DataFrame(),
            status="empty_csv",
            message="header found but no tabular rows",
        )
    except Exception as exc:  # noqa: BLE001
        return SnotelParseResult(
            frame=pd.DataFrame(),
            status="parse_error",
            message=f"parser failure: {exc}",
        )

    if frame.empty:
        return SnotelParseResult(
            frame=frame,
            status="empty_csv",
            message="parsed CSV was empty",
        )

    if "Date" not in frame.columns:
        return SnotelParseResult(
            frame=pd.DataFrame(),
            status="parse_error",
            message='missing required "Date" column',
        )

    swe_columns = [col for col in frame.columns if "Snow Water Equivalent" in str(col)]
    if not swe_columns:
        return SnotelParseResult(
            frame=pd.DataFrame(),
            status="parse_error",
            message='missing expected "Snow Water Equivalent" columns',
        )

    return SnotelParseResult(frame=frame, status="ok")


def transform_snotel(config: AppConfig) -> tuple[pd.DataFrame, dict[str, Path]]:
    raw_dir = config.paths.raw_dir / "snotel"
    if not raw_dir.exists():
        raw_dir = config.paths.data_root / "snotel"

    raw_files = sorted(raw_dir.glob("snotel_station_id_*_swe_hx.csv"))
    if not raw_files:
        master = pd.read_csv(config.paths.data_root / "snotel" / "tidy_data_master_snotel_swe_hx.csv")
        output = config.paths.tidy_dir / "snotel" / "tidy_data_master_snotel_swe_hx.csv"
        outputs = {
            "snotel_master_tidy": _safe_write_csv(master, output),
            "snotel_master_compat": _safe_write_csv(
                master, config.paths.data_root / "snotel" / "tidy_data_master_snotel_swe_hx.csv"
            ),
        }
        return master, outputs

    station_frames: list[pd.DataFrame] = []
    skipped: list[tuple[int, str]] = []
    outputs: dict[str, Path] = {}

    for raw_file in raw_files:
        station_match = re.search(r"station_id_(\d+)", raw_file.name)
        if not station_match:
            continue
        station_id = int(station_match.group(1))
        parsed = _read_snotel_raw(raw_file)
        if parsed.status != "ok" or parsed.frame.empty:
            reason = parsed.message or parsed.status
            LOGGER.warning("Skipping SNOTEL station %s: %s", station_id, reason)
            skipped.append((station_id, reason))
            continue
        frame = parsed.frame
        frame["snotel_station_id"] = station_id
        tidy_name = f"tidy_data_snotel_station_id_{station_id}_swe_hx.csv"
        outputs[f"snotel_{station_id}_tidy"] = _safe_write_csv(
            frame, config.paths.tidy_dir / "snotel" / tidy_name
        )
        outputs[f"snotel_{station_id}_compat"] = _safe_write_csv(
            frame, config.paths.data_root / "snotel" / tidy_name
        )
        station_frames.append(frame)

    if not station_frames:
        raise RuntimeError("No valid SNOTEL station files were parsed; see warnings for skipped stations.")

    processed_count = len(station_frames)
    skipped_count = len(skipped)
    if skipped_count:
        sample = ", ".join(str(station_id) for station_id, _ in skipped[:5])
        LOGGER.warning(
            "SNOTEL transform completed with skipped stations: processed=%s skipped=%s sample=%s",
            processed_count,
            skipped_count,
            sample,
        )
    else:
        LOGGER.info("SNOTEL transform completed: processed=%s skipped=0", processed_count)

    master = pd.concat(station_frames, ignore_index=True).drop_duplicates()
    outputs["snotel_master_tidy"] = _safe_write_csv(
        master, config.paths.tidy_dir / "snotel" / "tidy_data_master_snotel_swe_hx.csv"
    )
    outputs["snotel_master_compat"] = _safe_write_csv(
        master, config.paths.data_root / "snotel" / "tidy_data_master_snotel_swe_hx.csv"
    )
    return master, outputs


def transform_tidal(config: AppConfig) -> tuple[pd.DataFrame, dict[str, Path]]:
    raw_dir = config.paths.raw_dir / "tide_gauge"
    if not raw_dir.exists():
        raw_dir = config.paths.data_root / "tide_gauge"

    raw_files = sorted(raw_dir.glob("nwlon_station_id_*_datum_*.csv"))
    if not raw_files:
        master = pd.read_csv(config.paths.data_root / "tide_gauge" / "tidy_data_master_tidal_data.csv")
        output = config.paths.tidy_dir / "tide_gauge" / "tidy_data_master_tidal_data.csv"
        outputs = {
            "tidal_master_tidy": _safe_write_csv(master, output),
            "tidal_master_compat": _safe_write_csv(
                master, config.paths.data_root / "tide_gauge" / "tidy_data_master_tidal_data.csv"
            ),
        }
        return master, outputs

    outputs: dict[str, Path] = {}
    station_frames: list[pd.DataFrame] = []

    for raw_file in raw_files:
        match = re.search(r"nwlon_station_id_(\d+)_datum_(\w+)", raw_file.name)
        if not match:
            continue
        station_id, datum = match.group(1), match.group(2)
        frame = pd.read_csv(raw_file)
        if frame.empty or "Error" in frame.columns:
            continue
        frame.columns = [col.strip() for col in frame.columns]
        frame["Datum"] = datum
        frame["nwlon_station_id"] = station_id
        tidy_name = f"tidied_data_nwlon_station_id_{station_id}_datum_{datum}.csv"
        outputs[f"tidal_{station_id}_{datum}_tidy"] = _safe_write_csv(
            frame, config.paths.tidy_dir / "tide_gauge" / tidy_name
        )
        outputs[f"tidal_{station_id}_{datum}_compat"] = _safe_write_csv(
            frame, config.paths.data_root / "tide_gauge" / tidy_name
        )
        station_frames.append(frame)

    master = pd.concat(station_frames, ignore_index=True).drop_duplicates()
    outputs["tidal_master_tidy"] = _safe_write_csv(
        master, config.paths.tidy_dir / "tide_gauge" / "tidy_data_master_tidal_data.csv"
    )
    outputs["tidal_master_compat"] = _safe_write_csv(
        master, config.paths.data_root / "tide_gauge" / "tidy_data_master_tidal_data.csv"
    )
    return master, outputs


def _daily_status_from_code(code: str | float | None) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return "approved"
    token = str(code).upper()
    if "P" in token:
        return "provisional"
    if "A" in token:
        return "approved"
    return "provisional"


def _parse_rdb(path: Path) -> pd.DataFrame:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    payload = [line for line in lines if line and not line.startswith("#")]
    if len(payload) < 3:
        return pd.DataFrame()

    header = payload[0]
    data_lines = payload[2:]
    text = "\n".join([header, *data_lines])
    frame = pd.read_csv(io.StringIO(text), sep="\t", dtype=str)
    return frame


def _transform_streamflow_from_rdb(
    frame: pd.DataFrame,
    site_no: str,
    approved_only: bool,
) -> pd.DataFrame:
    date_col = "datetime" if "datetime" in frame.columns else "dateTime"
    value_candidates = [col for col in frame.columns if re.match(r"^\d+_\d+_\d+$", str(col))]
    if not value_candidates:
        value_candidates = [
            col
            for col in frame.columns
            if str(col) not in {"agency_cd", "site_no", "datetime", "dateTime", "tz_cd"}
            and not str(col).endswith("_cd")
        ]
    if not value_candidates:
        raise RuntimeError(f"No daily value column found for site {site_no}")
    value_col = value_candidates[0]
    code_col = f"{value_col}_cd" if f"{value_col}_cd" in frame.columns else None

    tidy = frame[["site_no", date_col, value_col] + ([code_col] if code_col else [])].copy()
    tidy = tidy.rename(columns={date_col: "Date", value_col: "daily_cfs"})
    tidy["Date"] = pd.to_datetime(tidy["Date"], errors="coerce")
    tidy["daily_cfs"] = pd.to_numeric(tidy["daily_cfs"], errors="coerce")
    tidy = tidy.dropna(subset=["Date", "daily_cfs"])
    if tidy.empty:
        return pd.DataFrame()

    tidy["day_status"] = (
        tidy[code_col].apply(_daily_status_from_code) if code_col else "approved"
    )

    if approved_only:
        tidy = tidy[tidy["day_status"] == "approved"]

    tidy["year_nu"] = tidy["Date"].dt.year
    tidy["month_nu"] = tidy["Date"].dt.month

    grouped = (
        tidy.groupby(["site_no", "year_nu", "month_nu"], as_index=False)
        .agg(
            mean_va=("daily_cfs", "mean"),
            observed_day_count=("daily_cfs", "count"),
            provisional_day_count=("day_status", lambda s: int((s == "provisional").sum())),
            approved_day_count=("day_status", lambda s: int((s == "approved").sum())),
        )
    )

    grouped["days_in_month"] = grouped.apply(
        lambda row: monthrange(int(row["year_nu"]), int(row["month_nu"]))[1], axis=1
    )
    grouped["month_completeness"] = grouped["observed_day_count"] / grouped["days_in_month"]

    def month_status(row: pd.Series) -> str:
        if row["provisional_day_count"] > 0 and row["approved_day_count"] > 0:
            return "mixed"
        if row["provisional_day_count"] > 0:
            return "provisional"
        return "approved"

    grouped["data_status"] = grouped.apply(month_status, axis=1)
    grouped["is_provisional_month"] = grouped["data_status"] != "approved"

    return grouped[
        [
            "site_no",
            "year_nu",
            "month_nu",
            "mean_va",
            "data_status",
            "provisional_day_count",
            "approved_day_count",
            "month_completeness",
            "is_provisional_month",
        ]
    ]


def _transform_streamflow_legacy(path: Path) -> pd.DataFrame:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    payload = [line for line in lines if line and not line.startswith("#")]
    if len(payload) < 3:
        return pd.DataFrame()

    header = payload[0]
    data_lines = payload[2:]
    text = "\n".join([header, *data_lines])
    frame = pd.read_csv(io.StringIO(text), sep="\t", dtype=str)
    frame["mean_va"] = pd.to_numeric(frame["mean_va"], errors="coerce")
    frame["year_nu"] = pd.to_numeric(frame["year_nu"], errors="coerce")
    frame["month_nu"] = pd.to_numeric(frame["month_nu"], errors="coerce")
    frame = frame.dropna(subset=["mean_va", "year_nu", "month_nu"])
    frame["data_status"] = "approved"
    frame["provisional_day_count"] = 0
    frame["approved_day_count"] = 0
    frame["month_completeness"] = 1.0
    frame["is_provisional_month"] = False
    return frame[
        [
            "site_no",
            "year_nu",
            "month_nu",
            "mean_va",
            "data_status",
            "provisional_day_count",
            "approved_day_count",
            "month_completeness",
            "is_provisional_month",
        ]
    ]


def transform_streamflow(config: AppConfig, approved_only: bool) -> tuple[pd.DataFrame, dict[str, Path]]:
    raw_dir = config.paths.raw_dir / "streamflow"
    legacy_dir = config.paths.data_root / "streamflow"

    rdb_files = sorted(raw_dir.glob("usgs_streamflow_site_no_*_dv.rdb")) if raw_dir.exists() else []
    legacy_files = sorted(legacy_dir.glob("usgs_streamflow_site_no_*.txt"))

    outputs: dict[str, Path] = {}
    site_frames: list[pd.DataFrame] = []

    if rdb_files:
        for raw_file in rdb_files:
            match = re.search(r"site_no_(\d+)_dv", raw_file.name)
            if not match:
                continue
            site_no = match.group(1)
            parsed = _parse_rdb(raw_file)
            if parsed.empty:
                continue
            monthly = _transform_streamflow_from_rdb(parsed, site_no, approved_only=approved_only)
            if monthly.empty:
                continue
            per_site = config.paths.tidy_dir / "streamflow" / f"tidy_data_usgs_streamflow_site_id_{site_no}.csv"
            outputs[f"streamflow_{site_no}_tidy"] = _safe_write_csv(monthly, per_site)
            outputs[f"streamflow_{site_no}_compat"] = _safe_write_csv(
                monthly, config.paths.data_root / "streamflow" / f"tidy_data_usgs_streamflow_site_id_{site_no}.csv"
            )
            site_frames.append(monthly)
    else:
        for legacy_file in legacy_files:
            match = re.search(r"site_no_(\d+)", legacy_file.name)
            if not match:
                continue
            site_no = match.group(1)
            monthly = _transform_streamflow_legacy(legacy_file)
            if monthly.empty:
                continue
            per_site = config.paths.tidy_dir / "streamflow" / f"tidy_data_usgs_streamflow_site_id_{site_no}.csv"
            outputs[f"streamflow_{site_no}_tidy"] = _safe_write_csv(monthly, per_site)
            outputs[f"streamflow_{site_no}_compat"] = _safe_write_csv(
                monthly, config.paths.data_root / "streamflow" / f"tidy_data_usgs_streamflow_site_id_{site_no}.csv"
            )
            site_frames.append(monthly)

    if not site_frames:
        raise RuntimeError("No streamflow data available from raw or legacy sources")

    master = pd.concat(site_frames, ignore_index=True).drop_duplicates()
    master["mean_va"] = pd.to_numeric(master["mean_va"], errors="coerce")

    outputs["streamflow_master_tidy"] = _safe_write_csv(
        master, config.paths.tidy_dir / "streamflow" / "tidy_data_master_usgs_streamflow.csv"
    )
    outputs["streamflow_master_compat"] = _safe_write_csv(
        master, config.paths.data_root / "streamflow" / "tidy_data_master_usgs_streamflow.csv"
    )
    return master, outputs


def transform_world_bank(config: AppConfig) -> tuple[pd.DataFrame, dict[str, Path]]:
    raw_json = config.paths.raw_dir / "world_bank" / "world_bank_SP.POP.TOTL.json"
    outputs: dict[str, Path] = {}

    if raw_json.exists():
        payload = json.loads(raw_json.read_text(encoding="utf-8"))
        rows = payload[1] if len(payload) > 1 else []
        frame = pd.DataFrame(
            {
                "Indicator ID": [row.get("indicator", {}).get("id") for row in rows],
                "Indicator": [row.get("indicator", {}).get("value") for row in rows],
                "Country": [row.get("country", {}).get("value") for row in rows],
                "Year": [row.get("date") for row in rows],
                "Value": [row.get("value") for row in rows],
            }
        )
    else:
        legacy = config.paths.data_root / "tidied_data_world_bank_SP.POP.TOTL.csv"
        frame = pd.read_csv(legacy)

    frame["Year"] = pd.to_numeric(frame["Year"], errors="coerce")
    frame["Value"] = pd.to_numeric(frame["Value"], errors="coerce")
    frame = frame.dropna(subset=["Year", "Value"]).sort_values("Year").reset_index(drop=True)

    tidy = config.paths.tidy_dir / "tidied_data_world_bank_SP.POP.TOTL.csv"
    compat = config.paths.data_root / "tidied_data_world_bank_SP.POP.TOTL.csv"
    outputs["world_bank_tidy"] = _safe_write_csv(frame, tidy)
    outputs["world_bank_compat"] = _safe_write_csv(frame, compat)
    return frame, outputs


def transform_population_census_idb(config: AppConfig) -> tuple[pd.DataFrame, dict[str, Path]]:
    raw_csv = config.paths.raw_dir / "population" / "census_idb_world_population.csv"
    if not raw_csv.exists():
        return pd.DataFrame(columns=["Year", "Value", "Source"]), {}

    frame = pd.read_csv(raw_csv)
    frame["Year"] = pd.to_numeric(frame["Year"], errors="coerce")
    frame["Value"] = pd.to_numeric(frame["Value"], errors="coerce")
    frame = frame.dropna(subset=["Year", "Value"]).sort_values("Year").reset_index(drop=True)
    frame["Source"] = "US Census IDB"

    tidy = config.paths.tidy_dir / "tidied_data_population_census_idb.csv"
    compat = config.paths.data_root / "tidied_data_population_census_idb.csv"
    outputs = {
        "population_census_idb_tidy": _safe_write_csv(frame, tidy),
        "population_census_idb_compat": _safe_write_csv(frame, compat),
    }
    return frame, outputs


def transform_grain(config: AppConfig) -> tuple[pd.DataFrame, dict[str, Path]]:
    raw_csv = _first_existing(
        config.paths.raw_dir / "grain" / "psd_grains_pulses.csv",
        config.paths.data_root / "psd_grains_pulses.csv",
    )
    frame = pd.read_csv(raw_csv)
    frame = frame[frame["Attribute_Description"].isin(["Production", "Total Supply"])].copy()
    frame["URL"] = config.sources["grain"]["url"]

    tidy = config.paths.tidy_dir / "psd_grains_pulses.csv"
    compat = config.paths.data_root / "psd_grains_pulses.csv"
    outputs = {
        "grain_tidy": _safe_write_csv(frame, tidy),
        "grain_compat": _safe_write_csv(frame, compat),
    }
    return frame, outputs


def build_combined_co2_data(config: AppConfig, co2_frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Path]]:
    modern = (
        co2_frame.assign(Year=co2_frame["Date"].dt.year)
        .groupby("Year", as_index=False)["avg CO2 ppm"]
        .mean()
        .rename(columns={"avg CO2 ppm": "CO2 ppm"})
    )
    modern["Source"] = "NOAA Mauna Loa"

    ice_file = config.paths.data_root / "tidied_data_CO2_ice_cores.csv"
    if ice_file.exists():
        ice = pd.read_csv(ice_file)
        ice = ice.rename(columns={"CO2_ppm": "CO2 ppm"})[["Year", "CO2 ppm", "Source"]]
        combined = pd.concat([ice, modern], ignore_index=True)
    else:
        combined = modern

    combined = combined.sort_values("Year").reset_index(drop=True)
    output = config.paths.tidy_dir / "models" / "combined_co2_data.csv"
    compat = config.paths.data_root / "tidied_data_combined_co2_data.csv"
    outputs = {
        "combined_co2_model": _safe_write_csv(combined, output),
        "combined_co2_compat": _safe_write_csv(combined, compat),
    }
    return combined, outputs


def build_population_series(
    config: AppConfig,
    world_bank_frame: pd.DataFrame,
    census_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    wb = world_bank_frame[["Year", "Value"]].copy()
    wb["Source"] = "World Bank"

    combined = wb.copy()
    overlap = pd.DataFrame(
        columns=[
            "Year",
            "world_bank_value",
            "census_idb_value",
            "pct_diff_vs_world_bank",
            "abs_pct_diff_vs_world_bank",
        ]
    )
    if not census_frame.empty:
        census = census_frame[["Year", "Value"]].copy()
        census["Source"] = "US Census IDB"
        overlap = (
            wb[["Year", "Value"]]
            .rename(columns={"Value": "world_bank_value"})
            .merge(
                census[["Year", "Value"]].rename(columns={"Value": "census_idb_value"}),
                on="Year",
                how="inner",
            )
            .sort_values("Year")
            .reset_index(drop=True)
        )
        if not overlap.empty:
            overlap["pct_diff_vs_world_bank"] = (
                (overlap["census_idb_value"] - overlap["world_bank_value"]) / overlap["world_bank_value"]
            ) * 100.0
            overlap.loc[overlap["world_bank_value"] <= 0, "pct_diff_vs_world_bank"] = pd.NA
            overlap["abs_pct_diff_vs_world_bank"] = overlap["pct_diff_vs_world_bank"].abs()

            valid_abs = pd.to_numeric(overlap["abs_pct_diff_vs_world_bank"], errors="coerce").dropna()
            if not valid_abs.empty:
                median_abs = float(valid_abs.median())
                max_abs = float(valid_abs.max())
                threshold = float(config.quality.get("population_overlap_warn_pct", 2.5))
                LOGGER.info(
                    "Population overlap check WB vs Census: years=%s median_abs_pct_diff=%.3f max_abs_pct_diff=%.3f",
                    int(len(overlap.index)),
                    median_abs,
                    max_abs,
                )
                if median_abs > threshold:
                    LOGGER.warning(
                        "Population overlap differs more than expected: median_abs_pct_diff=%.3f threshold=%.3f",
                        median_abs,
                        threshold,
                    )

        max_wb_year = int(wb["Year"].max()) if not wb.empty else -1
        census_extension = census[census["Year"] > max_wb_year]
        if not census_extension.empty:
            combined = pd.concat([wb, census_extension], ignore_index=True)

    combined = combined.dropna(subset=["Year", "Value"]).sort_values("Year").reset_index(drop=True)

    output = config.paths.tidy_dir / "models" / "population_combined.csv"
    overlap_output = config.paths.tidy_dir / "models" / "population_overlap_wb_vs_census.csv"
    compat = config.paths.data_root / "tidied_data_population_combined.csv"
    outputs = {
        "population_combined_model": _safe_write_csv(combined, output),
        "population_overlap_diagnostics": _safe_write_csv(overlap, overlap_output),
        "population_combined_compat": _safe_write_csv(combined, compat),
    }
    return combined, outputs


def build_grain_per_capita(
    config: AppConfig,
    grain_frame: pd.DataFrame,
    population_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    world_supply = grain_frame[
        (grain_frame["Attribute_Description"] == "Total Supply") & (grain_frame["Country_Name"] == "World")
    ].copy()

    if world_supply.empty:
        world_supply = (
            grain_frame[grain_frame["Attribute_Description"] == "Total Supply"]
            .groupby("Market_Year", as_index=False)["Value"]
            .sum()
            .rename(columns={"Market_Year": "Year"})
        )
    else:
        world_supply = world_supply.rename(columns={"Market_Year": "Year"})[["Year", "Value"]]

    world_supply["Year"] = pd.to_numeric(world_supply["Year"], errors="coerce")
    world_supply["Value"] = pd.to_numeric(world_supply["Value"], errors="coerce")
    pop = population_frame[["Year", "Value"]].rename(columns={"Value": "Population"})

    merged = world_supply.merge(pop, on="Year", how="inner").dropna()
    merged["kg Grain Per Capita"] = (merged["Value"] * 1_000_000.0) / merged["Population"]
    merged["Date"] = pd.to_datetime(merged["Year"].astype(int).astype(str) + "-01-01")
    merged = merged[["Date", "Year", "kg Grain Per Capita"]].sort_values("Year")

    output = config.paths.tidy_dir / "models" / "grain_per_capita.csv"
    compat = config.paths.data_root / "tidied_data_grain_per_capita.csv"
    outputs = {
        "grain_per_capita_model": _safe_write_csv(merged, output),
        "grain_per_capita_compat": _safe_write_csv(merged, compat),
    }
    return merged, outputs


def _wa03_metric(frame: pd.DataFrame) -> pd.DataFrame:
    metric = frame.copy()
    metric["STATE-CODE"] = metric["STATE-CODE"].astype(str)
    metric["DIVISION-NUMBER"] = metric["DIVISION-NUMBER"].astype(str)
    metric = metric[(metric["STATE-CODE"] == "45") & (metric["DIVISION-NUMBER"] == "03")].copy()
    metric["YEAR"] = pd.to_numeric(metric["YEAR"], errors="coerce")
    metric["Month"] = pd.to_numeric(metric["Month"], errors="coerce")
    metric["value"] = pd.to_numeric(metric["value"], errors="coerce")
    metric = metric.dropna(subset=["YEAR", "Month", "value"])
    return metric


def _complete_years_only(metric: pd.DataFrame) -> pd.DataFrame:
    counts = metric.groupby("YEAR", as_index=False)["Month"].nunique().rename(columns={"Month": "month_count"})
    complete_years = counts[counts["month_count"] == 12]["YEAR"]
    return metric[metric["YEAR"].isin(complete_years)].copy()


def build_wa03_annual_temp(config: AppConfig, nclim_tmp: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Path]]:
    tmp = _complete_years_only(_wa03_metric(nclim_tmp))
    annual = tmp.groupby("YEAR", as_index=False)["value"].mean().rename(columns={"value": "Avg Annual Temp F"})
    baseline = _baseline_mean(annual["Avg Annual Temp F"], annual["YEAR"])
    annual["Temp F Deviation"] = annual["Avg Annual Temp F"] - baseline
    annual["5 yr avg"] = annual["Temp F Deviation"].rolling(window=5, min_periods=1).mean()
    annual["Temp F Deviation rolling average"] = annual["Temp F Deviation"].rolling(window=30, min_periods=1).mean()
    annual["RPT_MO_STRT_DT"] = pd.to_datetime(annual["YEAR"].astype(int).astype(str) + "-01-01")

    output = config.paths.tidy_dir / "models" / "wa03_annual_temp.csv"
    compat = config.paths.data_root / "tidied_data_wa03_annual_temp.csv"
    outputs = {
        "wa03_annual_temp_model": _safe_write_csv(annual, output),
        "wa03_annual_temp_compat": _safe_write_csv(annual, compat),
    }
    return annual, outputs


def _season_from_month(month: int) -> str:
    if month in (12, 1, 2):
        return "Winter"
    if month in (3, 4, 5):
        return "Spring"
    if month in (6, 7, 8):
        return "Summer"
    return "Fall"


def build_wa03_precip(config: AppConfig, nclim_precip: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Path]]:
    precip = _complete_years_only(_wa03_metric(nclim_precip))
    precip["season"] = precip["Month"].astype(int).apply(_season_from_month)
    precip["season_year"] = precip["YEAR"].astype(int)
    precip.loc[precip["Month"].astype(int) == 12, "season_year"] += 1

    seasonal = (
        precip.groupby(["season_year", "season"], as_index=False)["value"]
        .sum()
        .pivot(index="season_year", columns="season", values="value")
        .reset_index()
        .rename(columns={"season_year": "Year"})
    )
    seasonal = seasonal.dropna(subset=["Spring", "Summer", "Fall", "Winter"])

    for season in ("Spring", "Summer", "Fall", "Winter"):
        if season not in seasonal.columns:
            seasonal[season] = pd.NA
        seasonal[f"{season} Precipitation Inches"] = seasonal[season]

    seasonal["Precip YearDate"] = pd.to_datetime(seasonal["Year"].astype(int).astype(str) + "-01-01")

    baseline_slice = seasonal[seasonal["Year"].between(1951, 1980)]
    if baseline_slice.empty:
        baseline_slice = seasonal

    for season in ("Spring", "Summer", "Fall", "Winter"):
        baseline = baseline_slice[season].mean()
        seasonal[f"{season} Precip % Dev"] = (seasonal[season] - baseline) / baseline
        seasonal[f"{season} Precip % Dev 5 Yr Avg"] = (
            seasonal[f"{season} Precip % Dev"].rolling(window=5, min_periods=1).mean()
        )
        seasonal[f"{season} Precip % Dev 30 Yr Avg"] = (
            seasonal[f"{season} Precip % Dev"].rolling(window=30, min_periods=1).mean()
        )

    output = config.paths.tidy_dir / "models" / "wa03_precip.csv"
    compat = config.paths.data_root / "tidied_data_wa03_precip.csv"
    outputs = {
        "wa03_precip_model": _safe_write_csv(seasonal, output),
        "wa03_precip_compat": _safe_write_csv(seasonal, compat),
    }
    return seasonal, outputs


def build_wa03_degree_days(
    config: AppConfig,
    nclim_hdd: pd.DataFrame,
    nclim_cdd: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    hdd = _complete_years_only(_wa03_metric(nclim_hdd)).groupby("YEAR", as_index=False)["value"].sum().rename(
        columns={"YEAR": "Year", "value": "Annual Heating Degree Days"}
    )
    cdd = _complete_years_only(_wa03_metric(nclim_cdd)).groupby("YEAR", as_index=False)["value"].sum().rename(
        columns={"YEAR": "Year", "value": "Annual Cooling Degree Days"}
    )

    merged = hdd.merge(cdd, on="Year", how="outer").sort_values("Year").reset_index(drop=True)

    hdd_base = _baseline_mean(merged["Annual Heating Degree Days"], merged["Year"])
    cdd_base = _baseline_mean(merged["Annual Cooling Degree Days"], merged["Year"])

    merged["Base Annual Heating Degree Days"] = hdd_base
    merged["Base Annual Cooling Degree Days"] = cdd_base

    merged["Heating Degree Days Deviation"] = merged["Annual Heating Degree Days"] - hdd_base
    merged["Cooling Degree Days Deviation"] = merged["Annual Cooling Degree Days"] - cdd_base

    merged["Annual Heating Degree Days 5 Yrs Avg"] = (
        merged["Annual Heating Degree Days"].rolling(window=5, min_periods=1).mean()
    )
    merged["Annual Heating Degree Days 30 Yrs Avg"] = (
        merged["Annual Heating Degree Days"].rolling(window=30, min_periods=1).mean()
    )
    merged["Annual Cooling Degree Days 5 Yrs Avg"] = (
        merged["Annual Cooling Degree Days"].rolling(window=5, min_periods=1).mean()
    )
    merged["Annual Cooling Degree Days 30 Yrs Avg"] = (
        merged["Annual Cooling Degree Days"].rolling(window=30, min_periods=1).mean()
    )

    merged["Heating Degree Days Deviation 5 Yr Avg"] = (
        merged["Heating Degree Days Deviation"].rolling(window=5, min_periods=1).mean()
    )
    merged["Heating Degree Days Deviation 30 Yr Avg"] = (
        merged["Heating Degree Days Deviation"].rolling(window=30, min_periods=1).mean()
    )
    merged["Cooling Degree Days Deviation 5 Yr Avg"] = (
        merged["Cooling Degree Days Deviation"].rolling(window=5, min_periods=1).mean()
    )
    merged["Cooling Degree Days Deviation 30 Yr Avg"] = (
        merged["Cooling Degree Days Deviation"].rolling(window=30, min_periods=1).mean()
    )

    # Scenario placeholders represented as static reference values.
    merged["2050 Mod Emissions Lower Bound HDD"] = hdd_base - 2300
    merged["2050 Mod Emissions Avg HDD"] = hdd_base - 1600
    merged["2050 Mod Emissions Higher Bound HDD"] = hdd_base - 1000

    merged["2050 Mod Emissions Lower Bound CDD"] = cdd_base + 5
    merged["2050 Mod Emissions Avg CDD"] = cdd_base + 17
    merged["2050 Mod Emissions Higher Bound CDD"] = cdd_base + 56

    merged["DD Year Date"] = pd.to_datetime(merged["Year"].astype(int).astype(str) + "-01-01")

    output = config.paths.tidy_dir / "models" / "wa03_degree_days.csv"
    compat = config.paths.data_root / "tidied_data_wa03_degree_days.csv"
    outputs = {
        "wa03_degree_days_model": _safe_write_csv(merged, output),
        "wa03_degree_days_compat": _safe_write_csv(merged, compat),
    }
    return merged, outputs


def build_streamflow_master(config: AppConfig, streamflow_monthly: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Path]]:
    stations = pd.read_csv(config.paths.data_root / "streamflow" / "streamflow_stations.csv")
    stations["USGS Site Number"] = stations["USGS Site Number"].astype(str)

    master = streamflow_monthly.copy()
    master["site_no"] = master["site_no"].astype(str)
    master["month_nu"] = pd.to_numeric(master["month_nu"], errors="coerce")
    master["Month Abbr"] = pd.to_datetime(master["month_nu"], format="%m", errors="coerce").dt.strftime("%b")
    master = master.rename(columns={"mean_va": "monthly_cfs"})

    master = master.merge(
        stations[["USGS Site Number", "River Name", "River Type"]],
        left_on="site_no",
        right_on="USGS Site Number",
        how="left",
    ).drop(columns=["USGS Site Number"])

    output = config.paths.tidy_dir / "models" / "streamflow_master.csv"
    compat = config.paths.data_root / "tidied_data_streamflow_master.csv"
    outputs = {
        "streamflow_master_model": _safe_write_csv(master, output),
        "streamflow_master_compat": _safe_write_csv(master, compat),
    }
    return master, outputs


def build_annual_avg_msl(config: AppConfig, tidal_master: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Path]]:
    frame = tidal_master.copy()
    frame["Datum"] = frame["Datum"].astype(str)
    frame = frame[frame["Datum"] == "STND"].copy()
    frame["Year"] = pd.to_numeric(frame["Year"], errors="coerce")
    frame["MSL"] = pd.to_numeric(frame["MSL"], errors="coerce")
    frame = frame.dropna(subset=["Year", "MSL"])

    annual = frame.groupby("Year", as_index=False)["MSL"].mean().rename(columns={"MSL": "Annual Avg MSL (m)"})
    baseline_row = annual[annual["Year"] == 2000]
    baseline = baseline_row["Annual Avg MSL (m)"].iloc[0] if not baseline_row.empty else annual["Annual Avg MSL (m)"].mean()

    annual["Change in Mean Sea Level since 2000 (in)"] = (annual["Annual Avg MSL (m)"] - baseline) * 39.3701
    annual["Year Date"] = pd.to_datetime(annual["Year"].astype(int).astype(str) + "-01-01")

    output = config.paths.tidy_dir / "models" / "annual_avg_msl_cf_stnd.csv"
    compat = config.paths.data_root / "tidied_data_annual_avg_msl_cf_stnd.csv"
    outputs = {
        "annual_avg_msl_model": _safe_write_csv(annual, output),
        "annual_avg_msl_compat": _safe_write_csv(annual, compat),
    }
    return annual, outputs


def transform_all_datasets(config: AppConfig, streamflow_approved_only: bool = False) -> TransformResult:
    datasets: dict[str, pd.DataFrame] = {}
    outputs: dict[str, Path] = {}

    co2, co2_out = transform_co2(config)
    datasets["co2"] = co2
    outputs.update(co2_out)

    temp, temp_out = transform_global_temp(config)
    datasets["global_temp"] = temp
    outputs.update(temp_out)

    nclim_frames, nclim_out = transform_nclimdiv(config)
    datasets.update({f"nclimdiv_{k}": v for k, v in nclim_frames.items()})
    outputs.update(nclim_out)

    snotel, snotel_out = transform_snotel(config)
    datasets["snotel_master"] = snotel
    outputs.update(snotel_out)

    tidal, tidal_out = transform_tidal(config)
    datasets["tidal_master"] = tidal
    outputs.update(tidal_out)

    streamflow, streamflow_out = transform_streamflow(config, approved_only=streamflow_approved_only)
    datasets["streamflow_monthly"] = streamflow
    outputs.update(streamflow_out)

    world_bank, wb_out = transform_world_bank(config)
    datasets["world_bank"] = world_bank
    outputs.update(wb_out)

    population_census, pop_census_out = transform_population_census_idb(config)
    datasets["population_census_idb"] = population_census
    outputs.update(pop_census_out)

    grain, grain_out = transform_grain(config)
    datasets["grain"] = grain
    outputs.update(grain_out)

    combined_co2, combined_out = build_combined_co2_data(config, co2)
    datasets["combined_co2"] = combined_co2
    outputs.update(combined_out)

    population_combined, population_combined_out = build_population_series(
        config,
        world_bank,
        population_census,
    )
    datasets["population_combined"] = population_combined
    outputs.update(population_combined_out)

    grain_pc, grain_pc_out = build_grain_per_capita(config, grain, population_combined)
    datasets["grain_per_capita"] = grain_pc
    outputs.update(grain_pc_out)

    wa03_temp, wa03_temp_out = build_wa03_annual_temp(config, nclim_frames["tmpcdv"])
    datasets["wa03_annual_temp"] = wa03_temp
    outputs.update(wa03_temp_out)

    wa03_precip, wa03_precip_out = build_wa03_precip(config, nclim_frames["pcpndv"])
    datasets["wa03_precip"] = wa03_precip
    outputs.update(wa03_precip_out)

    wa03_dd, wa03_dd_out = build_wa03_degree_days(config, nclim_frames["hddcdv"], nclim_frames["cddcdv"])
    datasets["wa03_degree_days"] = wa03_dd
    outputs.update(wa03_dd_out)

    streamflow_master, sf_master_out = build_streamflow_master(config, streamflow)
    datasets["streamflow_master"] = streamflow_master
    outputs.update(sf_master_out)

    annual_msl, annual_msl_out = build_annual_avg_msl(config, tidal)
    datasets["annual_avg_msl"] = annual_msl
    outputs.update(annual_msl_out)

    return TransformResult(datasets=datasets, outputs=outputs)
