"""Dataset validation and profiling."""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from apocalypse_status.config import AppConfig
from apocalypse_status.types import ValidationIssue


LOGGER = logging.getLogger(__name__)


def _series_to_datetime(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, errors="coerce", format="mixed")
    except TypeError:
        return pd.to_datetime(series, errors="coerce")


def profile_dataset(name: str, frame: pd.DataFrame) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "rows": int(len(frame.index)),
        "columns": list(frame.columns),
    }

    date_cols = [col for col in frame.columns if "date" in col.lower() or col.lower().endswith("_dt")]
    if date_cols:
        series = _series_to_datetime(frame[date_cols[0]])
        profile["min_date"] = str(series.min().date()) if series.notna().any() else None
        profile["max_date"] = str(series.max().date()) if series.notna().any() else None

    numeric_cols = frame.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        profile["numeric_columns"] = numeric_cols
        profile["null_fraction_by_numeric"] = {
            col: float(frame[col].isna().mean()) for col in numeric_cols
        }

    return profile


def _resolve_dataset_name(name: str, datasets: dict[str, pd.DataFrame]) -> str | None:
    if name in datasets:
        return name

    alias_map = {
        "tidy_data_master_usgs_streamflow": "streamflow_monthly",
        "tidied_data_HadCRUT_global_temperature_anomalies": "global_temp",
    }
    alias = alias_map.get(name)
    if alias in datasets:
        return alias
    return None


def validate_datasets(config: AppConfig, datasets: dict[str, pd.DataFrame]) -> tuple[list[ValidationIssue], dict[str, dict[str, Any]]]:
    issues: list[ValidationIssue] = []
    profiles: dict[str, dict[str, Any]] = {}

    for name, frame in datasets.items():
        profiles[name] = profile_dataset(name, frame)

        if frame.empty:
            issues.append(ValidationIssue(dataset=name, severity="high", message="Dataset is empty"))
            continue

        numeric = frame.select_dtypes(include=["number"])
        for col in numeric.columns:
            null_fraction = float(frame[col].isna().mean())
            if null_fraction > 0.4:
                issues.append(
                    ValidationIssue(
                        dataset=name,
                        severity="medium",
                        message=f"High null fraction in {col}: {null_fraction:.2f}",
                    )
                )

    for check in config.quality.get("duplicate_key_checks", []):
        dataset_name = check["dataset"]
        resolved = _resolve_dataset_name(dataset_name, datasets)
        if resolved is None:
            LOGGER.debug("Duplicate check dataset missing: %s", dataset_name)
            continue

        keys = check["keys"]
        frame = datasets[resolved]
        missing_keys = [key for key in keys if key not in frame.columns]
        if missing_keys:
            issues.append(
                ValidationIssue(
                    dataset=resolved,
                    severity="medium",
                    message=f"Duplicate check missing keys: {', '.join(missing_keys)}",
                )
            )
            continue

        dup_count = int(frame.duplicated(subset=keys).sum())
        if dup_count > 0:
            issues.append(
                ValidationIssue(
                    dataset=resolved,
                    severity="high",
                    message=f"Found {dup_count} duplicate rows by keys {keys}",
                )
            )

    stale_days = int(config.quality.get("stale_days_default", 45))
    now = datetime.now(timezone.utc)
    for name, profile in profiles.items():
        max_date = profile.get("max_date")
        if not max_date:
            continue
        try:
            parsed = datetime.fromisoformat(max_date).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        age_days = (now - parsed).days
        if age_days > stale_days:
            issues.append(
                ValidationIssue(
                    dataset=name,
                    severity="low",
                    message=f"Data appears stale ({age_days} days old)",
                )
            )

    return issues, profiles


def issues_to_dicts(issues: list[ValidationIssue]) -> list[dict[str, str]]:
    return [asdict(issue) for issue in issues]
