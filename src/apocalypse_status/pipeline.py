"""Pipeline orchestration."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from apocalypse_status.config import AppConfig, ensure_output_dirs
from apocalypse_status.extract.sources import fetch_all_sources
from apocalypse_status.quality.validation import issues_to_dicts, validate_datasets
from apocalypse_status.report.html import generate_dashboard_html
from apocalypse_status.transform.models import TransformResult, transform_all_datasets
from apocalypse_status.types import RunManifest, SourceResult


def _result_to_dict(result: SourceResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["raw_files"] = [str(path) for path in result.raw_files]
    payload["tidy_files"] = [str(path) for path in result.tidy_files]
    return payload


def write_manifest(manifest: RunManifest, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(manifest), indent=2), encoding="utf-8")
    return output_path


def capture_baseline_metrics(config: AppConfig) -> Path:
    baseline: dict[str, Any] = {}
    for csv_file in sorted(config.paths.data_root.glob("**/*.csv")):
        try:
            frame = pd.read_csv(csv_file)
        except Exception:  # noqa: BLE001
            continue

        profile: dict[str, Any] = {
            "rows": int(len(frame.index)),
            "columns": list(frame.columns),
        }

        date_cols = [col for col in frame.columns if "date" in col.lower()]
        if date_cols:
            parsed = pd.to_datetime(frame[date_cols[0]], errors="coerce")
            if parsed.notna().any():
                profile["min_date"] = str(parsed.min().date())
                profile["max_date"] = str(parsed.max().date())

        baseline[str(csv_file.relative_to(config.paths.data_root))] = profile

    output = config.paths.artifacts_dir / "baseline_metrics.json"
    output.write_text(json.dumps(baseline, indent=2), encoding="utf-8")
    return output


def run_fetch(
    config: AppConfig,
    source: str | None = None,
    streamflow_approved_only: bool = False,
    force: bool = False,
) -> list[SourceResult]:
    ensure_output_dirs(config)
    selected = None if source in (None, "all") else [source]
    return fetch_all_sources(
        config,
        selected_sources=selected,
        streamflow_approved_only=streamflow_approved_only,
        force=force,
    )


def run_transform(
    config: AppConfig,
    streamflow_approved_only: bool = False,
) -> TransformResult:
    ensure_output_dirs(config)
    return transform_all_datasets(config, streamflow_approved_only=streamflow_approved_only)


def run_validate(config: AppConfig, datasets: dict[str, pd.DataFrame]) -> tuple[list[dict[str, str]], dict[str, dict[str, Any]]]:
    issues, profiles = validate_datasets(config, datasets)
    return issues_to_dicts(issues), profiles


def run_report(config: AppConfig, datasets: dict[str, pd.DataFrame]) -> Path:
    ensure_output_dirs(config)
    output = config.paths.artifacts_dir / "apocalypse_status_dashboard.html"
    return generate_dashboard_html(config, datasets, output)


def run_all(
    config: AppConfig,
    source: str | None = None,
    streamflow_approved_only: bool = False,
    force: bool = False,
) -> tuple[list[SourceResult], TransformResult, Path, Path]:
    ensure_output_dirs(config)

    source_results = run_fetch(
        config,
        source=source,
        streamflow_approved_only=streamflow_approved_only,
        force=force,
    )
    transform_result = run_transform(config, streamflow_approved_only=streamflow_approved_only)
    issues, profiles = run_validate(config, transform_result.datasets)
    dashboard_path = run_report(config, transform_result.datasets)

    manifest = RunManifest(
        generated_at=datetime.now(timezone.utc).isoformat(),
        source_results=[_result_to_dict(result) for result in source_results],
        dataset_profiles=profiles,
        issues=issues,
        outputs={
            "dashboard_html": str(dashboard_path),
            "tidy_dir": str(config.paths.tidy_dir),
        },
    )
    manifest_path = write_manifest(manifest, config.paths.artifacts_dir / "run_manifest.json")

    return source_results, transform_result, dashboard_path, manifest_path
