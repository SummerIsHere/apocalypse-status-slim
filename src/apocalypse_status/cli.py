"""CLI entrypoint for the apocalypse status pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from apocalypse_status.config import DEFAULT_CONFIG_PATH, load_config
from apocalypse_status.logging_utils import configure_logging
from apocalypse_status.pipeline import (
    capture_baseline_metrics,
    run_all,
    run_fetch,
    run_report,
    run_transform,
    run_validate,
)


app = typer.Typer(help="Apocalypse Status data pipeline CLI")
report_app = typer.Typer(help="Generate report artifacts")
run_app = typer.Typer(help="Pipeline run commands")
app.add_typer(report_app, name="report")
app.add_typer(run_app, name="run")


@app.command()
def baseline(
    config_path: Path = typer.Option(DEFAULT_CONFIG_PATH, help="Path to config YAML"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    """Capture baseline parity metrics from existing CSV files."""
    configure_logging(verbose=verbose)
    config = load_config(config_path)
    output = capture_baseline_metrics(config)
    typer.echo(f"Baseline metrics written to {output}")


@app.command()
def fetch(
    source: str = typer.Option("all", help="Single source name or 'all'"),
    streamflow_approved_only: bool = typer.Option(
        False,
        "--streamflow-approved-only",
        help="Only include approved streamflow data (default includes provisional).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Force re-fetch even if source succeeded earlier today.",
    ),
    config_path: Path = typer.Option(DEFAULT_CONFIG_PATH, help="Path to config YAML"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    """Fetch source data into downloaded_data/raw."""
    configure_logging(verbose=verbose)
    config = load_config(config_path)
    results = run_fetch(
        config,
        source=source,
        streamflow_approved_only=streamflow_approved_only,
        force=force,
    )

    for result in results:
        status = "ok" if result.success else "failed"
        typer.echo(f"[{status}] {result.source} raw_files={len(result.raw_files)}")
        if result.warning:
            typer.echo(f"  warning: {result.warning}")


@app.command()
def transform(
    dataset: str = typer.Option("all", help="Dataset scope (currently all datasets are transformed)."),
    streamflow_approved_only: bool = typer.Option(
        False,
        "--streamflow-approved-only",
        help="Force approved-only streamflow output.",
    ),
    config_path: Path = typer.Option(DEFAULT_CONFIG_PATH, help="Path to config YAML"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    """Build tidy and modeled datasets from raw/legacy inputs."""
    configure_logging(verbose=verbose)
    config = load_config(config_path)
    result = run_transform(config, streamflow_approved_only=streamflow_approved_only)
    typer.echo(f"Transformed {len(result.datasets)} datasets (scope={dataset})")


@app.command()
def validate(
    config_path: Path = typer.Option(DEFAULT_CONFIG_PATH, help="Path to config YAML"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    """Validate transformed datasets and print issue summary."""
    configure_logging(verbose=verbose)
    config = load_config(config_path)
    transformed = run_transform(config)
    issues, profiles = run_validate(config, transformed.datasets)

    if not issues:
        typer.echo("No validation issues detected.")
    else:
        typer.echo(f"Validation issues: {len(issues)}")
        for issue in issues:
            typer.echo(f"- [{issue['severity']}] {issue['dataset']}: {issue['message']}")

    typer.echo(json.dumps(profiles, indent=2))


@report_app.command("html")
def report_html(
    config_path: Path = typer.Option(DEFAULT_CONFIG_PATH, help="Path to config YAML"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    """Generate the offline HTML dashboard artifact."""
    configure_logging(verbose=verbose)
    config = load_config(config_path)
    transformed = run_transform(config)
    output = run_report(config, transformed.datasets)
    typer.echo(f"Wrote dashboard to {output}")


@run_app.command("all")
def run_everything(
    source: str = typer.Option("all", help="Single source name or 'all'"),
    streamflow_approved_only: bool = typer.Option(
        False,
        "--streamflow-approved-only",
        help="Only include approved streamflow data.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Force re-fetch even if source succeeded earlier today.",
    ),
    config_path: Path = typer.Option(DEFAULT_CONFIG_PATH, help="Path to config YAML"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    """Fetch, transform, validate, and render the HTML dashboard."""
    configure_logging(verbose=verbose)
    config = load_config(config_path)
    source_results, transformed, dashboard_path, manifest_path = run_all(
        config,
        source=source,
        streamflow_approved_only=streamflow_approved_only,
        force=force,
    )

    failures = [result for result in source_results if not result.success]
    typer.echo(f"Sources processed: {len(source_results)} (failures={len(failures)})")
    typer.echo(f"Datasets transformed: {len(transformed.datasets)}")
    typer.echo(f"Dashboard: {dashboard_path}")
    typer.echo(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    app()
