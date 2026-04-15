"""Shared data types."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class SourceResult:
    source: str
    success: bool
    raw_files: list[Path] = field(default_factory=list)
    tidy_files: list[Path] = field(default_factory=list)
    warning: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationIssue:
    dataset: str
    severity: str
    message: str


@dataclass
class RunManifest:
    generated_at: str
    source_results: list[dict[str, Any]]
    dataset_profiles: dict[str, dict[str, Any]]
    issues: list[dict[str, str]]
    outputs: dict[str, str]
