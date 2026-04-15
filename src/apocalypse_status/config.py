"""Configuration helpers for the apocalypse status pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class AppPaths:
    data_root: Path
    raw_dir: Path
    tidy_dir: Path
    artifacts_dir: Path


@dataclass(frozen=True)
class AppConfig:
    paths: AppPaths
    sources: dict[str, Any]
    quality: dict[str, Any]
    report: dict[str, Any]


DEFAULT_CONFIG_PATH = Path("config/sources.yaml")


def _expand_path(raw: str, base_dir: Path) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    config_path = config_path.resolve()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    base_dir = config_path.parent.parent
    path_cfg = raw["paths"]
    paths = AppPaths(
        data_root=_expand_path(path_cfg["data_root"], base_dir),
        raw_dir=_expand_path(path_cfg["raw_dir"], base_dir),
        tidy_dir=_expand_path(path_cfg["tidy_dir"], base_dir),
        artifacts_dir=_expand_path(path_cfg["artifacts_dir"], base_dir),
    )
    return AppConfig(
        paths=paths,
        sources=raw.get("sources", {}),
        quality=raw.get("quality", {}),
        report=raw.get("report", {}),
    )


def ensure_output_dirs(config: AppConfig) -> None:
    config.paths.data_root.mkdir(parents=True, exist_ok=True)
    config.paths.raw_dir.mkdir(parents=True, exist_ok=True)
    config.paths.tidy_dir.mkdir(parents=True, exist_ok=True)
    config.paths.artifacts_dir.mkdir(parents=True, exist_ok=True)
