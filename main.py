#! python3
"""Legacy compatibility shim.

This entrypoint keeps old workflows working while delegating to the new CLI pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from apocalypse_status.config import DEFAULT_CONFIG_PATH, load_config
from apocalypse_status.logging_utils import configure_logging
from apocalypse_status.pipeline import run_all


def main() -> None:
    configure_logging(log_path=REPO_ROOT / "main_logging.txt", verbose=False)
    config = load_config(DEFAULT_CONFIG_PATH)
    _, _, dashboard_path, manifest_path = run_all(config, source="all", streamflow_approved_only=False)
    print(f"Pipeline complete. Dashboard: {dashboard_path}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
