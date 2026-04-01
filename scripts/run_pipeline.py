"""Entry point script to run the full trade blotter pipeline."""

import argparse
import sys
from pathlib import Path

# Allow running as `python scripts/run_pipeline.py` without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from trade_blotter.pipeline import run


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the trade blotter pipeline.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent.parent / "config" / "pipeline.yaml",
        help="Path to pipeline.yaml (default: config/pipeline.yaml)",
    )
    args = parser.parse_args()
    run(config_path=args.config)


if __name__ == "__main__":
    main()
