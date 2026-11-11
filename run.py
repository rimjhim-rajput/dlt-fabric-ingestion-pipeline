#!/usr/bin/env python3
"""CLI Entry point for the DummyJSON to Microsoft Fabric ingestion pipeline."""

import argparse
import sys
from src.ingestion_pipeline.pipeline import run_ingestion
from src.ingestion_pipeline.utils.logger import setup_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run dlt data ingestion from DummyJSON API to Microsoft Fabric OneLake or local storage."
    )
    parser.add_argument(
        "--env",
        "-e",
        type=str,
        default=None,
        help="Target environment override: 'local', 'local_filesystem', or 'fabric' (default: from config).",
    )
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="config/pipeline_config.yaml",
        help="Path to pipeline YAML configuration file (default: config/pipeline_config.yaml).",
    )
    parser.add_argument(
        "--entities",
        type=str,
        default=None,
        help="Comma-separated list of entities to extract, e.g. 'products,carts,users,categories'.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test API connectivity and extract a sample of records without loading into destination.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Set console logging verbosity.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger = setup_logger("cli", log_level=args.log_level)

    selected_entities = None
    if args.entities:
        selected_entities = [e.strip() for e in args.entities.split(",") if e.strip()]

    try:
        run_ingestion(
            config_path=args.config,
            env_override=args.env,
            selected_entities=selected_entities,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        logger.error("Pipeline run failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
