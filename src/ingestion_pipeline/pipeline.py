"""Main pipeline execution orchestrator."""

from datetime import datetime
import json
import os
from typing import Any, List, Optional
import dlt

from src.ingestion_pipeline.config import load_config, AppConfig
from src.ingestion_pipeline.destinations.fabric_onelake import resolve_destination
from src.ingestion_pipeline.sources.dummyjson import dummyjson_source
from src.ingestion_pipeline.utils.logger import setup_logger

logger = setup_logger("pipeline_orchestrator")


def run_ingestion(
    config_path: str = "config/pipeline_config.yaml",
    env_override: Optional[str] = None,
    selected_entities: Optional[List[str]] = None,
    dry_run: bool = False,
) -> Any:
    """Orchestrates configuration loading, source extraction, and destination loading.

    Flow:
      Config (YAML + .env) -> DummyJSON API Source -> dlt Pipeline -> Microsoft Fabric OneLake (or local)

    Args:
        config_path: Path to YAML pipeline configuration file.
        env_override: Environment name ('local', 'fabric', 'local_filesystem').
        selected_entities: Optional list of entities to extract (overriding YAML).
        dry_run: If True, fetches a small sample of items to verify extraction without loading.

    Returns:
        dlt LoadInfo object or dry run summary dict.
    """
    start_time = datetime.now()
    logger.info("Initializing ingestion pipeline run at %s", start_time.isoformat())

    # 1. Load config
    config: AppConfig = load_config(config_path=config_path, env_override=env_override)

    # 2. Filter entities if overridden
    if selected_entities:
        logger.info("Overriding enabled entities with: %s", selected_entities)
        for entity_name, entity_cfg in config.source.entities.items():
            entity_cfg.enabled = entity_name in selected_entities

    # 3. Instantiate Source
    if config.source.type == "dummyjson":
        source = dummyjson_source(source_config=config.source)
    else:
        raise ValueError(f"Unknown source type: {config.source.type}")

    # 4. Handle Dry Run
    if dry_run:
        logger.info("DRY RUN MODE: Testing extraction without loading into destination...")
        summary = {}
        for resource in source.selected_resources.values():
            logger.info("Sampling resource: %s", resource.name)
            sample_items = []
            for item in resource:
                sample_items.append(item)
                if len(sample_items) >= 2:
                    break
            summary[resource.name] = {
                "sample_count": len(sample_items),
                "sample_record_keys": list(sample_items[0].keys()) if sample_items else [],
            }
            if sample_items:
                logger.info(
                    "Sample record from '%s':\n%s",
                    resource.name,
                    json.dumps(sample_items[0], indent=2, default=str),
                )
        logger.info("Dry run complete. Extraction verified successfully.")
        return summary

    # 5. Resolve Destination
    dest_instance, extra_kwargs = resolve_destination(config.current_destination)

    # 6. Initialize dlt Pipeline
    pipeline_kwargs = {
        "pipeline_name": config.pipeline.name,
        "destination": dest_instance,
        "dataset_name": config.pipeline.dataset_name,
    }
    if config.pipeline.export_schema:
        pipeline_kwargs["export_schema_path"] = config.pipeline.schema_export_path

    pipeline = dlt.pipeline(**pipeline_kwargs)

    # 7. Execute Ingestion Run
    logger.info(
        "Starting dlt pipeline run: '%s' -> Destination: '%s'",
        config.pipeline.name,
        config.active_environment,
    )
    load_info = pipeline.run(source, **extra_kwargs)

    elapsed = datetime.now() - start_time
    logger.info("Ingestion pipeline finished in %s", str(elapsed))
    logger.info("dlt Load Summary:\n%s", str(load_info))

    return load_info
