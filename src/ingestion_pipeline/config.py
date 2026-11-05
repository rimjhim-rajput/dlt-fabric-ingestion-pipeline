"""Configuration loader and schema validator for the DummyJSON ingestion pipeline."""

from dataclasses import dataclass, field
import os
import re
from typing import Any, Dict, List, Optional
import yaml

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.ingestion_pipeline.utils.logger import setup_logger

logger = setup_logger("config")

# Regex to find ${VARIABLE_NAME} or $VARIABLE_NAME
ENV_VAR_PATTERN = re.compile(r"\$\{?([A-Za-z0-9_]+)\}?")


def substitute_env_vars(data: Any) -> Any:
    """Recursively substitutes environment variables inside strings, lists, and dicts."""
    if isinstance(data, dict):
        return {key: substitute_env_vars(val) for key, val in data.items()}
    elif isinstance(data, list):
        return [substitute_env_vars(item) for item in data]
    elif isinstance(data, str):
        def replace(match: re.Match) -> str:
            var_name = match.group(1)
            return os.getenv(var_name, "")
        return ENV_VAR_PATTERN.sub(replace, data)
    return data


@dataclass
class EntityConfig:
    enabled: bool = True
    write_disposition: str = "merge"
    primary_key: str = "id"
    page_size: int = 50
    max_items: Optional[int] = None


@dataclass
class ResilienceConfig:
    max_retries: int = 3
    initial_backoff_seconds: int = 2
    rate_limit_buffer: int = 50


@dataclass
class SourceConfig:
    type: str = "dummyjson"
    base_url: str = "https://dummyjson.com"
    entities: Dict[str, EntityConfig] = field(default_factory=dict)
    resilience: ResilienceConfig = field(default_factory=ResilienceConfig)


@dataclass
class DestinationConfig:
    destination_type: str  # duckdb, filesystem
    database_path: Optional[str] = None
    bucket_url: Optional[str] = None
    workspace_id: Optional[str] = None
    lakehouse_id: Optional[str] = None
    target_path: Optional[str] = None
    loader_file_format: Optional[str] = "parquet"
    credentials: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    name: str = "ecommerce_to_fabric_pipeline"
    dataset_name: str = "bronze_ecommerce"
    export_schema: bool = True
    schema_export_path: str = "schemas/export"


@dataclass
class AppConfig:
    active_environment: str
    pipeline: PipelineConfig
    source: SourceConfig
    destinations: Dict[str, DestinationConfig]

    @property
    def current_destination(self) -> DestinationConfig:
        if self.active_environment not in self.destinations:
            available = list(self.destinations.keys())
            raise ValueError(
                f"Active environment '{self.active_environment}' is not defined in destinations: {available}"
            )
        return self.destinations[self.active_environment]


def load_config(
    config_path: str = "config/pipeline_config.yaml",
    env_override: Optional[str] = None
) -> AppConfig:
    """Loads, interpolates environment variables, and parses the YAML configuration.

    Args:
        config_path: Path to the YAML configuration file.
        env_override: Optional environment name to override the YAML default.

    Returns:
        Structured and typed AppConfig.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f) or {}

    # Substitute environment variables throughout the config dictionary
    interpolated = substitute_env_vars(raw_config)

    # Determine active environment: CLI override > PIPELINE_ENV env var > YAML default
    active_env = (
        env_override
        or os.getenv("PIPELINE_ENV")
        or interpolated.get("active_environment", "local")
    )

    # Parse pipeline settings
    pipe_dict = interpolated.get("pipeline", {})
    pipeline_cfg = PipelineConfig(
        name=pipe_dict.get("name", "ecommerce_to_fabric_pipeline"),
        dataset_name=pipe_dict.get("dataset_name", "bronze_ecommerce"),
        export_schema=pipe_dict.get("export_schema", True),
        schema_export_path=pipe_dict.get("schema_export_path", "schemas/export"),
    )

    # Parse source settings
    source_dict = interpolated.get("source", {})

    entities = {}
    for entity_name, entity_vals in source_dict.get("entities", {}).items():
        entities[entity_name] = EntityConfig(
            enabled=entity_vals.get("enabled", True),
            write_disposition=entity_vals.get("write_disposition", "merge"),
            primary_key=entity_vals.get("primary_key", "id"),
            page_size=entity_vals.get("page_size", 50),
            max_items=entity_vals.get("max_items"),
        )

    resilience_dict = source_dict.get("resilience", {})
    resilience_cfg = ResilienceConfig(
        max_retries=resilience_dict.get("max_retries", 3),
        initial_backoff_seconds=resilience_dict.get("initial_backoff_seconds", 2),
        rate_limit_buffer=resilience_dict.get("rate_limit_buffer", 50),
    )

    source_cfg = SourceConfig(
        type=source_dict.get("type", "dummyjson"),
        base_url=source_dict.get("base_url", "https://dummyjson.com"),
        entities=entities,
        resilience=resilience_cfg,
    )

    # Parse destinations
    dest_dict = interpolated.get("destinations", {})
    destinations = {}
    for dest_key, dest_vals in dest_dict.items():
        destinations[dest_key] = DestinationConfig(
            destination_type=dest_vals.get("destination_type", "duckdb"),
            database_path=dest_vals.get("database_path"),
            bucket_url=dest_vals.get("bucket_url"),
            workspace_id=dest_vals.get("workspace_id"),
            lakehouse_id=dest_vals.get("lakehouse_id"),
            target_path=dest_vals.get("target_path"),
            loader_file_format=dest_vals.get("loader_file_format", "parquet"),
            credentials=dest_vals.get("credentials", {}),
        )

    config = AppConfig(
        active_environment=active_env,
        pipeline=pipeline_cfg,
        source=source_cfg,
        destinations=destinations,
    )

    logger.info(
        "Successfully loaded pipeline config. Active Environment: '%s', Dataset: '%s'",
        config.active_environment,
        config.pipeline.dataset_name,
    )
    return config
