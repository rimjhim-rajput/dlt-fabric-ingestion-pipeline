"""Destinations package."""

from src.ingestion_pipeline.destinations.fabric_onelake import (
    resolve_destination,
    build_onelake_abfss_url,
)

__all__ = ["resolve_destination", "build_onelake_abfss_url"]

