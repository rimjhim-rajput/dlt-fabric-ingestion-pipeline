"""Unit tests for destination resolving and Microsoft Fabric OneLake URLs."""

from src.ingestion_pipeline.config import DestinationConfig
from src.ingestion_pipeline.destinations.fabric_onelake import (
    build_onelake_abfss_url,
    resolve_destination,
)


def test_build_onelake_abfss_url():
    url = build_onelake_abfss_url(
        workspace_id="11111111-2222-3333-4444-555555555555",
        lakehouse_id="aaaa-bbbb-cccc-dddd",
        target_path="Files/bronze/ecommerce",
    )
    expected = (
        "abfss://11111111-2222-3333-4444-555555555555@onelake.dfs.fabric.microsoft.com/"
        "aaaa-bbbb-cccc-dddd.Lakehouse/Files/bronze/ecommerce"
    )
    assert url == expected


def test_resolve_duckdb_destination():
    cfg = DestinationConfig(
        destination_type="duckdb",
        database_path=".data/test_lakehouse.duckdb",
    )
    dest, extra = resolve_destination(cfg)
    assert dest is not None
    assert extra == {}


def test_resolve_fabric_direct_abfs_url():
    cfg = DestinationConfig(
        destination_type="filesystem",
        bucket_url="abfss://my_workspace@onelake.dfs.fabric.microsoft.com/my_lakehouse.Lakehouse/Files/bronze/ecommerce",
        target_path="Files/bronze/ecommerce",
    )
    dest, extra = resolve_destination(cfg)
    assert dest is not None
