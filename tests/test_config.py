"""Unit tests for configuration loading and environment substitution."""

import os
import pytest
from src.ingestion_pipeline.config import (
    substitute_env_vars,
    load_config,
    AppConfig,
)


def test_substitute_env_vars(monkeypatch):
    monkeypatch.setenv("TEST_VAR", "injected_value")
    data = {
        "key1": "${TEST_VAR}",
        "nested": {
            "key2": "prefix_${TEST_VAR}_suffix",
            "list": ["item1", "${TEST_VAR}"],
        },
    }
    result = substitute_env_vars(data)
    assert result["key1"] == "injected_value"
    assert result["nested"]["key2"] == "prefix_injected_value_suffix"
    assert result["nested"]["list"][1] == "injected_value"


def test_load_config_local_default():
    config = load_config("config/pipeline_config.yaml")
    assert isinstance(config, AppConfig)
    assert config.active_environment == "local"
    assert config.pipeline.name == "ecommerce_to_fabric_pipeline"
    assert "products" in config.source.entities
    assert "carts" in config.source.entities
    assert "users" in config.source.entities
    assert "categories" in config.source.entities


def test_load_config_env_override():
    config = load_config("config/pipeline_config.yaml", env_override="fabric")
    assert config.active_environment == "fabric"
    assert config.current_destination.destination_type == "filesystem"
