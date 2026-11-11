"""Unit tests for DummyJSON API client and offset-based pagination."""

from unittest.mock import MagicMock, patch
import pytest
from src.ingestion_pipeline.sources.dummyjson.client import (
    DummyJSONClient,
    APIClientError,
)
from src.ingestion_pipeline.utils.rate_limiter import RateLimiter


def test_get_request_success():
    client = DummyJSONClient(base_url="https://dummyjson.com")
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "products": [
            {"id": 1, "title": "Essence Mascara Lash Princess", "price": 9.99}
        ],
        "total": 1,
        "skip": 0,
        "limit": 30,
    }
    mock_response.headers = {}
    mock_response.status_code = 200

    with patch.object(client.session, "get", return_value=mock_response):
        data = client.get("products")
        assert len(data["products"]) == 1
        assert data["products"][0]["title"] == "Essence Mascara Lash Princess"


def test_paginate_endpoint_multiple_pages():
    """Tests multi-page traversal using limit and skip offset pagination."""
    client = DummyJSONClient(base_url="https://dummyjson.com")

    # Simulate 2 pages of products with total=3
    page1 = {
        "products": [
            {"id": 1, "title": "Product 1"},
            {"id": 2, "title": "Product 2"},
        ],
        "total": 3,
        "skip": 0,
        "limit": 2,
    }
    page2 = {
        "products": [
            {"id": 3, "title": "Product 3"},
        ],
        "total": 3,
        "skip": 2,
        "limit": 2,
    }

    mock_get = MagicMock(side_effect=[page1, page2])
    with patch.object(client, "get", mock_get):
        generator = client.paginate_endpoint(
            endpoint="products",
            entity_key="products",
            limit=2,
        )

        items = list(generator)
        assert len(items) == 3
        assert items[0]["id"] == 1
        assert items[1]["id"] == 2
        assert items[2]["id"] == 3

        # Verify second call passed skip=2
        assert mock_get.call_count == 2
        call_2_params = mock_get.call_args_list[1][1]["params"]
        assert call_2_params["skip"] == 2
        assert call_2_params["limit"] == 2


def test_paginate_endpoint_with_max_items():
    client = DummyJSONClient(base_url="https://dummyjson.com")

    page1 = {
        "products": [
            {"id": 1, "title": "Product 1"},
            {"id": 2, "title": "Product 2"},
            {"id": 3, "title": "Product 3"},
        ],
        "total": 100,
        "skip": 0,
        "limit": 3,
    }

    with patch.object(client, "get", return_value=page1):
        generator = client.paginate_endpoint(
            endpoint="products",
            entity_key="products",
            limit=3,
            max_items=2,
        )
        items = list(generator)
        assert len(items) == 2
        assert items[0]["id"] == 1
        assert items[1]["id"] == 2


def test_rate_limiter_parsing():
    rate_limiter = RateLimiter(buffer_threshold=10)
    mock_response = MagicMock()
    mock_response.headers = {
        "x-ratelimit-remaining": "5000",
        "x-ratelimit-reset": "1700000000",
    }
    # Should not raise or block
    rate_limiter.inspect_and_throttle(mock_response)

