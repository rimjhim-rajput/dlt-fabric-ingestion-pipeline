"""DummyJSON REST API client handling pagination, retries, and rate limiting."""

import logging
from typing import Any, Dict, Iterator, List, Optional
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from src.ingestion_pipeline.utils.logger import setup_logger
from src.ingestion_pipeline.utils.rate_limiter import RateLimiter

logger = setup_logger("dummyjson_client")


class APIClientError(Exception):
    """Exception raised for API errors."""
    pass


class DummyJSONClient:
    """Client for interacting with DummyJSON e-commerce API endpoints."""

    def __init__(
        self,
        base_url: str = "https://dummyjson.com",
        rate_limiter: Optional[RateLimiter] = None,
        max_retries: int = 3,
        initial_backoff: int = 2,
    ):
        """
        Args:
            base_url: Base URL for DummyJSON API.
            rate_limiter: RateLimiter instance to monitor API usage.
            max_retries: Maximum retry attempts for transient network errors.
            initial_backoff: Exponential backoff base in seconds.
        """
        self.base_url = base_url.rstrip("/")
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "dlt-fabric-ingestion-pipeline/0.1.0",
        })

        self._retry_decorator = retry(
            reraise=True,
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=initial_backoff, min=1, max=30),
            retry=retry_if_exception_type((requests.exceptions.RequestException,)),
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Executes a GET request against the API with retries and rate limit checks."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        def _request() -> requests.Response:
            response = self.session.get(url, params=params or {}, timeout=30)
            self.rate_limiter.inspect_and_throttle(response)
            response.raise_for_status()
            return response

        response = self._retry_decorator(_request)()
        return response.json()

    def paginate_endpoint(
        self,
        endpoint: str,
        entity_key: str,
        limit: int = 50,
        max_items: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Paginates through an endpoint using offset-based pagination (limit & skip).

        Args:
            endpoint: API endpoint path (e.g. 'products', 'carts', 'users').
            entity_key: JSON key holding the list of records (e.g. 'products', 'carts', 'users').
            limit: Number of items per batch.
            max_items: Optional maximum total items to retrieve.

        Yields:
            Individual item dictionaries.
        """
        skip = 0
        total_retrieved = 0
        has_more = True

        while has_more:
            current_limit = limit
            if max_items is not None:
                remaining = max_items - total_retrieved
                if remaining <= 0:
                    break
                current_limit = min(limit, remaining)

            params = {"limit": current_limit, "skip": skip}
            logger.debug("Fetching %s with params %s", endpoint, params)

            data = self.get(endpoint, params=params)

            if isinstance(data, list):
                # Endpoint returns a flat list (e.g. categories)
                for item in data:
                    yield item if isinstance(item, dict) else {"name": item}
                    total_retrieved += 1
                    if max_items is not None and total_retrieved >= max_items:
                        break
                break

            records: List[Dict[str, Any]] = data.get(entity_key, [])
            total = data.get("total", 0)

            if not records:
                logger.debug("No records returned for %s at skip=%d. Terminating loop.", endpoint, skip)
                break

            for record in records:
                yield record
                total_retrieved += 1
                if max_items is not None and total_retrieved >= max_items:
                    has_more = False
                    break

            skip += len(records)
            if skip >= total or not has_more:
                has_more = False
                logger.info(
                    "Completed extraction for '%s'. Total retrieved: %d / %d records.",
                    endpoint,
                    total_retrieved,
                    total,
                )

