"""Rate limiting and backoff utilities for API ingestion."""

import logging
import time
from typing import Any, Dict, Optional
import requests

from src.ingestion_pipeline.utils.logger import setup_logger

logger = setup_logger("rate_limiter")


class RateLimiter:
    """Monitors API rate limits and throttles requests to prevent threshold breaches."""

    def __init__(self, buffer_threshold: int = 50):
        """
        Args:
            buffer_threshold: Minimum remaining calls before pausing.
        """
        self.buffer_threshold = buffer_threshold

    def inspect_and_throttle(self, response: requests.Response) -> None:
        """Inspects response headers for rate limit information and throttles if needed.
        
        Supports standard HTTP rate limit headers (x-ratelimit-remaining, x-ratelimit-reset, retry-after).
        """
        headers = response.headers

        # Check for Retry-After header
        retry_after = headers.get("retry-after")
        if retry_after:
            wait_time = int(retry_after)
            logger.warning("Retry-After header received. Throttling execution for %s seconds.", wait_time)
            time.sleep(wait_time)
            return

        # Check for standard rate limit headers
        remaining_str = headers.get("x-ratelimit-remaining")
        reset_str = headers.get("x-ratelimit-reset")

        if remaining_str is not None and reset_str is not None:
            try:
                remaining = int(remaining_str)
                reset_timestamp = int(reset_str)
                now = int(time.time())

                if remaining < self.buffer_threshold:
                    sleep_duration = max(reset_timestamp - now, 1)
                    logger.warning(
                        "Rate limit buffer reached! Remaining points: %d, Threshold: %d. Sleeping %d seconds until reset.",
                        remaining,
                        self.buffer_threshold,
                        sleep_duration,
                    )
                    time.sleep(sleep_duration)
                elif remaining % 50 == 0 or remaining < 100:
                    logger.info("Current rate limit remaining: %d calls", remaining)
            except (ValueError, TypeError) as exc:
                logger.debug("Failed to parse rate limit headers: %s", exc)
