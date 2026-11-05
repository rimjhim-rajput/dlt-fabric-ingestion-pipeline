"""Structured logging setup for ingestion pipelines."""

import logging
import os
import sys
from typing import Optional


def setup_logger(
    name: str = "ingestion_pipeline",
    log_level: Optional[str] = None
) -> logging.Logger:
    """Configures and returns a structured logger with standardized formatting.
    
    Args:
        name: Name of the logger.
        log_level: Log level override (DEBUG, INFO, WARNING, ERROR). Defaults to
            LOG_LEVEL env var or INFO.
    
    Returns:
        Configured logging.Logger instance.
    """
    level_name = log_level or os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers if logger was already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

