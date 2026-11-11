"""dlt Source definition for DummyJSON e-commerce data extraction."""

from typing import Any, Iterator, Optional
import dlt
from dlt.common.typing import TDataItem

from src.ingestion_pipeline.config import SourceConfig
from src.ingestion_pipeline.sources.dummyjson.client import DummyJSONClient
from src.ingestion_pipeline.utils.logger import setup_logger
from src.ingestion_pipeline.utils.rate_limiter import RateLimiter

logger = setup_logger("dummyjson_source")


@dlt.source(name="dummyjson")
def dummyjson_source(
    source_config: SourceConfig,
) -> Any:
    """dlt source yielding configured DummyJSON e-commerce entities.

    Entities:
      - products: Detailed product catalog with dimensions, reviews, and tags.
      - carts: E-commerce shopping carts / orders with nested item lines.
      - users: Customer accounts with profiles, addresses, and companies.
      - categories: Product categorization hierarchy.

    Args:
        source_config: SourceConfig containing endpoints, entity configurations, and limits.

    Returns:
        dlt source instance configured with enabled resources.
    """
    rate_limiter = RateLimiter(buffer_threshold=source_config.resilience.rate_limit_buffer)
    client = DummyJSONClient(
        base_url=source_config.base_url,
        rate_limiter=rate_limiter,
        max_retries=source_config.resilience.max_retries,
        initial_backoff=source_config.resilience.initial_backoff_seconds,
    )

    entities = source_config.entities

    # --------------------------------------------------------------------------
    # Resource: Products Catalog
    # --------------------------------------------------------------------------
    prod_cfg = entities.get("products")
    if prod_cfg and prod_cfg.enabled:
        @dlt.resource(
            name="products",
            write_disposition=prod_cfg.write_disposition,
            primary_key=prod_cfg.primary_key,
        )
        def products() -> Iterator[TDataItem]:
            logger.info(
                "Extracting products (limit=%d, max_items=%s)",
                prod_cfg.page_size,
                prod_cfg.max_items,
            )
            yield from client.paginate_endpoint(
                endpoint="products",
                entity_key="products",
                limit=prod_cfg.page_size,
                max_items=prod_cfg.max_items,
            )

        yield products

    # --------------------------------------------------------------------------
    # Resource: Carts / Orders
    # --------------------------------------------------------------------------
    cart_cfg = entities.get("carts")
    if cart_cfg and cart_cfg.enabled:
        @dlt.resource(
            name="carts",
            write_disposition=cart_cfg.write_disposition,
            primary_key=cart_cfg.primary_key,
        )
        def carts() -> Iterator[TDataItem]:
            logger.info(
                "Extracting carts (limit=%d, max_items=%s)",
                cart_cfg.page_size,
                cart_cfg.max_items,
            )
            yield from client.paginate_endpoint(
                endpoint="carts",
                entity_key="carts",
                limit=cart_cfg.page_size,
                max_items=cart_cfg.max_items,
            )

        yield carts

    # --------------------------------------------------------------------------
    # Resource: Users / Customers
    # --------------------------------------------------------------------------
    user_cfg = entities.get("users")
    if user_cfg and user_cfg.enabled:
        @dlt.resource(
            name="users",
            write_disposition=user_cfg.write_disposition,
            primary_key=user_cfg.primary_key,
        )
        def users() -> Iterator[TDataItem]:
            logger.info(
                "Extracting users (limit=%d, max_items=%s)",
                user_cfg.page_size,
                user_cfg.max_items,
            )
            yield from client.paginate_endpoint(
                endpoint="users",
                entity_key="users",
                limit=user_cfg.page_size,
                max_items=user_cfg.max_items,
            )

        yield users

    # --------------------------------------------------------------------------
    # Resource: Product Categories
    # --------------------------------------------------------------------------
    cat_cfg = entities.get("categories")
    if cat_cfg and cat_cfg.enabled:
        @dlt.resource(
            name="categories",
            write_disposition=cat_cfg.write_disposition,
            primary_key=cat_cfg.primary_key,
        )
        def categories() -> Iterator[TDataItem]:
            logger.info("Extracting product categories")
            yield from client.paginate_endpoint(
                endpoint="products/categories",
                entity_key="categories",
                limit=cat_cfg.page_size,
                max_items=cat_cfg.max_items,
            )

        yield categories

