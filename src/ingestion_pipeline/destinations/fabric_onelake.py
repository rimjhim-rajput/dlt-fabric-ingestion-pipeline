"""Destination factory resolving Microsoft Fabric OneLake and local test destinations."""

import os
from typing import Any, Tuple
import dlt
from dlt.destinations import filesystem, duckdb

from src.ingestion_pipeline.config import DestinationConfig
from src.ingestion_pipeline.utils.logger import setup_logger

logger = setup_logger("destination_factory")


def build_onelake_abfss_url(workspace_id: str, lakehouse_id: str, target_path: str = "Files/bronze/ecommerce") -> str:
    """Builds the Microsoft Fabric OneLake ABFSS URL.

    Pattern:
      abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>.Lakehouse/<target_path>

    Args:
        workspace_id: Azure Fabric Workspace GUID or name.
        lakehouse_id: Azure Fabric Lakehouse GUID or Lakehouse name.
        target_path: Path inside the Lakehouse (e.g. 'Files/bronze/ecommerce' or 'Tables/bronze_ecommerce').

    Returns:
        Formatted ABFSS URI string.
    """
    clean_target = (target_path or "Files/bronze/ecommerce").strip("/")
    lakehouse_ref = lakehouse_id if lakehouse_id.endswith(".Lakehouse") else f"{lakehouse_id}.Lakehouse"
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_ref}/{clean_target}"


def resolve_destination(destination_config: DestinationConfig) -> Tuple[Any, dict]:
    """Resolves and configures the appropriate dlt destination based on configuration.

    Supports:
      1. 'fabric' (filesystem destination targeted at Microsoft Fabric OneLake)
         - Supports Free Trial / Organizational Accounts via Azure CLI (az login)
         - Supports Enterprise Service Principals (Client ID + Client Secret)
      2. 'duckdb' (embedded local database for instant development & testing)
      3. 'filesystem' (local directory landing mimicking OneLake folder structure)

    Args:
        destination_config: Destination configuration dataclass.

    Returns:
        Tuple of (dlt destination instance, extra pipeline kwargs).
    """
    dest_type = destination_config.destination_type.lower()

    if dest_type == "duckdb":
        db_path = destination_config.database_path or ".data/ecommerce_local.duckdb"
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        logger.info("Using DuckDB destination at: %s", db_path)
        return duckdb(credentials=db_path), {}

    elif dest_type == "filesystem":
        # Determine if targeting Microsoft Fabric OneLake
        is_onelake = (
            (destination_config.bucket_url and destination_config.bucket_url.startswith("abfss://"))
            or (destination_config.workspace_id and destination_config.lakehouse_id)
        )

        if is_onelake:
            # Resolve target bucket URL
            if destination_config.bucket_url and destination_config.bucket_url.startswith("abfss://"):
                bucket_url = destination_config.bucket_url.rstrip("/")
                if destination_config.target_path and not bucket_url.endswith(destination_config.target_path.strip("/")):
                    if "Files" not in bucket_url and "Tables" not in bucket_url:
                        bucket_url = f"{bucket_url}/{destination_config.target_path.strip('/')}"
            else:
                bucket_url = build_onelake_abfss_url(
                    workspace_id=destination_config.workspace_id,
                    lakehouse_id=destination_config.lakehouse_id,
                    target_path=destination_config.target_path or "Files/bronze/ecommerce",
                )

            creds = destination_config.credentials or {}
            client_id = creds.get("azure_client_id") or os.getenv("AZURE_CLIENT_ID")
            client_secret = creds.get("azure_client_secret") or os.getenv("AZURE_CLIENT_SECRET")
            tenant_id = creds.get("azure_tenant_id") or os.getenv("AZURE_TENANT_ID")

            def _is_valid_value(v: Any) -> bool:
                if not v or not isinstance(v, str):
                    return False
                v = v.strip()
                return bool(v and not v.startswith("${"))

            # Check if Service Principal is configured (Enterprise)
            if _is_valid_value(client_id) and _is_valid_value(client_secret) and _is_valid_value(tenant_id):
                logger.info("Fabric OneLake: Authenticating using Service Principal (App Registration).")
                azure_creds = {
                    "azure_storage_account_name": "onelake",
                    "azure_account_host": "onelake.blob.fabric.microsoft.com",
                    "azure_tenant_id": tenant_id,
                    "azure_client_id": client_id,
                    "azure_client_secret": client_secret,
                }
            else:
                # Clean up empty strings from environment to prevent azure.identity from attempting ClientSecretCredential
                for env_key in ("AZURE_CLIENT_SECRET", "AZURE_CLIENT_ID", "AZURE_TENANT_ID"):
                    if env_key in os.environ and not os.environ[env_key].strip():
                        del os.environ[env_key]

                # Free Trial / Organizational Account: Uses AzureCliCredential / DefaultAzureCredential (az login)
                logger.info(
                    "Fabric OneLake: Authenticating with Organizational Account via Azure CLI (az login)."
                )
                try:
                    from azure.identity import AzureCliCredential, DefaultAzureCredential
                    from dlt.common.configuration.specs.azure_credentials import AzureCredentials
                    
                    # Explicitly use AzureCliCredential from az login session
                    try:
                        credential = AzureCliCredential()
                    except Exception:
                        credential = DefaultAzureCredential(exclude_environment_credential=True)

                    azure_creds = AzureCredentials.from_credential(credential)
                    azure_creds.azure_storage_account_name = "onelake"
                    azure_creds.azure_account_host = "onelake.blob.fabric.microsoft.com"
                except Exception as ex:
                    logger.warning("Failed to initialize AzureCredentials wrapper: %s. Falling back to default dict.", ex)
                    azure_creds = {
                        "azure_storage_account_name": "onelake",
                        "azure_account_host": "onelake.blob.fabric.microsoft.com",
                    }

            logger.info("Targeting Microsoft Fabric OneLake at: %s", bucket_url)
            dest = filesystem(bucket_url=bucket_url, credentials=azure_creds)
            extra_kwargs = {}
            if destination_config.loader_file_format:
                extra_kwargs["loader_file_format"] = destination_config.loader_file_format
            return dest, extra_kwargs

        # Otherwise local filesystem simulation
        bucket_url = destination_config.bucket_url or ".data/onelake_simulation/Files/bronze/ecommerce"
        os.makedirs(os.path.abspath(bucket_url.replace("file://", "")), exist_ok=True)
        logger.info("Using local filesystem simulation destination: %s", bucket_url)
        dest = filesystem(bucket_url=bucket_url)
        extra_kwargs = {}
        if destination_config.loader_file_format:
            extra_kwargs["loader_file_format"] = destination_config.loader_file_format
        return dest, extra_kwargs

    else:
        raise ValueError(f"Unsupported destination type: '{dest_type}'")
