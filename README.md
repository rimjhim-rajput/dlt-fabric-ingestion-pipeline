# DummyJSON to Microsoft Fabric OneLake Ingestion Pipeline (dlt)

A production-grade, config-driven data ingestion pipeline built with **dlt (data load tool)**. It extracts realistic e-commerce data (`products`, `carts`, `users`, `categories`) from the **DummyJSON REST API** and lands it directly into **Microsoft Fabric OneLake** (or local DuckDB/filesystem for local development).

This pipeline serves as an end-to-end reference architecture for extracting e-commerce and retail APIs, handling schema normalization (nested line items, reviews, customer addresses), and loading data into Microsoft Fabric Lakehouse Delta/Parquet tables for downstream **dbt** transformations.

---

## 🏛️ Architecture Overview

```
                      ┌────────────────────────────────────────┐
                      │        config/pipeline_config.yaml     │
                      │       + .env / .dlt/secrets.toml       │
                      └──────────────────┬─────────────────────┘
                                         │
                                         ▼
┌───────────────────────┐      ┌──────────────────┐      ┌───────────────────────────────┐
│   DummyJSON REST API  │ ───► │  dlt Ingestion   │ ───► │      Microsoft Fabric         │
│                       │      │    Pipeline      │      │                               │
│ • Products & Reviews  │      │ • Auto-schema    │      │ • OneLake ABFSS Endpoint      │
│ • Carts & Line Items  │      │ • Normalization  │      │ • Parquet / Delta Lake tables │
│ • Customer Profiles   │      │ • Merge / Replace│      │ • Staging layer for dbt       │
│ • Limit/Skip Paging   │      │ • Micro-batching │      │ • Lakehouse / Warehouse       │
└───────────────────────┘      └──────────────────┘      └───────────────────────────────┘
```

---

## 📁 Project Structure

```
dlt-fabric-ingestion-pipeline/
├── .dlt/
│   ├── config.toml.example          # dlt engine runtime settings template
│   └── secrets.toml.example         # dlt secrets template (alternative to .env)
├── .env.example                     # Environment variables & credentials template
├── .gitignore                       # Production gitignore (keeps secrets & data local)
├── pyproject.toml                   # Project packaging, metadata, & dependencies
├── requirements.txt                 # Core dependencies
├── run.py                           # CLI entry point for executing the pipeline
│
├── config/
│   ├── __init__.py
│   └── pipeline_config.yaml         # Config-driven pipeline definitions (entities, batch sizes)
│
├── src/
│   └── ingestion_pipeline/
│       ├── __init__.py
│       ├── config.py                # Typed config parser & environment variable resolver
│       ├── pipeline.py              # Main orchestrator (source -> dlt -> destination)
│       │
│       ├── utils/
│       │   ├── __init__.py
│       │   ├── logger.py            # Standard structured logger
│       │   └── rate_limiter.py      # Resilient rate-limit monitoring & exponential backoff
│       │
│       ├── sources/
│       │   ├── __init__.py
│       │   └── dummyjson/           # Production DummyJSON API connector
│       │       ├── __init__.py
│       │       ├── client.py        # HTTP client with limit/skip pagination & retries
│       │       └── source.py        # @dlt.source and @dlt.resource definitions
│       │
│       └── destinations/
│           ├── __init__.py
│           └── fabric_onelake.py    # Fabric OneLake ABFSS resolver & local backends
│
├── tests/
│   ├── __init__.py
│   ├── test_config.py               # Config interpolation & loader tests
│   ├── test_destination.py          # Fabric OneLake URL builder tests
│   └── test_dummyjson_client.py     # Pagination & API client unit tests
│
└── docs/
    └── fabric_dbt_guide.md          # Guide for downstream Fabric Lakehouse & dbt models
```

---

## ⚙️ Configuration-Driven Design

Every aspect of the extraction and loading is controlled through `config/pipeline_config.yaml`:

```yaml
active_environment: "local"          # 'local', 'local_filesystem', or 'fabric'

pipeline:
  name: "ecommerce_to_fabric_pipeline"
  dataset_name: "bronze_ecommerce"

source:
  type: "dummyjson"
  base_url: "https://dummyjson.com"

  entities:
    products:
      enabled: true
      write_disposition: "merge"
      primary_key: "id"
      page_size: 50
      max_items: null                # Set an integer (e.g. 10) to limit during dev/testing

    carts:
      enabled: true
      write_disposition: "merge"
      primary_key: "id"
      page_size: 20
      max_items: null

    users:
      enabled: true
      write_disposition: "merge"
      primary_key: "id"
      page_size: 50
      max_items: null

    categories:
      enabled: true
      write_disposition: "replace"
      primary_key: "slug"
```

You can toggle entities, adjust batch sizes, or switch destinations purely via config changes without modifying code.

---

## 🔑 Authentication & Microsoft Fabric Setup

### For Local Development (Zero Cloud Setup)
No credentials required! DummyJSON is an open public API, and local runs store data directly in embedded DuckDB (`.data/ecommerce_local.duckdb`) or local Parquet files.

### For Microsoft Fabric OneLake (60-day Free Trial / Organizational Account)
1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```
2. In Microsoft Fabric, go to your **Lakehouse** $\rightarrow$ right-click the **`Files`** folder $\rightarrow$ **Properties** $\rightarrow$ copy **ABFS path**.
3. Paste into `.env`:
   ```env
   PIPELINE_ENV=fabric
   FABRIC_ONELAKE_ABFS_PATH=abfss://<workspace_guid>@onelake.dfs.fabric.microsoft.com/<lakehouse_guid>/Files/bronze/ecommerce
   ```
4. Authenticate your terminal with Azure CLI:
   ```powershell
   az login
   ```
   *(Leave `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` blank; `dlt` automatically authenticates via your `az login` session!)*

---

## 🚀 Running the Pipeline

### 1. Dry Run (Inspect API queries without loading)
```bash
python run.py --dry-run
```

### 2. Run Locally (Zero Cloud Setup using DuckDB)
```bash
python run.py --env local
```
*Data lands inside `.data/ecommerce_local.duckdb` for immediate SQL inspection.*

### 3. Extract Specific Entities
```bash
python run.py --entities products,carts
```

### 4. Run Ingestion to Microsoft Fabric OneLake
```bash
python run.py --env fabric
```
*Writes directly to your Fabric Lakehouse at `abfss://<workspace_guid>@onelake.dfs.fabric.microsoft.com/<lakehouse_guid>/Files/bronze/ecommerce`.*

---

## 📊 Downstream Transformations with dbt in Fabric

Once data lands in OneLake:
1. Microsoft Fabric registers the files or Delta tables in the Lakehouse catalog.
2. **dbt** (via `dbt-fabric`) connects to the Fabric SQL analytics endpoint.
3. Nested JSON objects (such as `carts.products` line items or `users.address`) are already normalized by `dlt` into relational tables linked by `_dlt_parent_id`.
4. Staging models (`stg_products`, `stg_carts`, `stg_cart_items`, `stg_users`) and dimensional marts (`fct_orders`, `dim_products`, `dim_customers`) are built seamlessly.

See [`docs/fabric_dbt_guide.md`](docs/fabric_dbt_guide.md) for full configuration, `profiles.yml` setup, and sample models.

---

## 🧪 Running Unit Tests

Run the included unit test suite:
```bash
python -m pytest tests/ -v
```
