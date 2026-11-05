# Downstream Transformation Guide: Microsoft Fabric & dbt

This document outlines how data ingested from **DummyJSON** by `dlt` into **Microsoft Fabric OneLake** is consumed and transformed by **dbt** (data build tool) into production-grade Lakehouse/Warehouse Delta tables.

---

## 1. Fabric OneLake Storage Architecture (Medallion Pattern)

When `dlt` runs, it loads raw, typed, schema-inferred records into Fabric OneLake:

```
Fabric Workspace (OneLake)
│
└── 📁 Lakehouse: "bronze_ecommerce"
    │
    ├── 📁 Files/bronze/ecommerce/      <-- Raw landing parquet files (ingested by dlt)
    │   ├── products/
    │   ├── carts/
    │   ├── users/
    │   └── categories/
    │
    └── 📁 Tables/                      <-- Managed Delta Tables
        ├── bronze_products             (dlt can write directly here or dbt stages from Files)
        ├── bronze_carts
        ├── bronze_carts__products      (Normalized line items)
        ├── bronze_users
        │
        ├── silver_dim_products         (Cleaned & deduplicated via dbt)
        ├── silver_dim_customers        (Customer profiles and addresses)
        └── gold_fct_orders             (Order transactions and revenue metrics via dbt)
```

---

## 2. dlt's Automatic Relational Normalization

E-commerce APIs return deeply nested JSON structures (e.g., carts contain an array of purchased products; products contain reviews and dimensions; users contain nested company and address structures).

`dlt` solves this automatically:
1. **Root Tables**: `carts`, `products`, `users`
2. **Child Tables**: `carts__products` (individual line items), `products__reviews`
3. **Surrogate Keys**: `dlt` generates `_dlt_id` on parents and `_dlt_parent_id` on children so you can easily join them in SQL.

---

## 3. Connecting dbt to Microsoft Fabric

In Microsoft Fabric, you can transform data using dbt via the official adapter: `dbt-fabric` (supported by Microsoft).

### `profiles.yml` Configuration:
```yaml
fabric_dbt_project:
  target: dev
  outputs:
    dev:
      type: fabric
      driver: 'ODBC Driver 18 for SQL Server'
      server: '<fabric_workspace_guid>.datawarehouse.fabric.microsoft.com'
      database: '<fabric_lakehouse_or_dw_name>'
      schema: 'silver'
      authentication: 'service_principal' # or CLI / ActiveDirectoryPassword
      client_id: '{{ env_var("AZURE_CLIENT_ID") }}'
      client_secret: '{{ env_var("AZURE_CLIENT_SECRET") }}'
      tenant_id: '{{ env_var("AZURE_TENANT_ID") }}'
```

---

## 4. Example dbt Models for Fabric

### Staging Model: `models/staging/stg_products.sql`
```sql
with source as (
    select * from {{ source('bronze_ecommerce', 'products') }}
),

renamed as (
    select
        id as product_id,
        title as product_title,
        category,
        brand,
        sku,
        cast(price as decimal(10,2)) as unit_price,
        cast(discount_percentage as decimal(5,2)) as discount_pct,
        rating,
        stock as inventory_stock,
        availability_status,
        shipping_information,
        warranty_information,
        _dlt_load_id,
        _dlt_id
    from source
)

select * from renamed
```

### Staging Model: `models/staging/stg_cart_items.sql`
```sql
with line_items as (
    select * from {{ source('bronze_ecommerce', 'carts__products') }}
)

select
    id as product_id,
    title as item_title,
    cast(quantity as int) as quantity,
    cast(price as decimal(10,2)) as unit_price,
    cast(total as decimal(10,2)) as line_total,
    cast(discounted_total as decimal(10,2)) as line_discounted_total,
    _dlt_parent_id as cart_dlt_id
from line_items
```

### Mart Model: `models/marts/fct_orders.sql`
```sql
with carts as (
    select * from {{ source('bronze_ecommerce', 'carts') }}
),

items as (
    select * from {{ ref('stg_cart_items') }}
)

select
    c.id as order_cart_id,
    c.user_id as customer_id,
    c.total_products,
    c.total_quantity,
    cast(c.total as decimal(12,2)) as order_gross_total,
    cast(c.discounted_total as decimal(12,2)) as order_net_total,
    count(distinct i.product_id) as distinct_product_count
from carts c
left join items i
    on c._dlt_id = i.cart_dlt_id
group by
    c.id,
    c.user_id,
    c.total_products,
    c.total_quantity,
    c.total,
    c.discounted_total
```
