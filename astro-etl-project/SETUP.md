# Astro ETL Pipeline - Snowflake Integration

This project uses Astro (by Astronomer) to build an ETL pipeline that extracts order data, validates and transforms it, then loads to Snowflake.

## Prerequisites

- Astro CLI installed
- Snowflake account with appropriate permissions
- Docker (for running Astro locally)

## Setup Instructions

### 1. Configure Snowflake Connection

Add Snowflake connection to Airflow:

```bash
astro dev start
```

Then in the Airflow UI (http://localhost:8080):
1. Go to Admin → Connections
2. Create a new connection with:
   - **Connection Id**: `snowflake_conn`
   - **Connection Type**: Snowflake
   - **Login**: Your Snowflake username
   - **Password**: Your Snowflake password
   - **Account**: Your Snowflake account (e.g., `xy12345.us-east-1`)
   - **Warehouse**: Your Snowflake warehouse
   - **Database**: `analytics` (or your target database)
   - **Schema**: `public` (or your target schema)
   - **Role**: Your Snowflake role

### 2. Start Astro Project

```bash
cd astro-etl-project
astro dev start
```

This will:
- Build the Docker image
- Start Airflow webserver, scheduler, and postgres
- Open Airflow UI at http://localhost:8080

### 3. Create Snowflake Table

Run this SQL in your Snowflake worksheet:

```sql
CREATE DATABASE IF NOT EXISTS analytics;
USE DATABASE analytics;
CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

CREATE TABLE IF NOT EXISTS orders_summary (
    region VARCHAR(50),
    total_revenue FLOAT
);
```

### 4. Enable and Run the DAG

1. In Airflow UI, find the DAG `orders_etl_snowflake`
2. Toggle the DAG to enable it
3. Click "Trigger DAG" to run manually, or wait for the scheduled daily run

## DAG Structure

The DAG consists of 4 tasks:

1. **extract**: Reads data from `dags/data/orders.json` (JSON Lines format)
2. **validate**: Validates data using vectorized operations:
   - Removes negative amounts
   - Removes duplicate order_ids
   - Removes invalid regions
3. **transform**: Transforms data:
   - Replaces null amounts with 0
   - Aggregates total revenue by region
4. **load_to_snowflake**: Loads data to Snowflake:
   - Creates table if not exists
   - Truncates existing data
   - Inserts aggregated data

## Configuration

Edit the configuration variables in `dags/orders_etl_snowflake.py`:

```python
INPUT_FILE = '/usr/local/airflow/dags/data/orders.json'
REQUIRED_COLUMNS = ['order_id', 'customer_id', 'region', 'amount', 'order_date']
VALID_REGIONS = ['North', 'South', 'East', 'West']

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_TABLE = 'orders_summary'
SNOWFLAKE_SCHEMA = 'public'
SNOWFLAKE_DATABASE = 'analytics'
```

## Data Format

Input file (`dags/data/orders.json`) should be in JSON Lines format:

```json
{"order_id": 1, "customer_id": 101, "region": "North", "amount": 150.50, "order_date": "2024-01-15"}
{"order_id": 2, "customer_id": 102, "region": "South", "amount": 200.00, "order_date": "2024-01-16"}
```

## Monitoring

- View DAG runs in Airflow UI
- Check task logs for detailed execution information
- Monitor task duration and retry attempts
- Query Snowflake table to verify data: `SELECT * FROM orders_summary;`

## Troubleshooting

**DAG not appearing:**
- Verify DAG file is in `dags/` folder
- Check for syntax errors in DAG file
- Restart Astro: `astro dev restart`

**Snowflake connection error:**
- Verify connection credentials in Airflow UI
- Check Snowflake account format (should be `xy12345.us-east-1`)
- Ensure network access to Snowflake

**Task failures:**
- Check task logs in Airflow UI
- Verify input file exists and is accessible
- Ensure Snowflake table exists and has correct schema

**Docker issues:**
- Ensure Docker is running
- Check available disk space
- Try `astro dev prune` to clean up

## Stopping Astro

```bash
astro dev stop
```

## Deploying to Astro Cloud

To deploy to Astro Cloud:

```bash
astro deploy
```

This requires an Astro Cloud account and proper configuration in `astro.config.yaml`.
