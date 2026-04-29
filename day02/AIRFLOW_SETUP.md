# Airflow ETL Pipeline Setup

This directory contains an Apache Airflow DAG for the ETL pipeline.

## Prerequisites

- Python 3.9+
- Apache Airflow 2.8.0+
- Docker (optional, for running Airflow with Docker Compose)

## Installation

### Option 1: Local Airflow Installation

1. Install Airflow and dependencies:
```bash
pip install -r airflow_requirements.txt
```

2. Initialize Airflow database:
```bash
airflow db init
```

3. Create an Airflow user:
```bash
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

4. Set the `AIRFLOW_HOME` environment variable to this directory:
```bash
export AIRFLOW_HOME=/path/to/day02
```

5. Copy the DAG file to your Airflow DAGs folder:
```bash
# If using default AIRFLOW_HOME
cp dags/etl_pipeline_dag.py $AIRFLOW_HOME/dags/

# Or set AIRFLOW_HOME to this directory
export AIRFLOW_HOME=$(pwd)
```

6. Start the Airflow webserver:
```bash
airflow webserver -p 8080
```

7. In another terminal, start the Airflow scheduler:
```bash
airflow scheduler
```

8. Access the Airflow UI at http://localhost:8080

### Option 2: Docker Compose (Recommended)

1. Create a `docker-compose.yml` file in this directory:
```yaml
version: '3.8'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db:/var/lib/postgresql/data

  airflow:
    image: apache/airflow:2.8.0-python3.10
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./dags/data:/opt/airflow/dags/data
    ports:
      - "8080:8080"
    command: >
      bash -c "
      airflow db init &&
      airflow users create -u admin -p admin -r Admin -e admin@example.com -f Admin -l User &&
      airflow webserver
      "

  scheduler:
    image: apache/airflow:2.8.0-python3.10
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
    volumes:
      - ./dags:/opt/airflow/dags
      - ./dags/data:/opt/airflow/dags/data
    command: airflow scheduler

volumes:
  postgres-db:
```

2. Start Airflow with Docker Compose:
```bash
docker-compose up -d
```

3. Access the Airflow UI at http://localhost:8080
   - Username: admin
   - Password: admin

## DAG Configuration

The ETL pipeline DAG (`etl_pipeline_dag.py) includes:

- **Schedule**: Daily (`@daily`)
- **Tasks**:
  1. `extract` - Reads data from JSON file
  2. `validate` - Validates data (removes negative amounts, duplicates, invalid regions)
  3. `transform` - Transforms data (replaces nulls, aggregates by region)
  4. `load` - Loads data to CSV file

- **Task Dependencies**: extract → validate → transform → load

## Data Files

- Input: `dags/data/orders.json` (JSON Lines format)
- Output: `dags/data/region_revenue.csv`

## Running the DAG

1. Enable the DAG in the Airflow UI
2. Trigger the DAG manually or wait for the scheduled run
3. Monitor task execution in the Airflow UI
4. Check the output file at `dags/data/region_revenue.csv`

## Monitoring

- View DAG runs in the Airflow UI
- Check task logs for detailed execution information
- Monitor task duration and retry attempts

## Customization

To customize the DAG:

1. Edit the configuration variables in `etl_pipeline_dag.py`:
   - `INPUT_FILE`: Path to input JSON file
   - `OUTPUT_FILE`: Path to output CSV file
   - `REQUIRED_COLUMNS`: List of required columns
   - `VALID_REGIONS`: List of valid region values

2. Modify the schedule interval in the DAG definition

3. Add additional tasks or change task dependencies as needed

## Troubleshooting

**DAG not appearing in Airflow UI:**
- Check that the DAG file is in the correct `dags` folder
- Verify the DAG file has no syntax errors
- Check Airflow scheduler logs

**Task failures:**
- Check task logs in the Airflow UI
- Verify input file exists and is accessible
- Ensure all required dependencies are installed

**Permission errors:**
- Ensure the Airflow user has read/write permissions for the data directory
