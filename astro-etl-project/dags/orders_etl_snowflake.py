from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import logging
from pathlib import Path
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
INPUT_FILE = '/usr/local/airflow/dags/data/orders.json'
REQUIRED_COLUMNS = ['order_id', 'customer_id', 'region', 'amount', 'order_date']
VALID_REGIONS = ['North', 'South', 'East', 'West']

# Snowflake configuration
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_TABLE = 'orders_summary'
SNOWFLAKE_SCHEMA = 'public'
SNOWFLAKE_DATABASE = 'analytics'


def extract_data(**context):
    """
    Extract data from JSON file
    """
    logger.info(f"Starting extraction from {INPUT_FILE}")
    
    try:
        df = pd.read_json(INPUT_FILE, lines=True)
        logger.info(f"Successfully extracted {len(df)} records")
        
        # Push DataFrame to XCom for next task
        context['task_instance'].xcom_push(key='raw_data', value=df.to_json())
        return len(df)
    except FileNotFoundError as e:
        logger.error(f"File not found: {INPUT_FILE}")
        raise
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        raise


def validate_data(**context):
    """
    Validate data using vectorized operations
    """
    logger.info("Starting data validation")
    
    # Pull data from XCom
    ti = context['task_instance']
    raw_data_json = ti.xcom_pull(task_ids='extract', key='raw_data')
    df = pd.read_json(raw_data_json)
    
    initial_count = len(df)
    
    # Check required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Convert valid_regions to set
    valid_regions_set = set(VALID_REGIONS)
    
    # Vectorized operations for filtering
    # Remove negative amounts
    negative_mask = df['amount'] < 0
    negative_count = negative_mask.sum()
    if negative_count > 0:
        logger.warning(f"Removing {negative_count} records with negative amounts")
        df = df[~negative_mask]
    
    # Remove duplicate order_ids
    duplicate_count = df['order_id'].duplicated().sum()
    if duplicate_count > 0:
        logger.warning(f"Removing {duplicate_count} duplicate order_ids")
        df = df.drop_duplicates(subset=['order_id'], keep='first')
    
    # Validate region values
    invalid_region_mask = ~df['region'].isin(valid_regions_set)
    invalid_region_count = invalid_region_mask.sum()
    if invalid_region_count > 0:
        logger.warning(f"Removing {invalid_region_count} records with invalid regions")
        df = df[~invalid_region_mask]
    
    final_count = len(df)
    logger.info(f"Validation complete: {final_count} valid records remaining (removed {initial_count - final_count})")
    
    # Push validated data to XCom
    ti.xcom_push(key='validated_data', value=df.to_json())
    return final_count


def transform_data(**context):
    """
    Transform data using vectorized operations
    """
    logger.info("Starting data transformation")
    
    # Pull validated data from XCom
    ti = context['task_instance']
    validated_data_json = ti.xcom_pull(task_ids='validate', key='validated_data')
    df = pd.read_json(validated_data_json)
    
    try:
        # Replace null amounts with 0 (vectorized)
        null_count = df['amount'].isnull().sum()
        df['amount'] = df['amount'].fillna(0)
        logger.info(f"Replaced {null_count} null amounts with 0")
        
        # Aggregate total revenue by region (vectorized)
        aggregated_df = df.groupby('region', observed=True)['amount'].sum().reset_index()
        aggregated_df.columns = ['region', 'total_revenue']
        logger.info(f"Aggregated data into {len(aggregated_df)} regions")
        
        # Push transformed data to XCom
        ti.xcom_push(key='transformed_data', value=aggregated_df.to_json())
        return len(aggregated_df)
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise


def load_to_snowflake(**context):
    """
    Load transformed data to Snowflake using SnowflakeHook
    """
    logger.info("Starting data load to Snowflake")
    
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    # Pull transformed data from XCom
    ti = context['task_instance']
    transformed_data_json = ti.xcom_pull(task_ids='transform', key='transformed_data')
    df = pd.read_json(transformed_data_json)
    
    try:
        # Create Snowflake connection
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            region VARCHAR(50),
            total_revenue FLOAT
        )
        """
        snowflake_hook.run(create_table_sql)
        logger.info(f"Table {SNOWFLAKE_TABLE} created/verified")
        
        # Clear existing data (optional - remove if you want to append)
        clear_sql = f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
        snowflake_hook.run(clear_sql)
        logger.info("Cleared existing data from table")
        
        # Insert data row by row
        for _, row in df.iterrows():
            insert_sql = f"""
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} 
            (region, total_revenue) 
            VALUES ('{row['region']}', {row['total_revenue']})
            """
            snowflake_hook.run(insert_sql)
        
        logger.info(f"Successfully loaded {len(df)} records to Snowflake table {SNOWFLAKE_TABLE}")
        return len(df)
    except Exception as e:
        logger.error(f"Error during Snowflake load: {str(e)}")
        raise


# Define the DAG
with DAG(
    'orders_etl_snowflake',
    default_args=default_args,
    description='ETL Pipeline for Order Data - Extract, Transform, Load to Snowflake',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['etl', 'orders', 'snowflake', 'analytics'],
) as dag:
    
    # Define tasks
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        doc_md='Extract data from JSON file'
    )
    
    validate_task = PythonOperator(
        task_id='validate',
        python_callable=validate_data,
        doc_md='Validate data - remove negative amounts, duplicates, invalid regions'
    )
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        doc_md='Transform data - replace nulls, aggregate by region'
    )
    
    load_snowflake_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        doc_md='Load data to Snowflake table'
    )
    
    # Set task dependencies
    extract_task >> validate_task >> transform_task >> load_snowflake_task
