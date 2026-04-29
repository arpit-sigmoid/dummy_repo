"""
ETL Pipeline for Order Data
Extracts data from JSON, validates, transforms, and loads to CSV (mock Snowflake)
"""

import json
import yaml
import pandas as pd
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Any

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_path: Path to config file
        
    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        print(f"Error parsing config file: {e}")
        raise


# Load configuration
config = load_config()

# Configure logging from config
logging.basicConfig(
    level=getattr(logging, config['logging']['level']),
    format=config['logging']['format'],
    handlers=[
        logging.FileHandler(config['log_file']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def extract_data(file_path: str, chunk_size: int = None) -> pd.DataFrame:
    """
    Extract data from JSON file with optional chunk processing for large datasets
    
    Args:
        file_path: Path to the JSON file
        chunk_size: Number of records to process at a time (None for all at once)
        
    Returns:
        DataFrame containing order data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If JSON is invalid
    """
    logger.info(f"Starting extraction from {file_path}")
    
    try:
        if chunk_size:
            # Process in chunks for large files
            logger.info(f"Using chunk processing with chunk_size={chunk_size}")
            chunks = []
            for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size):
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
        else:
            # Load all at once for smaller files
            df = pd.read_json(file_path, lines=True)
        
        logger.info(f"Successfully extracted {len(df)} records")
        return df
    except FileNotFoundError as e:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in {file_path}")
        raise


def validate_data(df: pd.DataFrame, required_columns: List[str], valid_regions: List[str]) -> pd.DataFrame:
    """
    Validate data using vectorized operations for large datasets.
    Checks required columns, removes negative amounts, duplicate order_ids, and invalid regions.
    
    Args:
        df: DataFrame containing order data
        required_columns: List of required column names
        valid_regions: List of valid region values
        
    Returns:
        Validated and filtered DataFrame
    """
    logger.info("Starting data validation")
    initial_count = len(df)
    
    # Check required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Convert valid_regions to set for efficient lookup
    valid_regions_set = set(valid_regions)
    
    # Vectorized operations for filtering
    # Remove negative amounts
    negative_mask = df['amount'] < 0
    negative_count = negative_mask.sum()
    if negative_count > 0:
        logger.warning(f"Removing {negative_count} records with negative amounts")
        df = df[~negative_mask]
    
    # Remove duplicate order_ids (keep first occurrence)
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
    
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform data using vectorized operations.
    Replaces null amounts with 0 and aggregates by region.
    
    Args:
        df: Validated DataFrame
        
    Returns:
        DataFrame with aggregated revenue by region
    """
    logger.info("Starting data transformation")
    
    try:
        # Replace null amounts with 0 (vectorized)
        null_count = df['amount'].isnull().sum()
        df['amount'] = df['amount'].fillna(0)
        logger.info(f"Replaced {null_count} null amounts with 0")
        
        # Aggregate total revenue by region (vectorized)
        aggregated_df = df.groupby('region', observed=True)['amount'].sum().reset_index()
        aggregated_df.columns = ['region', 'total_revenue']
        logger.info(f"Aggregated data into {len(aggregated_df)} regions")
        
        return aggregated_df
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise


def load_to_snowflake(df: pd.DataFrame, snowflake_config: Dict[str, str]) -> None:
    """
    Load data to Snowflake table
    
    Args:
        df: DataFrame to load
        snowflake_config: Dictionary containing Snowflake connection parameters
    """
    if not SNOWFLAKE_AVAILABLE:
        logger.error("Snowflake connector not installed. Install with: pip install snowflake-connector-python")
        raise ImportError("snowflake-connector-python is required for Snowflake loading")
    
    logger.info("Starting data load to Snowflake")
    
    try:
        conn = snowflake.connector.connect(
            user=snowflake_config['user'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            warehouse=snowflake_config.get('warehouse'),
            database=snowflake_config.get('database'),
            schema=snowflake_config.get('schema')
        )
        
        cursor = conn.cursor()
        table_name = snowflake_config['table']
        
        # Insert data row by row
        for _, row in df.iterrows():
            cursor.execute(
                f"INSERT INTO {table_name} (region, total_revenue) VALUES (%s, %s)",
                (row['region'], row['total_revenue'])
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully loaded {len(df)} records to Snowflake table {table_name}")
    except Exception as e:
        logger.error(f"Error during Snowflake load: {str(e)}")
        raise


def load_data(df: pd.DataFrame, output_path: str, use_snowflake: bool = False, snowflake_config: Dict[str, str] = None) -> None:
    """
    Load data to CSV file or Snowflake
    
    Args:
        df: DataFrame to save
        output_path: Path for output CSV file
        use_snowflake: If True, load to Snowflake instead of CSV
        snowflake_config: Dictionary containing Snowflake connection parameters
    """
    if use_snowflake:
        load_to_snowflake(df, snowflake_config)
    else:
        logger.info(f"Starting data load to {output_path}")
        
        try:
            # Create directory if it doesn't exist
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Save to CSV
            df.to_csv(output_path, index=False)
            logger.info(f"Successfully loaded {len(df)} records to {output_path}")
        except Exception as e:
            logger.error(f"Error during load: {str(e)}")
            raise


def run_etl_pipeline(input_file: str, output_file: str, required_columns: List[str], valid_regions: List[str], 
                    use_snowflake: bool = False, snowflake_config: Dict[str, str] = None, chunk_size: int = None) -> None:
    """
    Run the complete ETL pipeline with optimizations for large datasets
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output CSV file
        required_columns: List of required column names
        valid_regions: List of valid region values
        use_snowflake: If True, load to Snowflake instead of CSV
        snowflake_config: Dictionary containing Snowflake connection parameters
        chunk_size: Number of records to process at a time (for large files)
    """
    logger.info("="*50)
    logger.info("Starting ETL Pipeline")
    if use_snowflake:
        logger.info("Target: Snowflake")
    else:
        logger.info("Target: CSV")
    if chunk_size:
        logger.info(f"Chunk processing enabled: {chunk_size} records per chunk")
    logger.info("="*50)
    
    try:
        # Extract (now returns DataFrame with optional chunking)
        raw_df = extract_data(input_file, chunk_size)
        
        # Validate (now uses vectorized operations on DataFrame)
        validated_df = validate_data(raw_df, required_columns, valid_regions)
        
        # Transform (now accepts DataFrame directly)
        transformed_df = transform_data(validated_df)
        
        # Load
        load_data(transformed_df, output_file, use_snowflake, snowflake_config)
        
        logger.info("="*50)
        logger.info("ETL Pipeline completed successfully")
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="ETL Pipeline for Order Data - Extract, Transform, Load orders data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python etl_pipeline.py                          # Use default config.yaml
  python etl_pipeline.py --config my_config.yaml  # Use custom config file
  python etl_pipeline.py --input data/orders.json --output data/output.csv  # Override paths
  python etl_pipeline.py --log-level DEBUG        # Set log level to DEBUG
        """
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration YAML file (default: config.yaml)"
    )
    
    parser.add_argument(
        "--input",
        type=str,
        help="Override input file path from config"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="Override output file path from config"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Override log level from config"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and transform data without saving output"
    )
    
    parser.add_argument(
        "--snowflake",
        action="store_true",
        help="Load data to Snowflake instead of CSV"
    )
    
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Number of records to process at a time (for large files)"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override config with CLI arguments if provided
    input_file = args.input if args.input else config['input_file']
    output_file = args.output if args.output else config['output_file']
    
    # Override log level if provided
    if args.log_level:
        config['logging']['level'] = args.log_level
        # Reconfigure logging with new level
        logging.basicConfig(
            level=getattr(logging, config['logging']['level']),
            format=config['logging']['format'],
            handlers=[
                logging.FileHandler(config['log_file']),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger(__name__)
    
    # Get configuration values
    required_columns = config['required_columns']
    valid_regions = config['valid_regions']
    
    # Performance configuration
    chunk_size = args.chunk_size if args.chunk_size else config.get('performance', {}).get('chunk_size')
    
    # Snowflake configuration
    use_snowflake = args.snowflake
    snowflake_config = config.get('snowflake') if use_snowflake else None
    
    # Run pipeline
    if args.dry_run:
        logger.info("DRY RUN MODE - Output will not be saved")
        logger.info(f"Input file: {input_file}")
        if use_snowflake:
            logger.info(f"Target: Snowflake table {snowflake_config.get('table')} (will not be saved)")
        else:
            logger.info(f"Output file: {output_file} (will not be saved)")
    
    run_etl_pipeline(input_file, output_file, required_columns, valid_regions, use_snowflake, snowflake_config, chunk_size)
    
    if args.dry_run:
        logger.info("DRY RUN completed - No output saved")
