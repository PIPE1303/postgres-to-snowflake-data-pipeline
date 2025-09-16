import os
import pandas as pd
import boto3
import logging
from datetime import datetime
import sys
import warnings
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import psycopg2
import pytz

# Suppress pandas warnings
warnings.filterwarnings('ignore', category=UserWarning)

# Colombia timezone (UTC-5)
COLOMBIA_TZ = pytz.timezone('America/Bogota')

# Absolute path to the 'logs' directory from the script in 'scripts/'
LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

log_file_path = os.path.join(LOGS_DIR, 'export_to_s3.log')

logging.basicConfig(
    filename=log_file_path,
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log(msg):
    """Logs a message to both console and file."""
    print(msg)
    logging.info(msg)

def get_colombia_datetime():
    """Get current datetime in Colombia timezone"""
    return datetime.now(COLOMBIA_TZ)

# S3 Configuration - Use environment variables (passed by DAG)
S3_CONFIG = {
    'bucket': os.getenv("S3_BUCKET"),
    'region': os.getenv("AWS_REGION", "us-east-1"),
    'access_key': os.getenv("AWS_ACCESS_KEY_ID"),
    'secret_key': os.getenv("AWS_SECRET_ACCESS_KEY")
}

# Table list to export
TABLES = [
    'banco',
    'programa', 
    'usuario',
    'cardholder',
    'tarjeta_puntos'
]

def get_s3_client():
    """Create and return S3 client"""
    log(f"Using S3 bucket: {S3_CONFIG['bucket']}")
    return boto3.client(
        's3',
        aws_access_key_id=S3_CONFIG['access_key'],
        aws_secret_access_key=S3_CONFIG['secret_key'],
        region_name=S3_CONFIG['region']
    )

def export_table_to_s3(conn, table_name, db_name, s3_client):
    """Export a single table to S3 as Parquet"""
    log(f"Exporting table {table_name} from {db_name} to S3 as Parquet...")
    
    try:
        # Query the table using psycopg2 connection
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            log(f"Warning: Table {table_name} from {db_name} is empty")
            return True
        
        # Add source database column
        df['source_database'] = db_name.lower()
        
        # Add export timestamp (Colombia timezone)
        df['export_timestamp'] = get_colombia_datetime().isoformat()
        
        # Convert timestamp columns to string format to avoid nanosecond issues
        timestamp_columns = ['creation_date_time', 'createdat', 'transaction_date_time']
        for col in timestamp_columns:
            if col in df.columns:
                # Convert to string format if it's a datetime column
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert to PyArrow Table for better type preservation
        table = pa.Table.from_pandas(df)
        
        # Convert to Parquet bytes
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)
        
        # S3 key with organized structure: database/table/date/file.parquet (Colombia timezone)
        colombia_date = get_colombia_datetime()
        date_str = colombia_date.strftime('%Y/%m/%d')
        s3_key = f"data/{table_name}/{date_str}/{db_name.lower()}_{table_name}.parquet"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_CONFIG['bucket'],
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        log(f"Successfully exported {len(df)} rows from {table_name} ({db_name}) to s3://{S3_CONFIG['bucket']}/{s3_key}")
        log(f"Parquet file size: {len(parquet_buffer.getvalue())} bytes")
        return True
        
    except Exception as e:
        log(f"Error exporting table {table_name} from {db_name}: {e}")
        return False

def export_database_to_s3(db_config, db_name, s3_client):
    """Export all tables from a database to S3"""
    log(f"Starting export of {db_name} database to S3 as Parquet files...")
    
    try:
        # Create psycopg2 connection
        conn = psycopg2.connect(
            host=db_config['host'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port']
        )
        log(f"Connected to {db_name}")
        
        success_count = 0
        total_tables = len(TABLES)
        
        for table_name in TABLES:
            if export_table_to_s3(conn, table_name, db_name, s3_client):
                success_count += 1
        
        conn.close()
        log(f"Export completed for {db_name}: {success_count}/{total_tables} tables exported successfully")
        return success_count == total_tables
        
    except Exception as e:
        log(f"Error exporting {db_name} database: {e}")
        return False

def main():
    """Main function to export all databases to S3"""
    log("Starting data export to S3 as Parquet files...")
    log("=" * 50)
    
    try:
        # Validate S3 configuration
        if not all([S3_CONFIG['bucket'], S3_CONFIG['access_key'], S3_CONFIG['secret_key']]):
            log("Error: S3 configuration is incomplete. Please check your environment variables.")
            sys.exit(1)
        
        # Create S3 client
        s3_client = get_s3_client()
        log(f"Connected to S3 bucket: {S3_CONFIG['bucket']}")
        
        # Database configurations
        supabase_config = {
            'host': os.getenv("SUPABASE_HOST"),
            'database': os.getenv("SUPABASE_NAME"),
            'user': os.getenv("SUPABASE_USER"),
            'password': os.getenv("SUPABASE_PASSWORD"),
            'port': os.getenv("SUPABASE_PORT", "5432")
        }
        
        rds_config = {
            'host': os.getenv("RDS_HOST"),
            'database': os.getenv("RDS_NAME"),
            'user': os.getenv("RDS_USER"),
            'password': os.getenv("RDS_PASSWORD"),
            'port': os.getenv("RDS_PORT", "5432")
        }
        
        # Export Supabase
        log("Exporting SUPABASE database...")
        supabase_success = export_database_to_s3(supabase_config, "SUPABASE", s3_client)
        
        # Export RDS
        log("Exporting RDS database...")
        rds_success = export_database_to_s3(rds_config, "RDS", s3_client)
        
        # Summary
        log("=" * 50)
        if supabase_success and rds_success:
            log("All databases exported successfully to S3 as Parquet files")
            log("Data is now available in S3 organized by table:")
            log(f"  - s3://{S3_CONFIG['bucket']}/data/banco/")
            log(f"  - s3://{S3_CONFIG['bucket']}/data/programa/")
            log(f"  - s3://{S3_CONFIG['bucket']}/data/usuario/")
            log(f"  - s3://{S3_CONFIG['bucket']}/data/cardholder/")
            log(f"  - s3://{S3_CONFIG['bucket']}/data/tarjeta_puntos/")
            log("Format: Parquet (optimized for Snowflake)")
        else:
            log("Some exports failed. Check logs for details")
            sys.exit(1)
        
        log("=" * 50)
        
    except Exception as e:
        log(f"Fatal error in export process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
