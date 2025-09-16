import os
import csv
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# Absolute path to the 'logs' directory from the script in 'scripts/'
LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

log_file_path = os.path.join(LOGS_DIR, 'load_data.log')

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

# Database configurations (will be set from environment variables)
SUPABASE_CONFIG = {
    'host': os.getenv('SUPABASE_HOST'),
    'database': os.getenv('SUPABASE_NAME'),
    'user': os.getenv('SUPABASE_USER'),
    'password': os.getenv('SUPABASE_PASSWORD'),
    'port': os.getenv('SUPABASE_PORT', '5432')
}

RDS_CONFIG = {
    'host': os.getenv('RDS_HOST'),
    'database': os.getenv('RDS_NAME'),
    'user': os.getenv('RDS_USER'),
    'password': os.getenv('RDS_PASSWORD'),
    'port': os.getenv('RDS_PORT', '5432')
}

# Data directory paths
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
SUPABASE_DATA_DIR = os.path.join(DATA_DIR, 'supabase')
RDS_DATA_DIR = os.path.join(DATA_DIR, 'rds')

# Table loading order (respecting foreign key constraints)
TABLE_ORDER = [
    'banco',
    'programa', 
    'usuario',
    'cardholder',
    'tarjeta_puntos'
]

# Table clear order (reverse of TABLE_ORDER to respect foreign key constraints)
TABLE_CLEAR_ORDER = [
    'tarjeta_puntos',
    'cardholder',
    'usuario',
    'programa',
    'banco'
]

# CSV files mapping
CSV_FILES = {
    'banco': 'banco.csv',
    'programa': 'programa.csv',
    'usuario': 'usuario.csv',
    'cardholder': 'card_holder.csv',
    'tarjeta_puntos': 'tarjeta_puntos.csv'
}

def parse_timestamp(timestamp_str):
    """Parse timestamp string to datetime object"""
    if not timestamp_str or timestamp_str.strip() == '':
        return None
    
    timestamp_str = timestamp_str.strip()
    
    # Try different timestamp formats
    formats = [
        '%Y-%m-%d %H:%M:%S',  # 2025-09-14 10:30:00
        '%Y-%m-%dT%H:%M:%S',  # 2025-09-14T10:30:00
        '%m/%d/%Y %H:%M',     # 4/1/2019 11:45
        '%Y-%m-%d',           # 2025-09-14
        '%m/%d/%Y'            # 4/1/2019
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    
    # If no format matches, try to parse as ISO format with timezone
    try:
        from dateutil import parser
        return parser.parse(timestamp_str)
    except:
        log(f"Warning: Could not parse timestamp '{timestamp_str}', using None")
        return None

def truncate_string(value, max_length):
    """Truncate string if it exceeds max_length"""
    if value and len(str(value)) > max_length:
        return str(value)[:max_length]
    return value

def clean_csv_row(row):
    """Clean CSV row to ensure proper number of values"""
    # Remove any None values and empty strings at the end
    values = list(row.values())
    while values and (values[-1] is None or values[-1] == ''):
        values.pop()
    return values

def clear_all_tables(conn):
    """Clear all tables in the correct order to respect foreign key constraints"""
    try:
        log("Clearing all existing data from tables...")
        with conn.cursor() as cursor:
            for table_name in TABLE_CLEAR_ORDER:
                log(f"Clearing table: {table_name}")
                cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()
        log("All tables cleared successfully")
    except Exception as e:
        log(f"Error clearing tables: {e}")
        raise e

def load_table_data(conn, table_name, csv_file_path):
    """Load data from CSV file to database table"""
    log(f"Loading data for table: {table_name}")
    
    try:
        # Get table column information
        columns_query = """
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """
        with conn.cursor() as cursor:
            cursor.execute(columns_query, (table_name,))
            columns_info = cursor.fetchall()
        
        if not columns_info:
            log(f"Warning: No columns found for table {table_name}")
            return False
        
        # Read CSV file
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Prepare INSERT statement with ON CONFLICT DO NOTHING
            column_names = [col[0] for col in columns_info]
            placeholders = ', '.join(['%s'] * len(column_names))
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(column_names)})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """
            
            success_count = 0
            error_count = 0
            
            for row in reader:
                try:
                    # Clean the row
                    values = clean_csv_row(row)
                    
                    # Ensure we have the right number of values
                    if len(values) != len(column_names):
                        log(f"Warning: Row has {len(values)} values but table has {len(column_names)} columns")
                        continue
                    
                    # Process each value based on column type
                    processed_values = []
                    for i, (value, col_info) in enumerate(zip(values, columns_info)):
                        col_name, data_type, max_length, is_nullable = col_info
                        
                        if value is None or value == '':
                            processed_values.append(None)
                        elif 'timestamp' in data_type.lower():
                            processed_values.append(parse_timestamp(value))
                        elif 'character' in data_type.lower() and max_length:
                            processed_values.append(truncate_string(value, max_length))
                        else:
                            processed_values.append(value)
                    
                    # Execute insert
                    with conn.cursor() as cursor:
                        cursor.execute(insert_query, processed_values)
                        conn.commit()
                    success_count += 1
                    
                except Exception as e:
                    error_count += 1
                    log(f"Error inserting row in {table_name}: {e}")
                    continue
            
            log(f"Successfully inserted {success_count} rows into {table_name}")
            if error_count > 0:
                log(f"Skipped {error_count} duplicate rows in {table_name}")
            
            return True
            
    except Exception as e:
        log(f"Error loading table {table_name}: {e}")
        return False

def load_data_to_database(db_type, db_name, data_dir, clear_existing=False):
    """Load all CSV data to a specific database from a specific data directory"""
    log(f"Starting data load to {db_name} from {data_dir}")
    
    try:
        # Get database configuration
        if db_type == 'supabase':
            config = SUPABASE_CONFIG
        elif db_type == 'rds':
            config = RDS_CONFIG
        else:
            raise ValueError(f"Unknown database type: {db_type}")
        
        # Create connection
        conn = psycopg2.connect(**config)
        log(f"Connected to {db_name}")
        
        if clear_existing:
            clear_all_tables(conn)
        
        success_count = 0
        total_tables = len(TABLE_ORDER)
        
        for table_name in TABLE_ORDER:
            csv_file = CSV_FILES[table_name]
            csv_file_path = os.path.join(data_dir, csv_file)
            
            if not os.path.exists(csv_file_path):
                log(f"Warning: CSV file not found: {csv_file_path}")
                continue
            
            if load_table_data(conn, table_name, csv_file_path):
                success_count += 1
        
        conn.close()
        log(f"Data load to {db_name} completed: {success_count}/{total_tables} tables loaded successfully")
        return success_count == total_tables
        
    except Exception as e:
        log(f"Error loading data to {db_name}: {e}")
        return False

def main():
    """Main function to load data to specified database"""
    import sys
    
    if len(sys.argv) > 1:
        # Load data to specific database
        db_type = sys.argv[1].lower()
        
        if db_type == 'supabase':
            log("Loading data to SUPABASE from supabase/ directory")
            log("=" * 50)
            success = load_data_to_database(
                'supabase', 
                "SUPABASE", 
                SUPABASE_DATA_DIR, 
                clear_existing=True
            )
        elif db_type == 'rds':
            log("Loading data to RDS from rds/ directory")
            log("=" * 50)
            success = load_data_to_database(
                'rds', 
                "RDS", 
                RDS_DATA_DIR, 
                clear_existing=True
            )
        else:
            log(f"Error: Unknown database type '{db_type}'. Use 'supabase' or 'rds'")
            sys.exit(1)
        
        if success:
            log(f"Data load to {db_type.upper()} completed successfully")
        else:
            log(f"Data load to {db_type.upper()} failed")
            sys.exit(1)
    else:
        # Load data to both databases (original behavior)
        log("Starting data load process...")
        log("=" * 50)
        
        # Load data to Supabase
        log("Loading data to SUPABASE from supabase/ directory")
        log("=" * 50)
        supabase_success = load_data_to_database(
            'supabase', 
            "SUPABASE", 
            SUPABASE_DATA_DIR, 
            clear_existing=True
        )
        
        # Load data to RDS
        log("Loading data to RDS from rds/ directory")
        log("=" * 50)
        rds_success = load_data_to_database(
            'rds', 
            "RDS", 
            RDS_DATA_DIR, 
            clear_existing=True
        )
        
        # Summary
        log("=" * 50)
        if supabase_success and rds_success:
            log("Data load process completed successfully")
        else:
            log("Some data loads failed. Check logs for details")
            sys.exit(1)
    
    log("=" * 50)

if __name__ == "__main__":
    main()
