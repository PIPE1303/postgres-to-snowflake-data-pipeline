from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Andres_Marciales',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description='Copy Parquet data from S3 to Snowflake tables',
    schedule=None,
    catchup=False,
    tags=['s3', 'snowflake', 'parquet', 'copy', 'etl'],
    dag_id='s3_to_snowflake_dag'
)
def s3_to_snowflake_dag():
    snowflake_conn_id = 'snowflake_default'
    s3_bucket = Variable.get("S3_BUCKET", default_var="gennius-bucket")
    
    # Updated with correct Snowflake external stage and schema
    snowflake_stage = "@GENNIUS_XYZ.COMPANY_BRONZE_LAYER.S3_STAGE"
    snowflake_schema = "GENNIUS_XYZ.COMPANY_BRONZE_LAYER"

    tables = ['banco', 'programa', 'usuario', 'cardholder', 'tarjeta_puntos']
    
    @task
    def create_snowflake_tables():
        """Create Snowflake tables with proper schema for Parquet data"""
        logger.info(f"Creating Snowflake tables in schema: {snowflake_schema}")
        
        # Create tables with proper data types for Parquet
        create_table_sqls = []
        
        # Banco table
        create_table_sqls.append(f"""
            CREATE OR REPLACE TABLE {snowflake_schema}.banco (
                bankid INTEGER COMMENT 'Identificador único del banco',
                issuer VARCHAR COMMENT 'Nombre del emisor del banco',
                name VARCHAR COMMENT 'Nombre comercial del banco',
                creation_date_time TIMESTAMP_NTZ COMMENT 'Fecha y hora de creación del registro del banco',
                status VARCHAR COMMENT 'Estado actual del banco (activo, inactivo, suspendido)',
                source_database VARCHAR COMMENT 'Base de datos origen (SUPABASE o RDS)',
                export_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp de cuando se exportó el registro a Snowflake'
            ) COMMENT = 'Tabla maestra de bancos que participan en el programa de puntos';
        """)
        
        # Programa table
        create_table_sqls.append(f"""
            CREATE OR REPLACE TABLE {snowflake_schema}.programa (
                programid INTEGER COMMENT 'Identificador único del programa de puntos',
                name VARCHAR COMMENT 'Nombre del programa de puntos',
                redemption_value_per_point DECIMAL(10,6) COMMENT 'Valor de redención por punto en moneda local',
                creation_date_time TIMESTAMP_NTZ COMMENT 'Fecha y hora de creación del programa',
                source_database VARCHAR COMMENT 'Base de datos origen (SUPABASE o RDS)',
                export_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp de cuando se exportó el registro a Snowflake'
            ) COMMENT = 'Tabla maestra de programas de puntos disponibles';
        """)
        
        # Usuario table
        create_table_sqls.append(f"""
            CREATE OR REPLACE TABLE {snowflake_schema}.usuario (
                id_user INTEGER COMMENT 'Identificador único del usuario en el sistema',
                firstname VARCHAR COMMENT 'Nombre del usuario',
                lastname VARCHAR COMMENT 'Apellido del usuario',
                identificationnumber VARCHAR COMMENT 'Número de identificación (cédula, pasaporte, etc.)',
                emailaddress VARCHAR COMMENT 'Dirección de correo electrónico del usuario',
                streetaddress VARCHAR COMMENT 'Dirección física del usuario',
                idauthuserstatus INTEGER COMMENT 'Estado de autenticación del usuario (1=activo, 0=inactivo)',
                createdat TIMESTAMP_NTZ COMMENT 'Fecha y hora de creación del usuario en el sistema',
                channel_id INTEGER COMMENT 'Identificador del canal por el cual se registró el usuario',
                source_database VARCHAR COMMENT 'Base de datos origen (SUPABASE o RDS)',
                export_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp de cuando se exportó el registro a Snowflake'
            ) COMMENT = 'Tabla de usuarios registrados en el sistema de puntos';
        """)
        
        # Cardholder table
        create_table_sqls.append(f"""
            CREATE OR REPLACE TABLE {snowflake_schema}.cardholder (
                cardholder_id INTEGER COMMENT 'Identificador único del titular de la tarjeta',
                first_name VARCHAR COMMENT 'Nombre del titular de la tarjeta',
                last_name VARCHAR COMMENT 'Apellido del titular de la tarjeta',
                email VARCHAR COMMENT 'Correo electrónico del titular de la tarjeta',
                creation_date_time TIMESTAMP_NTZ COMMENT 'Fecha y hora de creación del titular',
                user_id INTEGER COMMENT 'Referencia al usuario asociado (FK a usuario.id_user)',
                status VARCHAR COMMENT 'Estado del titular (activo, inactivo, suspendido)',
                source_database VARCHAR COMMENT 'Base de datos origen (SUPABASE o RDS)',
                export_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp de cuando se exportó el registro a Snowflake'
            ) COMMENT = 'Tabla de titulares de tarjetas de puntos';
        """)
        
        # Tarjeta_puntos table
        create_table_sqls.append(f"""
            CREATE OR REPLACE TABLE {snowflake_schema}.tarjeta_puntos (
                points_card_id INTEGER COMMENT 'Identificador único de la tarjeta de puntos',
                cardholder_id INTEGER COMMENT 'Referencia al titular de la tarjeta (FK a cardholder.cardholder_id)',
                program_id INTEGER COMMENT 'Referencia al programa de puntos (FK a programa.programid)',
                bank_id INTEGER COMMENT 'Referencia al banco emisor (FK a banco.bankid)',
                transaction_date_time TIMESTAMP_NTZ COMMENT 'Fecha y hora de la transacción de puntos',
                type VARCHAR COMMENT 'Tipo de transacción (CREDIT=acumulación, DEBIT=redención)',
                points INTEGER COMMENT 'Cantidad de puntos involucrados en la transacción',
                operation VARCHAR COMMENT 'Descripción de la operación realizada',
                source_database VARCHAR COMMENT 'Base de datos origen (SUPABASE o RDS)',
                export_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp de cuando se exportó el registro a Snowflake'
            ) COMMENT = 'Tabla de transacciones de puntos en tarjetas';
        """)
        
        # Execute all create table statements
        for i, sql in enumerate(create_table_sqls):
            logger.info(f"Creating table {i+1}/5 in schema {snowflake_schema}...")
            SQLExecuteQueryOperator(
                task_id=f'create_table_{i+1}',
                conn_id=snowflake_conn_id,
                sql=sql
            ).execute(context={})
        
        return f"All Snowflake tables created successfully in schema {snowflake_schema}"

    @task
    def copy_data_to_snowflake(table_name: str):
        """Copy Parquet data for a specific table from S3 to Snowflake"""
        logger.info(f"Copying Parquet data for table {table_name} from S3 to Snowflake...")
        
        # COPY INTO command optimized for Parquet files
        # Using the correct external stage and schema
        copy_sql = f"""
            COPY INTO {snowflake_schema}.{table_name}
            FROM {snowflake_stage}/data/{table_name}/
            FILE_FORMAT = (
                TYPE = PARQUET
                COMPRESSION = AUTO
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
            PURGE = FALSE;
        """
        
        logger.info(f"Executing COPY INTO for {snowflake_schema}.{table_name}")
        logger.info(f"Using external stage: {snowflake_stage}")
        logger.info(f"COPY SQL: {copy_sql}")
        
        return SQLExecuteQueryOperator(
            task_id=f'copy_{table_name}_to_snowflake',
            conn_id=snowflake_conn_id,
            sql=copy_sql
        ).execute(context={})

    @task
    def verify_data_loaded():
        """Verify that data was loaded correctly"""
        logger.info("Verifying data load...")
        
        verification_sql = f"""
            SELECT 
                'banco' as table_name, 
                COUNT(*) as row_count,
                COUNT(DISTINCT source_database) as source_count
            FROM {snowflake_schema}.banco
            UNION ALL
            SELECT 
                'programa' as table_name, 
                COUNT(*) as row_count,
                COUNT(DISTINCT source_database) as source_count
            FROM {snowflake_schema}.programa
            UNION ALL
            SELECT 
                'usuario' as table_name, 
                COUNT(*) as row_count,
                COUNT(DISTINCT source_database) as source_count
            FROM {snowflake_schema}.usuario
            UNION ALL
            SELECT 
                'cardholder' as table_name, 
                COUNT(*) as row_count,
                COUNT(DISTINCT source_database) as source_count
            FROM {snowflake_schema}.cardholder
            UNION ALL
            SELECT 
                'tarjeta_puntos' as table_name, 
                COUNT(*) as row_count,
                COUNT(DISTINCT source_database) as source_count
            FROM {snowflake_schema}.tarjeta_puntos;
        """
        
        result = SQLExecuteQueryOperator(
            task_id='verify_data_loaded',
            conn_id=snowflake_conn_id,
            sql=verification_sql
        ).execute(context={})
        
        logger.info(f"Data verification results: {result}")
        return result

    @task
    def create_data_summary_table():
        """Create data summary table with statistics"""
        logger.info(f"Creating data summary table in Snowflake schema {snowflake_schema}...")
        summary_sql = f"""
            CREATE OR REPLACE TABLE {snowflake_schema}.data_summary AS
            SELECT
                'banco' AS table_name,
                source_database,
                COUNT(*) AS record_count,
                MAX(export_timestamp) AS last_export_timestamp
            FROM {snowflake_schema}.banco
            GROUP BY source_database
            UNION ALL
            SELECT
                'programa' AS table_name,
                source_database,
                COUNT(*) AS record_count,
                MAX(export_timestamp) AS last_export_timestamp
            FROM {snowflake_schema}.programa
            GROUP BY source_database
            UNION ALL
            SELECT
                'usuario' AS table_name,
                source_database,
                COUNT(*) AS record_count,
                MAX(export_timestamp) AS last_export_timestamp
            FROM {snowflake_schema}.usuario
            GROUP BY source_database
            UNION ALL
            SELECT
                'cardholder' AS table_name,
                source_database,
                COUNT(*) AS record_count,
                MAX(export_timestamp) AS last_export_timestamp
            FROM {snowflake_schema}.cardholder
            GROUP BY source_database
            UNION ALL
            SELECT
                'tarjeta_puntos' AS table_name,
                source_database,
                COUNT(*) AS record_count,
                MAX(export_timestamp) AS last_export_timestamp
            FROM {snowflake_schema}.tarjeta_puntos
            GROUP BY source_database;
        """
        return SQLExecuteQueryOperator(
            task_id='create_data_summary',
            conn_id=snowflake_conn_id,
            sql=summary_sql
        ).execute(context={})

    @task
    def s3_to_snowflake_complete():
        """Final task to mark completion"""
        logger.info("All Parquet data copied from S3 to Snowflake successfully!")
        logger.info(f"Data is now available in schema: {snowflake_schema}")
        logger.info("Data summary table created with statistics by table and source")
        return "S3 to Snowflake pipeline completed"

    # Define task flow
    create_tables = create_snowflake_tables()
    copy_tasks = [copy_data_to_snowflake.override(task_id=f'copy_{table}_task')(table) for table in tables]
    verify_data = verify_data_loaded()
    summary_table = create_data_summary_table()
    completion = s3_to_snowflake_complete()

    # Set dependencies
    create_tables >> copy_tasks >> verify_data >> summary_table >> completion

# Create the DAG instance
s3_to_snowflake_dag_instance = s3_to_snowflake_dag()
