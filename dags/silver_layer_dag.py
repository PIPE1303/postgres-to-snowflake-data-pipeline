from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import pandas as pd
import psycopg2

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
    description='Create Silver layer table in Snowflake and export to Supabase',
    schedule=None,
    catchup=False,
    tags=['silver', 'snowflake', 'supabase', 'reporte_gerencial'],
    dag_id='silver_layer_dag'
)
def silver_layer_dag():
    snowflake_conn_id = 'snowflake_default'
    snowflake_schema = "GENNIUS_XYZ.COMPANY_SILVER_LAYER"
    

    @task
    def create_reporte_gerencial_table():
        """Create reporte_gerencial table in Snowflake Silver layer"""
        logger.info(f"Creating reporte_gerencial table in schema: {snowflake_schema}")
        
        create_table_sql = f"""
            CREATE OR REPLACE TABLE {snowflake_schema}.reporte_gerencial (
                ANIO INTEGER COMMENT 'Año de la transacción',
                BANCO VARCHAR COMMENT 'Nombre del banco emisor',
                PROGRAMA VARCHAR COMMENT 'Nombre del programa de puntos',
                TOTAL_CREDITOS INTEGER COMMENT 'Total de puntos acumulados (créditos)',
                TOTAL_DEBITOS INTEGER COMMENT 'Total de puntos redimidos (débitos)',
                CARDHOLDERS_UNICOS INTEGER COMMENT 'Número de titulares únicos que participaron',
                CREATED_AT TIMESTAMP_NTZ COMMENT 'Fecha y hora de creación del reporte'
            ) COMMENT = 'Reporte gerencial consolidado de transacciones de puntos por banco y programa';
            
            INSERT INTO {snowflake_schema}.reporte_gerencial
            SELECT
                YEAR(TP.TRANSACTION_DATE_TIME) AS ANIO,
                B.NAME AS BANCO,
                P.NAME AS PROGRAMA,
                SUM(CASE WHEN TP.TYPE = 'CREDIT' THEN TP.POINTS ELSE 0 END) AS TOTAL_CREDITOS,
                SUM(CASE WHEN TP.TYPE = 'DEBIT' THEN TP.POINTS ELSE 0 END) AS TOTAL_DEBITOS,
                COUNT(DISTINCT TP.CARDHOLDER_ID) AS CARDHOLDERS_UNICOS,
                CURRENT_TIMESTAMP() AS CREATED_AT
            FROM
                GENNIUS_XYZ.COMPANY_BRONZE_LAYER.TARJETA_PUNTOS TP
                JOIN GENNIUS_XYZ.COMPANY_BRONZE_LAYER.BANCO B ON TP.BANK_ID = B.BANKID
                JOIN GENNIUS_XYZ.COMPANY_BRONZE_LAYER.PROGRAMA P ON TP.PROGRAM_ID = P.PROGRAMID
            GROUP BY
                ANIO, BANCO, PROGRAMA
            ORDER BY
                ANIO, BANCO, PROGRAMA;
        """
        
        return SQLExecuteQueryOperator(
            task_id='create_reporte_gerencial_table',
            conn_id=snowflake_conn_id,
            sql=create_table_sql
        ).execute(context={})

    @task
    def create_reporte_gerencial_supabase():
        """Create reporte_gerencial table in Supabase with the same query logic"""
        logger.info("Creating reporte_gerencial table in Supabase...")
        
        try:
            # Get Supabase connection
            supabase_hook = PostgresHook(postgres_conn_id='supabase_default')
            
            # Create table in Supabase with the same query logic (adapted for PostgreSQL)
            create_supabase_table_sql = """
                DROP TABLE IF EXISTS reporte_gerencial;
                CREATE TABLE reporte_gerencial (
                    ANIO INTEGER,
                    BANCO VARCHAR,
                    PROGRAMA VARCHAR,
                    TOTAL_CREDITOS INTEGER,
                    TOTAL_DEBITOS INTEGER,
                    CARDHOLDERS_UNICOS INTEGER,
                    CREATED_AT TIMESTAMP
                );
                
                COMMENT ON TABLE reporte_gerencial IS 'Reporte gerencial consolidado de transacciones de puntos por banco y programa';
                COMMENT ON COLUMN reporte_gerencial.ANIO IS 'Año de la transacción';
                COMMENT ON COLUMN reporte_gerencial.BANCO IS 'Nombre del banco emisor';
                COMMENT ON COLUMN reporte_gerencial.PROGRAMA IS 'Nombre del programa de puntos';
                COMMENT ON COLUMN reporte_gerencial.TOTAL_CREDITOS IS 'Total de puntos acumulados (créditos)';
                COMMENT ON COLUMN reporte_gerencial.TOTAL_DEBITOS IS 'Total de puntos redimidos (débitos)';
                COMMENT ON COLUMN reporte_gerencial.CARDHOLDERS_UNICOS IS 'Número de titulares únicos que participaron';
                COMMENT ON COLUMN reporte_gerencial.CREATED_AT IS 'Fecha y hora de creación del reporte';
                
                INSERT INTO reporte_gerencial
                SELECT
                    EXTRACT(YEAR FROM TP.TRANSACTION_DATE_TIME) AS ANIO,
                    B.NAME AS BANCO,
                    P.NAME AS PROGRAMA,
                    SUM(CASE WHEN TP.TYPE = 'CREDIT' THEN TP.POINTS ELSE 0 END) AS TOTAL_CREDITOS,
                    SUM(CASE WHEN TP.TYPE = 'DEBIT' THEN TP.POINTS ELSE 0 END) AS TOTAL_DEBITOS,
                    COUNT(DISTINCT TP.CARDHOLDER_ID) AS CARDHOLDERS_UNICOS,
                    CURRENT_TIMESTAMP AS CREATED_AT
                FROM
                    TARJETA_PUNTOS TP
                    JOIN BANCO B ON TP.BANK_ID = B.BANKID
                    JOIN PROGRAMA P ON TP.PROGRAM_ID = P.PROGRAMID
                GROUP BY
                    EXTRACT(YEAR FROM TP.TRANSACTION_DATE_TIME), B.NAME, P.NAME
                ORDER BY
                    EXTRACT(YEAR FROM TP.TRANSACTION_DATE_TIME), B.NAME, P.NAME;
            """
            
            supabase_hook.run(create_supabase_table_sql)
            logger.info("Created reporte_gerencial table in Supabase with data")
            
            return "Reporte gerencial table created in Supabase successfully"
            
        except Exception as e:
            logger.error(f"Error creating Supabase table: {e}")
            raise

    @task
    def silver_layer_complete():
        """Final task to mark completion"""
        logger.info("Silver layer pipeline completed successfully!")
        logger.info(f"Reporte gerencial table created in Snowflake schema: {snowflake_schema}")
        logger.info("Reporte gerencial table created in Supabase with same query logic")
        return "Silver layer pipeline completed"

    # Define task flow
    create_snowflake_table = create_reporte_gerencial_table()
    create_supabase_table = create_reporte_gerencial_supabase()
    completion = silver_layer_complete()

    # Set dependencies
    create_snowflake_table >> create_supabase_table >> completion

# Create the DAG instance
silver_layer_dag_instance = silver_layer_dag()
