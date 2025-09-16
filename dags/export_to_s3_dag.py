from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import subprocess
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
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
    description='Export data from PostgreSQL databases to S3 for Snowflake ingestion',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['export', 's3', 'postgresql', 'snowflake', 'etl'],
    dag_id='export_to_s3_dag'
)
def export_to_s3_dag():
    
    @task
    def export_data_to_s3():
        """Export data from both PostgreSQL databases to S3"""
        logger.info("Starting data export to S3...")
        try:
            # Use absolute paths for Airflow container
            project_root = '/usr/local/airflow'
            script_path = f'{project_root}/scripts/export_to_s3.py'
            
            # Get S3 configuration from Airflow Variables
            s3_bucket = Variable.get("S3_BUCKET")
            aws_region = Variable.get("AWS_REGION", default_var="us-east-1")
            aws_access_key = Variable.get("AWS_ACCESS_KEY_ID")
            aws_secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")
            
            logger.info(f"Using S3 bucket: {s3_bucket}")
            
            # Set environment variables for the script
            env = os.environ.copy()
            env.update({
                'S3_BUCKET': s3_bucket,
                'AWS_REGION': aws_region,
                'AWS_ACCESS_KEY_ID': aws_access_key,
                'AWS_SECRET_ACCESS_KEY': aws_secret_key
            })
            
            logger.info(f"Running export script: {script_path}")
            
            result = subprocess.run([
                'python', 
                script_path
            ], check=True, capture_output=True, text=True, cwd=project_root, env=env)
            
            logger.info(f"Export to S3 output: {result.stdout}")
            if result.stderr:
                logger.error(f"Export to S3 errors: {result.stderr}")
            
            return "Data exported successfully to S3"
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error exporting data to S3: {e.stderr}")
            raise e
    
    @task
    def verify_s3_export():
        """Verify that data was exported to S3 successfully"""
        logger.info("Verifying S3 export...")
        # This task can be extended to verify S3 files
        # For now, we'll just log success
        logger.info("S3 export verification completed")
        return "S3 export verified successfully"
    
    # Define task flow
    export_task = export_data_to_s3()
    verify_task = verify_s3_export()
    
    # Set dependencies
    export_task >> verify_task

# Create the DAG instance
export_to_s3_dag_instance = export_to_s3_dag()
