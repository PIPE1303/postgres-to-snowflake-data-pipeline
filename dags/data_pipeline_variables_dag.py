from airflow.decorators import dag, task, task_group
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
    description='Data pipeline with metadata using Airflow Variables for configuration',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['data', 'etl', 'postgresql', 'supabase', 'rds', 'variables', 'task_group', 'metadata'],
    dag_id='data_pipeline_variables_dag'
)
def data_pipeline_variables_dag():
    
    # Database configurations from Airflow Variables
    databases = [
        {
            'name': 'supabase',
            'display_name': 'SUPABASE',
            'data_dir': Variable.get('data_dir_supabase', default_var='/usr/local/airflow/data/supabase')
        },
        {
            'name': 'rds', 
            'display_name': 'RDS',
            'data_dir': Variable.get('data_dir_rds', default_var='/usr/local/airflow/data/rds')
        }
    ]
    
    @task_group(group_id='schema_creation')
    def create_schemas():
        """Task group for creating schemas in parallel"""
        
        @task
        def create_schema_task(db_config: dict):
            """Create tables and constraints for a specific database"""
            db_name = db_config['display_name']
            db_type = db_config['name']
            
            logger.info(f"Creating schema for {db_name}")
            try:
                # Use absolute paths for Airflow container - fixed paths
                project_root = '/usr/local/airflow'
                script_path = f'{project_root}/scripts/create_schema_with_metadata.py'
                
                logger.info(f"Creating schema for {db_name} using script: {script_path}")
                
                # Set environment variables for the script
                env_vars = {
                    "SUPABASE_HOST": Variable.get("supabase_host"),
                    "SUPABASE_NAME": Variable.get("supabase_name"),
                    "SUPABASE_USER": Variable.get("supabase_user"),
                    "SUPABASE_PASSWORD": Variable.get("supabase_password"),
                    "SUPABASE_PORT": Variable.get("supabase_port"),
                    "RDS_HOST": Variable.get("rds_host"),
                    "RDS_NAME": Variable.get("rds_name"),
                    "RDS_USER": Variable.get("rds_user"),
                    "RDS_PASSWORD": Variable.get("rds_password"),
                    "RDS_PORT": Variable.get("rds_port")
                }
                
                result = subprocess.run([
                    'python', 
                    script_path,
                    db_type
                ], check=True, capture_output=True, text=True, cwd=project_root, env={**os.environ, **env_vars})
                
                logger.info(f"{db_name} schema creation output: {result.stdout}")
                if result.stderr:
                    logger.error(f"{db_name} schema creation errors: {result.stderr}")
                
                return f"{db_name} schema created successfully"
                
            except subprocess.CalledProcessError as e:
                logger.error(f"Error creating {db_name} schema: {e.stderr}")
                raise e
        
        # Use expand to create tasks for each database
        return create_schema_task.expand(db_config=databases)
    
    @task_group(group_id='data_loading')
    def load_data_parallel():
        """Task group for loading data to each database in parallel"""
        
        @task
        def load_data_task(db_config: dict):
            """Load data to a specific database"""
            db_name = db_config['display_name']
            db_type = db_config['name']
            data_dir = db_config['data_dir']
            
            logger.info(f"Loading data to {db_name} from {data_dir}")
            try:
                # Use absolute paths for Airflow container - fixed paths
                project_root = '/usr/local/airflow'
                script_path = f'{project_root}/scripts/load_data.py'
                
                logger.info(f"Loading data to {db_name} using script: {script_path}")
                
                # Create a temporary script that loads only to specific database
                # Set environment variables for the script
                env_vars = {
                    "SUPABASE_HOST": Variable.get("supabase_host"),
                    "SUPABASE_NAME": Variable.get("supabase_name"),
                    "SUPABASE_USER": Variable.get("supabase_user"),
                    "SUPABASE_PASSWORD": Variable.get("supabase_password"),
                    "SUPABASE_PORT": Variable.get("supabase_port"),
                    "RDS_HOST": Variable.get("rds_host"),
                    "RDS_NAME": Variable.get("rds_name"),
                    "RDS_USER": Variable.get("rds_user"),
                    "RDS_PASSWORD": Variable.get("rds_password"),
                    "RDS_PORT": Variable.get("rds_port")
                }
                
                result = subprocess.run([
                    'python', 
                    f'{project_root}/scripts/load_data.py',
                    db_type
                ], check=True, capture_output=True, text=True, cwd=project_root, env={**os.environ, **env_vars})
                
                logger.info(f"{db_name} data load output: {result.stdout}")
                if result.stderr:
                    logger.error(f"{db_name} data load errors: {result.stderr}")
                
                return f"Data loaded successfully to {db_name}"
                
            except subprocess.CalledProcessError as e:
                logger.error(f"Error loading data to {db_name}: {e.stderr}")
                raise e
        
        # Use expand to create tasks for each database
        return load_data_task.expand(db_config=databases)
    
    @task
    def pipeline_complete():
        """Final task to mark pipeline completion"""
        logger.info("Data pipeline with metadata completed successfully!")
        logger.info("All databases now have:")
        logger.info("- Tables with comprehensive metadata (comments)")
        logger.info("- Column descriptions explaining purpose and usage")
        logger.info("- Table descriptions explaining business context")
        logger.info("- Foreign key constraint descriptions")
        logger.info("- Sample data loaded for testing")
        return "Pipeline with metadata completed"
    
    # Define task flow
    schema_tasks = create_schemas()
    data_tasks = load_data_parallel()
    completion = pipeline_complete()
    
    # Set dependencies: schemas first, then data loading, then completion
    schema_tasks >> data_tasks >> completion

# Create the DAG instance
data_pipeline_variables_dag_instance = data_pipeline_variables_dag()
