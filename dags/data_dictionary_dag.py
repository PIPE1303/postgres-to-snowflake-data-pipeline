from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import logging
import subprocess
import os

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
    description='Generate comprehensive data dictionaries for PostgreSQL and Snowflake',
    schedule='@weekly',  # Ejecutar semanalmente
    catchup=False,
    tags=['documentation', 'data-dictionary', 'metadata', 'postgresql', 'snowflake'],
    dag_id='data_dictionary_dag'
)
def data_dictionary_dag():
    
    @task
    def generate_postgres_dictionary():
        """Generate data dictionary for PostgreSQL databases (Supabase and RDS)"""
        logger.info("Starting PostgreSQL data dictionary generation...")
        project_root = '/usr/local/airflow'
        script_path = f'{project_root}/scripts/generate_postgres_dictionary.py'
        
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
        
        try:
            result = subprocess.run([
                'python', 
                script_path
            ], check=True, capture_output=True, text=True, cwd=project_root, env={**os.environ, **env_vars})
            
            logger.info("PostgreSQL dictionary generation completed successfully")
            logger.info(f"Script output: {result.stdout}")
            return "PostgreSQL data dictionary generated successfully"
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating PostgreSQL dictionary: {e}")
            logger.error(f"Error output: {e.stderr}")
            raise

    @task
    def generate_snowflake_dictionary():
        """Generate data dictionary for Snowflake"""
        logger.info("Starting Snowflake data dictionary generation...")
        project_root = '/usr/local/airflow'
        script_path = f'{project_root}/scripts/generate_snowflake_dictionary.py'
        
        # Get Snowflake connection credentials
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Get connection details from the hook
        conn_details = snowflake_hook.get_connection('snowflake_default')
        
        # Extract connection parameters with sensible defaults
        env_vars = {
            "SNOWFLAKE_ACCOUNT": conn_details.extra_dejson.get('account', ''),
            "SNOWFLAKE_USER": conn_details.login or '',
            "SNOWFLAKE_PASSWORD": conn_details.password or '',
            "SNOWFLAKE_WAREHOUSE": conn_details.extra_dejson.get('warehouse', 'COMPUTE_WH'),
            "SNOWFLAKE_DATABASE": conn_details.extra_dejson.get('database', 'GENNIUS_XYZ'),
            "SNOWFLAKE_SCHEMA": conn_details.extra_dejson.get('schema', 'COMPANY_BRONZE_LAYER'),
            "SNOWFLAKE_ROLE": conn_details.extra_dejson.get('role', 'AIRFLOW_ROLE')
        }
        
        # Filter out None values and empty strings
        env_vars = {k: v for k, v in env_vars.items() if v is not None and v != ''}
        
        # Log the environment variables being passed (without sensitive data)
        logger.info(f"Snowflake connection parameters: {list(env_vars.keys())}")
        logger.info(f"SNOWFLAKE_ACCOUNT: {env_vars.get('SNOWFLAKE_ACCOUNT', 'NOT_SET')}")
        logger.info(f"SNOWFLAKE_USER: {env_vars.get('SNOWFLAKE_USER', 'NOT_SET')}")
        logger.info(f"SNOWFLAKE_WAREHOUSE: {env_vars.get('SNOWFLAKE_WAREHOUSE', 'NOT_SET')}")
        logger.info(f"SNOWFLAKE_DATABASE: {env_vars.get('SNOWFLAKE_DATABASE', 'NOT_SET')}")
        logger.info(f"SNOWFLAKE_SCHEMA: {env_vars.get('SNOWFLAKE_SCHEMA', 'NOT_SET')}")
        logger.info(f"SNOWFLAKE_ROLE: {env_vars.get('SNOWFLAKE_ROLE', 'NOT_SET')}")
        
        try:
            result = subprocess.run([
                'python', 
                script_path
            ], check=True, capture_output=True, text=True, cwd=project_root, env={**os.environ, **env_vars})
            
            logger.info("Snowflake dictionary generation completed successfully")
            logger.info(f"Script output: {result.stdout}")
            return "Snowflake data dictionary generated successfully"
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating Snowflake dictionary: {e}")
            logger.error(f"Error output: {e.stderr}")
            raise

    @task
    def generate_markdown_documentation():
        """Generate comprehensive Markdown documentation from JSON dictionaries"""
        logger.info("Starting Markdown documentation generation...")
        project_root = '/usr/local/airflow'
        script_path = f'{project_root}/scripts/generate_markdown_docs.py'
        
        try:
            result = subprocess.run([
                'python', 
                script_path
            ], check=True, capture_output=True, text=True, cwd=project_root, env=os.environ)
            
            logger.info("Markdown documentation generation completed successfully")
            logger.info(f"Script output: {result.stdout}")
            return "Markdown documentation generated successfully"
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating Markdown documentation: {e}")
            logger.error(f"Error output: {e.stderr}")
            raise

    @task
    def validate_dictionaries():
        """Validate that all dictionary files were generated correctly"""
        logger.info("Validating generated dictionaries...")
        
        docs_dir = '/tmp/data_dictionaries'
        required_files = [
            'supabase_dictionary.json',
            'rds_dictionary.json', 
            'postgres_unified_dictionary.json',
            'snowflake_dictionary.json',
            'data_dictionary_summary.md'
        ]
        
        missing_files = []
        for file_name in required_files:
            file_path = os.path.join(docs_dir, file_name)
            if not os.path.exists(file_path):
                missing_files.append(file_name)
        
        if missing_files:
            error_msg = f"Missing dictionary files: {', '.join(missing_files)}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        logger.info("All dictionary files validated successfully")
        return "Dictionary validation completed successfully"

    @task
    def upload_dictionaries_to_s3():
        """Upload generated dictionaries to S3 in metadata folder"""
        logger.info("Starting upload of dictionaries to S3...")
        
        # Get S3 configuration
        s3_bucket = Variable.get("S3_BUCKET")
        local_dir = '/tmp/data_dictionaries'
        
        # Create S3 prefix with date for organization
        current_date = datetime.now().strftime('%Y/%m/%d')
        s3_prefix = f"metadata/data-dictionaries/{current_date}"
        
        # Files to upload
        files_to_upload = [
            'data_dictionary_summary.md',
            'postgres_unified_dictionary.json',
            'rds_dictionary.json',
            'rds_dictionary.md',
            'snowflake_dictionary.json',
            'snowflake_dictionary.md',
            'supabase_dictionary.json',
            'supabase_dictionary.md'
        ]
        
        # Initialize S3 hook
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        uploaded_count = 0
        failed_count = 0
        
        for filename in files_to_upload:
            local_file_path = os.path.join(local_dir, filename)
            
            if os.path.exists(local_file_path):
                s3_key = f"{s3_prefix}/{filename}"
                
                try:
                    logger.info(f"Uploading {filename} to s3://{s3_bucket}/{s3_key}")
                    
                    # Upload file to S3
                    s3_hook.load_file(
                        filename=local_file_path,
                        key=s3_key,
                        bucket_name=s3_bucket,
                        replace=True
                    )
                    
                    logger.info(f"Successfully uploaded: {filename}")
                    uploaded_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to upload {filename}: {e}")
                    failed_count += 1
            else:
                logger.warning(f"File not found: {local_file_path}")
                failed_count += 1
        
        # Summary
        logger.info(f"Upload Summary:")
        logger.info(f"Files uploaded successfully: {uploaded_count}")
        logger.info(f"Files failed: {failed_count}")
        logger.info(f"S3 Location: s3://{s3_bucket}/{s3_prefix}/")
        
        if failed_count == 0:
            logger.info("All dictionaries uploaded successfully to S3")
            return f"All dictionaries uploaded to s3://{s3_bucket}/{s3_prefix}/"
        else:
            error_msg = f"Upload completed with {failed_count} failures"
            logger.warning(error_msg)
            return error_msg

    @task
    def cleanup_temp_files():
        """Clean up temporary files after successful upload to S3"""
        logger.info("Starting cleanup of temporary files...")
        
        local_dir = '/tmp/data_dictionaries'
        files_to_cleanup = [
            'data_dictionary_summary.md',
            'postgres_unified_dictionary.json',
            'rds_dictionary.json',
            'rds_dictionary.md',
            'snowflake_dictionary.json',
            'snowflake_dictionary.md',
            'supabase_dictionary.json',
            'supabase_dictionary.md'
        ]
        
        cleaned_count = 0
        failed_count = 0
        
        for filename in files_to_cleanup:
            file_path = os.path.join(local_dir, filename)
            
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"Cleaned up: {filename}")
                    cleaned_count += 1
                except Exception as e:
                    logger.error(f"Failed to clean up {filename}: {e}")
                    failed_count += 1
            else:
                logger.warning(f"File not found for cleanup: {filename}")
        
        # Remove the directory if it's empty
        try:
            if os.path.exists(local_dir) and not os.listdir(local_dir):
                os.rmdir(local_dir)
                logger.info(f"Removed empty directory: {local_dir}")
        except Exception as e:
            logger.warning(f"Could not remove directory {local_dir}: {e}")
        
        logger.info(f"Cleanup Summary:")
        logger.info(f"Files cleaned up: {cleaned_count}")
        logger.info(f"Files failed to clean: {failed_count}")
        
        if failed_count == 0:
            logger.info("All temporary files cleaned up successfully")
            return f"Cleanup completed: {cleaned_count} files removed"
        else:
            logger.warning(f"Cleanup completed with {failed_count} failures")
            return f"Cleanup completed with {failed_count} failures"

    @task
    def data_dictionary_complete():
        """Final task to mark completion"""
        logger.info("Data dictionary generation pipeline completed successfully!")
        logger.info("Generated files:")
        logger.info("- PostgreSQL dictionaries (Supabase, RDS, Unified)")
        logger.info("- Snowflake dictionary (Bronze and Silver layers)")
        logger.info("- Markdown documentation")
        logger.info("Files have been uploaded to S3 in the metadata folder")
        logger.info("Temporary files have been cleaned up")
        return "Data dictionary pipeline completed"

    # Define task flow
    postgres_dict = generate_postgres_dictionary()
    snowflake_dict = generate_snowflake_dictionary()
    markdown_docs = generate_markdown_documentation()
    validation = validate_dictionaries()
    upload_to_s3 = upload_dictionaries_to_s3()
    cleanup = cleanup_temp_files()
    completion = data_dictionary_complete()

    # Set dependencies - run in parallel then validate, upload to S3, cleanup, and complete
    [postgres_dict, snowflake_dict] >> markdown_docs >> validation >> upload_to_s3 >> cleanup >> completion

# Create the DAG instance
data_dictionary_dag_instance = data_dictionary_dag()
