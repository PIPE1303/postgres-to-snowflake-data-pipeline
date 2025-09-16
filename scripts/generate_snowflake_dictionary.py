#!/usr/bin/env python3
"""
Script para generar diccionario de datos de Snowflake (Bronze y Silver layers)
Extrae metadatos completos de tablas, columnas, stages y warehouses
"""

import os
import sys
import json
import logging
from datetime import datetime
import snowflake.connector
from snowflake.connector import DictCursor

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Obtener conexión Snowflake usando snowflake.connector con credenciales de Airflow"""
    # Obtener credenciales desde variables de entorno (pasadas por el DAG)
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'role': os.getenv('SNOWFLAKE_ROLE')
    }
    
    # Log the configuration being used
    logger.info(f"Snowflake connection config keys: {list(config.keys())}")
    logger.info(f"SNOWFLAKE_ACCOUNT: {config.get('account', 'NOT_SET')}")
    logger.info(f"SNOWFLAKE_USER: {config.get('user', 'NOT_SET')}")
    logger.info(f"SNOWFLAKE_WAREHOUSE: {config.get('warehouse', 'NOT_SET')}")
    logger.info(f"SNOWFLAKE_DATABASE: {config.get('database', 'NOT_SET')}")
    logger.info(f"SNOWFLAKE_SCHEMA: {config.get('schema', 'NOT_SET')}")
    logger.info(f"SNOWFLAKE_ROLE: {config.get('role', 'NOT_SET')}")
    
    # Filtrar valores None
    config = {k: v for k, v in config.items() if v is not None}
    
    logger.info(f"Final config for connection: {list(config.keys())}")
    
    return snowflake.connector.connect(**config)

def get_database_metadata(conn):
    """Extraer metadatos de bases de datos disponibles"""
    logger.info("Extrayendo metadatos de bases de datos Snowflake")
    
    databases_query = """
    SHOW DATABASES;
    """
    
    try:
        with conn.cursor(DictCursor) as cursor:
            logger.info("Executing SHOW DATABASES query...")
            cursor.execute(databases_query)
            databases = cursor.fetchall()
            logger.info(f"Databases query executed successfully, got {len(databases)} results")
        return databases
    except Exception as e:
        logger.error(f"Error in get_database_metadata: {e}")
        raise

def get_schema_metadata(conn, database_name):
    """Extraer metadatos de schemas en una base de datos"""
    logger.info(f"Extrayendo metadatos de schemas en database: {database_name}")
    
    schemas_query = f"""
    SHOW SCHEMAS IN DATABASE {database_name};
    """
    
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(schemas_query)
        schemas = cursor.fetchall()
    return schemas

def get_table_metadata(conn, database_name, schema_name):
    """Extraer metadatos de tablas en un schema"""
    logger.info(f"Extrayendo metadatos de tablas en {database_name}.{schema_name}")
    
    tables_query = f"""
    SHOW TABLES IN SCHEMA {database_name}.{schema_name};
    """
    
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(tables_query)
        tables = cursor.fetchall()
    return tables

def get_column_metadata(conn, database_name, schema_name, table_name):
    """Extraer metadatos de columnas de una tabla específica"""
    logger.info(f"Extrayendo metadatos de columnas para {database_name}.{schema_name}.{table_name}")
    
    columns_query = f"""
    DESCRIBE TABLE {database_name}.{schema_name}.{table_name};
    """
    
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(columns_query)
        columns = cursor.fetchall()
    return columns

def get_table_info(conn, database_name, schema_name, table_name):
    """Obtener información adicional de la tabla"""
    logger.info(f"Obteniendo información adicional para {database_name}.{schema_name}.{table_name}")
    
    info_query = f"""
    SELECT 
        TABLE_CATALOG,
        TABLE_SCHEMA,
        TABLE_NAME,
        TABLE_TYPE,
        ROW_COUNT,
        BYTES,
        COMMENT
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_CATALOG = '{database_name}'
    AND TABLE_SCHEMA = '{schema_name}'
    AND TABLE_NAME = '{table_name}';
    """
    
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(info_query)
        info = cursor.fetchall()
    return info[0] if info else None

def get_stage_metadata(conn, database_name, schema_name):
    """Extraer metadatos de stages en un schema"""
    logger.info(f"Extrayendo metadatos de stages en {database_name}.{schema_name}")
    
    stages_query = f"""
    SHOW STAGES IN SCHEMA {database_name}.{schema_name};
    """
    
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(stages_query)
        stages = cursor.fetchall()
    return stages

def get_warehouse_metadata(conn):
    """Extraer metadatos de warehouses"""
    logger.info("Extrayendo metadatos de warehouses")
    
    warehouses_query = """
    SHOW WAREHOUSES;
    """
    
    try:
        with conn.cursor(DictCursor) as cursor:
            logger.info("Executing SHOW WAREHOUSES query...")
            cursor.execute(warehouses_query)
            warehouses = cursor.fetchall()
            logger.info(f"Warehouses query executed successfully, got {len(warehouses)} results")
        return warehouses
    except Exception as e:
        logger.error(f"Error in get_warehouse_metadata: {e}")
        raise

def generate_snowflake_dictionary():
    """Generar diccionario completo para Snowflake"""
    logger.info("Generando diccionario para Snowflake")
    
    try:
        conn = get_snowflake_connection()
        
        # Estructura del diccionario
        dictionary = {
            'snowflake_info': {
                'generated_at': datetime.now().isoformat(),
                'generated_by': 'Snowflake Data Dictionary Generator'
            },
            'warehouses': {},
            'databases': {},
            'statistics': {
                'total_warehouses': 0,
                'total_databases': 0,
                'total_schemas': 0,
                'total_tables': 0,
                'total_columns': 0,
                'total_stages': 0
            }
        }
        
        # Obtener warehouses
        logger.info("Starting warehouse metadata extraction...")
        try:
            warehouses = get_warehouse_metadata(conn)
            logger.info(f"Successfully retrieved {len(warehouses)} warehouses")
        except Exception as e:
            logger.error(f"Error getting warehouse metadata: {e}")
            raise
        
        for i, warehouse in enumerate(warehouses):
            try:
                logger.info(f"Processing warehouse {i+1}/{len(warehouses)}")
                warehouse_name = warehouse['name']
                logger.info(f"Warehouse name: {warehouse_name}")
                
                dictionary['warehouses'][warehouse_name] = {
                    'name': warehouse['name'],
                    'state': warehouse['state'],
                    'size': warehouse['size'],
                    'min_cluster_count': warehouse['min_cluster_count'],
                    'max_cluster_count': warehouse['max_cluster_count'],
                    'started_clusters': warehouse['started_clusters'],
                    'running': warehouse['running'],
                    'queued': warehouse['queued'],
                    'is_default': warehouse['is_default'],
                    'is_current': warehouse['is_current'],
                    'auto_suspend': warehouse['auto_suspend'],
                    'auto_resume': warehouse['auto_resume'],
                    'available': warehouse['available'],
                    'provisioning': warehouse['provisioning'],
                    'quiescing': warehouse['quiescing'],
                    'other': warehouse['other'],
                    'created_on': warehouse['created_on'],
                    'resumed_on': warehouse['resumed_on'],
                    'updated_on': warehouse['updated_on'],
                    'owner': warehouse['owner'],
                    'comment': warehouse['comment'],
                    'enable_query_acceleration': warehouse['enable_query_acceleration'],
                    'query_acceleration_max_scale_factor': warehouse['query_acceleration_max_scale_factor']
                }
                logger.info(f"Successfully processed warehouse: {warehouse_name}")
            except Exception as e:
                logger.error(f"Error processing warehouse {i+1}: {e}")
                logger.error(f"Warehouse data: {warehouse}")
                raise
        
        # Obtener databases
        logger.info("Starting database metadata extraction...")
        try:
            databases = get_database_metadata(conn)
            logger.info(f"Successfully retrieved {len(databases)} databases")
        except Exception as e:
            logger.error(f"Error getting database metadata: {e}")
            raise
        
        for database in databases:
            database_name = database['name']
            logger.info(f"Procesando database: {database_name}")
            
            dictionary['databases'][database_name] = {
                'name': database['name'],
                'created_on': database['created_on'],
                'is_default': database['is_default'],
                'is_current': database['is_current'],
                'origin': database['origin'],
                'owner': database['owner'],
                'comment': database['comment'],
                'options': database['options'],
                'retention_time': database['retention_time'],
                'schemas': {}
            }
            
            # Obtener schemas
            logger.info(f"Getting schemas for database: {database_name}")
            try:
                schemas = get_schema_metadata(conn, database_name)
                logger.info(f"Successfully retrieved {len(schemas)} schemas for {database_name}")
            except Exception as e:
                logger.error(f"Error getting schemas for {database_name}: {e}")
                continue
            
            for schema in schemas:
                schema_name = schema['name']
                logger.info(f"Procesando schema: {database_name}.{schema_name}")
                
                dictionary['databases'][database_name]['schemas'][schema_name] = {
                    'name': schema['name'],
                    'created_on': schema['created_on'],
                    'is_default': schema['is_default'],
                    'is_current': schema['is_current'],
                    'database_name': schema['database_name'],
                    'owner': schema['owner'],
                    'comment': schema['comment'],
                    'options': schema['options'],
                    'retention_time': schema['retention_time'],
                    'tables': {},
                    'stages': {}
                }
                
                # Obtener tablas
                logger.info(f"Getting tables for schema: {database_name}.{schema_name}")
                try:
                    tables = get_table_metadata(conn, database_name, schema_name)
                    logger.info(f"Successfully retrieved {len(tables)} tables for {database_name}.{schema_name}")
                except Exception as e:
                    logger.error(f"Error getting tables for {database_name}.{schema_name}: {e}")
                    continue
                
                for table in tables:
                    table_name = table['name']
                    logger.info(f"Procesando tabla: {database_name}.{schema_name}.{table_name}")
                    
                    # Obtener información adicional de la tabla
                    table_info = get_table_info(conn, database_name, schema_name, table_name)
                    
                    table_dict = {
                        'name': table.get('name', ''),
                        'kind': table.get('kind', ''),
                        'database_name': table.get('database_name', ''),
                        'schema_name': table.get('schema_name', ''),
                        'created_on': table.get('created_on', ''),
                        'cluster_by': table.get('cluster_by', ''),
                        'rows': table.get('rows', 0),
                        'bytes': table.get('bytes', 0),
                        'owner': table.get('owner', ''),
                        'retention_time': table.get('retention_time', ''),
                        'dropped_on': table.get('dropped_on', ''),
                        'automatic_clustering': table.get('automatic_clustering', ''),
                        'change_tracking': table.get('change_tracking', ''),
                        'search_optimization': table.get('search_optimization', ''),
                        'search_optimization_progress': table.get('search_optimization_progress', ''),
                        'search_optimization_bytes': table.get('search_optimization_bytes', 0),
                        'is_external': table.get('is_external', ''),
                        'columns': {}
                    }
                    
                    # Agregar información adicional si está disponible
                    if table_info:
                        table_dict.update({
                            'table_catalog': table_info.get('TABLE_CATALOG', ''),
                            'table_type': table_info.get('TABLE_TYPE', ''),
                            'row_count': table_info.get('ROW_COUNT', 0),
                            'bytes_size': table_info.get('BYTES', 0),
                            'comment': table_info.get('COMMENT', '')
                        })
                    
                    # Obtener columnas
                    columns = get_column_metadata(conn, database_name, schema_name, table_name)
                    
                    for column in columns:
                        column_name = column.get('name', '')
                        column_dict = {
                            'name': column_name,
                            'type': column.get('type', ''),
                            'kind': column.get('kind', ''),
                            'null?': column.get('null?', ''),
                            'default': column.get('default', ''),
                            'primary_key': column.get('primary_key', ''),
                            'unique_key': column.get('unique_key', ''),
                            'check': column.get('check', ''),
                            'expression': column.get('expression', ''),
                            'comment': column.get('comment', ''),
                            'policy_name': column.get('policy_name', '')
                        }
                        table_dict['columns'][column_name] = column_dict
                    
                    dictionary['databases'][database_name]['schemas'][schema_name]['tables'][table_name] = table_dict
                
                # Obtener stages
                stages = get_stage_metadata(conn, database_name, schema_name)
                
                for stage in stages:
                    stage_name = stage.get('name', '')
                    stage_dict = {
                        'name': stage_name,
                        'database_name': stage.get('database_name', ''),
                        'schema_name': stage.get('schema_name', ''),
                        'url': stage.get('url', ''),
                        'has_credentials': stage.get('has_credentials', ''),
                        'has_encryption_key': stage.get('has_encryption_key', ''),
                        'type': stage.get('type', ''),
                        'cloud': stage.get('cloud', ''),
                        'region': stage.get('region', ''),
                        'notification_channel': stage.get('notification_channel', ''),
                        'storage_integration': stage.get('storage_integration', ''),
                        'api_integration': stage.get('api_integration', ''),
                        'created_on': stage.get('created_on', ''),
                        'owner': stage.get('owner', ''),
                        'comment': stage.get('comment', '')
                    }
                    dictionary['databases'][database_name]['schemas'][schema_name]['stages'][stage_name] = stage_dict
        
        # Calcular estadísticas
        dictionary['statistics']['total_warehouses'] = len(dictionary['warehouses'])
        dictionary['statistics']['total_databases'] = len(dictionary['databases'])
        
        total_schemas = 0
        total_tables = 0
        total_columns = 0
        total_stages = 0
        
        for db_name, db_info in dictionary['databases'].items():
            total_schemas += len(db_info['schemas'])
            for schema_name, schema_info in db_info['schemas'].items():
                total_tables += len(schema_info['tables'])
                total_stages += len(schema_info['stages'])
                for table_name, table_info in schema_info['tables'].items():
                    total_columns += len(table_info['columns'])
        
        dictionary['statistics'].update({
            'total_schemas': total_schemas,
            'total_tables': total_tables,
            'total_columns': total_columns,
            'total_stages': total_stages
        })
        
        logger.info(f"Diccionario Snowflake generado: {dictionary['statistics']['total_databases']} databases, {dictionary['statistics']['total_tables']} tablas, {dictionary['statistics']['total_columns']} columnas")
        
        conn.close()
        return dictionary
        
    except Exception as e:
        logger.error(f"Error generando diccionario Snowflake: {e}")
        raise

def convert_datetime_to_string(obj):
    """Convertir objetos datetime a string para serialización JSON"""
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_datetime_to_string(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_string(item) for item in obj]
    else:
        return obj

def save_dictionary_to_file(dictionary, output_path):
    """Guardar diccionario en archivo JSON"""
    logger.info(f"Guardando diccionario Snowflake en: {output_path}")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Convertir objetos datetime a string
    dictionary_serializable = convert_datetime_to_string(dictionary)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dictionary_serializable, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Diccionario Snowflake guardado exitosamente en {output_path}")

def main():
    """Función principal"""
    logger.info("Iniciando generación de diccionario de datos Snowflake")
    
    # Directorio de salida - usar directorio temporal para evitar problemas de permisos
    output_dir = '/tmp/data_dictionaries'
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Generar diccionario para Snowflake
        logger.info("Generando diccionario para Snowflake...")
        snowflake_dict = generate_snowflake_dictionary()
        save_dictionary_to_file(snowflake_dict, os.path.join(output_dir, 'snowflake_dictionary.json'))
        
        logger.info("Diccionario Snowflake generado exitosamente")
        logger.info(f"Archivo creado en: {output_dir}/snowflake_dictionary.json")
        logger.info(f"Estadísticas:")
        logger.info(f"- Warehouses: {snowflake_dict['statistics']['total_warehouses']}")
        logger.info(f"- Databases: {snowflake_dict['statistics']['total_databases']}")
        logger.info(f"- Schemas: {snowflake_dict['statistics']['total_schemas']}")
        logger.info(f"- Tables: {snowflake_dict['statistics']['total_tables']}")
        logger.info(f"- Columns: {snowflake_dict['statistics']['total_columns']}")
        logger.info(f"- Stages: {snowflake_dict['statistics']['total_stages']}")
        
    except Exception as e:
        logger.error(f"Error en la generación del diccionario Snowflake: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
