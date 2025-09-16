#!/usr/bin/env python3
"""
Script para generar diccionario de datos de PostgreSQL (Supabase y RDS)
Extrae metadatos completos de tablas, columnas, constraints y relaciones
"""

import os
import sys
import json
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_postgres_connection(db_type):
    """Obtener conexión PostgreSQL usando psycopg2 directamente"""
    # Database configurations from environment variables
    if db_type == 'supabase':
        config = {
            'host': os.getenv('SUPABASE_HOST'),
            'database': os.getenv('SUPABASE_NAME'),
            'user': os.getenv('SUPABASE_USER'),
            'password': os.getenv('SUPABASE_PASSWORD'),
            'port': os.getenv('SUPABASE_PORT', '5432')
        }
    elif db_type == 'rds':
        config = {
            'host': os.getenv('RDS_HOST'),
            'database': os.getenv('RDS_NAME'),
            'user': os.getenv('RDS_USER'),
            'password': os.getenv('RDS_PASSWORD'),
            'port': os.getenv('RDS_PORT', '5432')
        }
    else:
        raise ValueError(f"Unknown database type: {db_type}")
    
    return psycopg2.connect(**config)

def get_table_metadata(conn, schema_name='public'):
    """Extraer metadatos de todas las tablas"""
    logger.info(f"Extrayendo metadatos de tablas en schema: {schema_name}")
    
    # Query para obtener información de tablas
    tables_query = """
    SELECT 
        t.table_name,
        obj_description(c.oid) as table_comment,
        pg_size_pretty(pg_total_relation_size(c.oid)) as table_size
    FROM information_schema.tables t
    LEFT JOIN pg_class c ON c.relname = t.table_name
    WHERE t.table_schema = %s
    AND t.table_type = 'BASE TABLE'
    ORDER BY t.table_name;
    """
    
    with conn.cursor() as cursor:
        cursor.execute(tables_query, (schema_name,))
        tables = cursor.fetchall()
    return tables

def get_column_metadata(conn, table_name, schema_name='public'):
    """Extraer metadatos de columnas de una tabla específica"""
    logger.info(f"Extrayendo metadatos de columnas para tabla: {table_name}")
    
    columns_query = """
    SELECT 
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.is_nullable,
        c.column_default,
        c.ordinal_position,
        col_description(pgc.oid, c.ordinal_position) as column_comment
    FROM information_schema.columns c
    LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
    WHERE c.table_schema = %s
    AND c.table_name = %s
    ORDER BY c.ordinal_position;
    """
    
    with conn.cursor() as cursor:
        cursor.execute(columns_query, (schema_name, table_name))
        columns = cursor.fetchall()
    return columns

def get_constraints_metadata(conn, table_name, schema_name='public'):
    """Extraer metadatos de constraints (PK, FK, UK, etc.)"""
    logger.info(f"Extrayendo constraints para tabla: {table_name}")
    
    constraints_query = """
    SELECT 
        tc.constraint_name,
        tc.constraint_type,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    LEFT JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE tc.table_schema = %s
    AND tc.table_name = %s
    ORDER BY tc.constraint_type, tc.constraint_name;
    """
    
    with conn.cursor() as cursor:
        cursor.execute(constraints_query, (schema_name, table_name))
        constraints = cursor.fetchall()
    return constraints

def get_foreign_key_relationships(conn, schema_name='public'):
    """Extraer todas las relaciones de claves foráneas"""
    logger.info(f"Extrayendo relaciones de claves foráneas en schema: {schema_name}")
    
    fk_query = """
    SELECT
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        tc.constraint_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = %s
    ORDER BY tc.table_name, kcu.column_name;
    """
    
    with conn.cursor() as cursor:
        cursor.execute(fk_query, (schema_name,))
        foreign_keys = cursor.fetchall()
    return foreign_keys

def generate_database_dictionary(db_type, db_name, schema_name='public'):
    """Generar diccionario completo para una base de datos"""
    logger.info(f"Generando diccionario para {db_name} (tipo: {db_type})")
    
    try:
        conn = get_postgres_connection(db_type)
        
        # Estructura del diccionario
        dictionary = {
            'database_info': {
                'name': db_name,
                'connection_type': db_type,
                'schema': schema_name,
                'generated_at': datetime.now().isoformat(),
                'generated_by': 'PostgreSQL Data Dictionary Generator'
            },
            'tables': {},
            'relationships': [],
            'statistics': {
                'total_tables': 0,
                'total_columns': 0,
                'total_constraints': 0
            }
        }
        
        # Obtener metadatos de tablas
        tables = get_table_metadata(conn, schema_name)
        
        for table_info in tables:
            table_name = table_info[0]
            table_comment = table_info[1] or "Sin descripción"
            table_size = table_info[2] or "Desconocido"
            
            logger.info(f"Procesando tabla: {table_name}")
            
            # Obtener columnas
            columns = get_column_metadata(conn, table_name, schema_name)
            
            # Obtener constraints
            constraints = get_constraints_metadata(conn, table_name, schema_name)
            
            # Estructura de la tabla
            table_dict = {
                'name': table_name,
                'description': table_comment,
                'size': table_size,
                'columns': {},
                'constraints': {
                    'primary_keys': [],
                    'foreign_keys': [],
                    'unique_keys': [],
                    'check_constraints': []
                }
            }
            
            # Procesar columnas
            for col_info in columns:
                col_name = col_info[0]
                data_type = col_info[1]
                max_length = col_info[2]
                is_nullable = col_info[3]
                default_value = col_info[4]
                position = col_info[5]
                comment = col_info[6] or "Sin descripción"
                
                # Formatear tipo de dato
                if max_length:
                    data_type = f"{data_type}({max_length})"
                
                table_dict['columns'][col_name] = {
                    'name': col_name,
                    'data_type': data_type,
                    'nullable': is_nullable == 'YES',
                    'default_value': default_value,
                    'position': position,
                    'description': comment
                }
            
            # Procesar constraints
            for constraint_info in constraints:
                constraint_name = constraint_info[0]
                constraint_type = constraint_info[1]
                column_name = constraint_info[2]
                foreign_table = constraint_info[3]
                foreign_column = constraint_info[4]
                
                constraint_dict = {
                    'name': constraint_name,
                    'column': column_name
                }
                
                if constraint_type == 'PRIMARY KEY':
                    table_dict['constraints']['primary_keys'].append(constraint_dict)
                elif constraint_type == 'FOREIGN KEY':
                    constraint_dict['references'] = {
                        'table': foreign_table,
                        'column': foreign_column
                    }
                    table_dict['constraints']['foreign_keys'].append(constraint_dict)
                elif constraint_type == 'UNIQUE':
                    table_dict['constraints']['unique_keys'].append(constraint_dict)
                elif constraint_type == 'CHECK':
                    table_dict['constraints']['check_constraints'].append(constraint_dict)
            
            dictionary['tables'][table_name] = table_dict
        
        # Obtener relaciones
        relationships = get_foreign_key_relationships(conn, schema_name)
        for rel in relationships:
            relationship_dict = {
                'from_table': rel[0],
                'from_column': rel[1],
                'to_table': rel[2],
                'to_column': rel[3],
                'constraint_name': rel[4]
            }
            dictionary['relationships'].append(relationship_dict)
        
        # Calcular estadísticas
        dictionary['statistics']['total_tables'] = len(dictionary['tables'])
        dictionary['statistics']['total_columns'] = sum(
            len(table['columns']) for table in dictionary['tables'].values()
        )
        dictionary['statistics']['total_constraints'] = sum(
            len(table['constraints']['primary_keys']) +
            len(table['constraints']['foreign_keys']) +
            len(table['constraints']['unique_keys']) +
            len(table['constraints']['check_constraints'])
            for table in dictionary['tables'].values()
        )
        
        conn.close()
        logger.info(f"Diccionario generado para {db_name}: {dictionary['statistics']['total_tables']} tablas, {dictionary['statistics']['total_columns']} columnas")
        
        return dictionary
        
    except Exception as e:
        logger.error(f"Error generando diccionario para {db_name}: {e}")
        raise

def save_dictionary_to_file(dictionary, output_path):
    """Guardar diccionario en archivo JSON"""
    logger.info(f"Guardando diccionario en: {output_path}")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dictionary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Diccionario guardado exitosamente en {output_path}")

def main():
    """Función principal"""
    logger.info("Iniciando generación de diccionarios de datos PostgreSQL")
    
    # Database types for processing
    databases = ['supabase', 'rds']
    
    # Directorio de salida - usar directorio temporal para evitar problemas de permisos
    output_dir = '/tmp/data_dictionaries'
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Generar diccionarios para todas las bases de datos
        dictionaries = {}
        for db_type in databases:
            db_name = db_type.upper()
            logger.info(f"Generando diccionario para {db_name}...")
            dictionaries[db_type] = generate_database_dictionary(db_type, db_name)
            save_dictionary_to_file(dictionaries[db_type], os.path.join(output_dir, f'{db_type}_dictionary.json'))
        
        # Generar diccionario unificado
        logger.info("Generando diccionario unificado...")
        unified_dict = {
            'generated_at': datetime.now().isoformat(),
            'generated_by': 'PostgreSQL Data Dictionary Generator',
            'databases': dictionaries,
            'summary': {
                'total_databases': len(databases),
                'total_tables': sum(dict['statistics']['total_tables'] for dict in dictionaries.values()),
                'total_columns': sum(dict['statistics']['total_columns'] for dict in dictionaries.values())
            }
        }
        
        save_dictionary_to_file(unified_dict, os.path.join(output_dir, 'postgres_unified_dictionary.json'))
        
        logger.info("Diccionarios PostgreSQL generados exitosamente")
        logger.info(f"Archivos creados en: {output_dir}")
        logger.info("- supabase_dictionary.json")
        logger.info("- rds_dictionary.json") 
        logger.info("- postgres_unified_dictionary.json")
        
    except Exception as e:
        logger.error(f"Error en la generación de diccionarios: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
