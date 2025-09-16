#!/usr/bin/env python3
"""
Script para generar documentación en Markdown a partir de los diccionarios JSON
Crea documentación completa y estructurada para PostgreSQL y Snowflake
"""

import os
import sys
import json
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_json_dictionary(file_path):
    """Cargar diccionario desde archivo JSON"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error cargando {file_path}: {e}")
        return None

def generate_postgres_table_doc(table_name, table_info):
    """Generar documentación para una tabla PostgreSQL"""
    doc = f"### Tabla: `{table_name}`\n\n"
    
    if table_info.get('description'):
        doc += f"**Descripción:** {table_info['description']}\n\n"
    
    if table_info.get('size'):
        doc += f"**Tamaño:** {table_info['size']}\n\n"
    
    # Columnas
    doc += "#### Columnas\n\n"
    doc += "| Nombre | Tipo | Nulo | Valor por Defecto | Posición | Descripción |\n"
    doc += "|--------|------|------|-------------------|----------|-------------|\n"
    
    for col_name, col_info in table_info['columns'].items():
        nullable = "Sí" if col_info['nullable'] else "No"
        default = col_info.get('default_value', '') or '-'
        position = col_info.get('position', '')
        description = col_info.get('description', 'Sin descripción')
        
        doc += f"| `{col_name}` | `{col_info['data_type']}` | {nullable} | {default} | {position} | {description} |\n"
    
    doc += "\n"
    
    # Constraints
    constraints = table_info.get('constraints', {})
    
    if constraints.get('primary_keys'):
        doc += "#### Claves Primarias\n\n"
        for pk in constraints['primary_keys']:
            doc += f"- `{pk['column']}`\n"
        doc += "\n"
    
    if constraints.get('foreign_keys'):
        doc += "#### Claves Foráneas\n\n"
        for fk in constraints['foreign_keys']:
            ref_table = fk['references']['table']
            ref_column = fk['references']['column']
            doc += f"- `{fk['column']}` → `{ref_table}.{ref_column}`\n"
        doc += "\n"
    
    if constraints.get('unique_keys'):
        doc += "#### Claves Únicas\n\n"
        for uk in constraints['unique_keys']:
            doc += f"- `{uk['column']}`\n"
        doc += "\n"
    
    return doc

def generate_snowflake_table_doc(table_name, table_info):
    """Generar documentación para una tabla Snowflake"""
    doc = f"### Tabla: `{table_name}`\n\n"
    
    if table_info.get('comment'):
        doc += f"**Descripción:** {table_info['comment']}\n\n"
    
    if table_info.get('row_count'):
        doc += f"**Filas:** {table_info['row_count']:,}\n\n"
    
    if table_info.get('bytes_size'):
        doc += f"**Tamaño:** {table_info['bytes_size']:,} bytes\n\n"
    
    if table_info.get('created_on'):
        doc += f"**Creada:** {table_info['created_on']}\n\n"
    
    if table_info.get('owner'):
        doc += f"**Propietario:** {table_info['owner']}\n\n"
    
    # Columnas
    doc += "#### Columnas\n\n"
    doc += "| Nombre | Tipo | Nulo | Valor por Defecto | Clave Primaria | Clave Única | Descripción |\n"
    doc += "|--------|------|------|-------------------|----------------|-------------|-------------|\n"
    
    for col_name, col_info in table_info['columns'].items():
        nullable = "Sí" if col_info.get('null?') == 'Y' else "No"
        default = col_info.get('default', '') or '-'
        pk = "Sí" if col_info.get('primary_key') == 'Y' else "No"
        uk = "Sí" if col_info.get('unique_key') == 'Y' else "No"
        description = col_info.get('comment', 'Sin descripción')
        
        doc += f"| `{col_name}` | `{col_info['type']}` | {nullable} | {default} | {pk} | {uk} | {description} |\n"
    
    doc += "\n"
    
    return doc

def generate_postgres_database_doc(database_name, database_dict):
    """Generar documentación para una base de datos PostgreSQL"""
    doc = f"# Base de Datos: {database_name}\n\n"
    
    db_info = database_dict.get('database_info', {})
    doc += f"**Generado:** {db_info.get('generated_at', 'N/A')}\n\n"
    
    stats = database_dict.get('statistics', {})
    doc += f"**Estadísticas:**\n"
    doc += f"- Total de tablas: {stats.get('total_tables', 0)}\n"
    doc += f"- Total de columnas: {stats.get('total_columns', 0)}\n"
    doc += f"- Total de constraints: {stats.get('total_constraints', 0)}\n\n"
    
    # Tablas
    tables = database_dict.get('tables', {})
    if tables:
        doc += "## Tablas\n\n"
        for table_name, table_info in tables.items():
            doc += generate_postgres_table_doc(table_name, table_info)
    
    # Relaciones
    relationships = database_dict.get('relationships', [])
    if relationships:
        doc += "## Relaciones entre Tablas\n\n"
        doc += "| Tabla Origen | Columna | Tabla Destino | Columna | Constraint |\n"
        doc += "|--------------|---------|---------------|---------|------------|\n"
        
        for rel in relationships:
            doc += f"| `{rel['from_table']}` | `{rel['from_column']}` | `{rel['to_table']}` | `{rel['to_column']}` | `{rel['constraint_name']}` |\n"
    
    return doc

def generate_snowflake_database_doc(database_name, database_dict):
    """Generar documentación para una base de datos Snowflake"""
    doc = f"# Base de Datos: {database_name}\n\n"
    
    # database_dict ya es la información de la base de datos específica
    db_info = database_dict
    if db_info.get('created_on'):
        doc += f"**Creada:** {db_info['created_on']}\n\n"
    
    if db_info.get('owner'):
        doc += f"**Propietario:** {db_info['owner']}\n\n"
    
    if db_info.get('comment'):
        doc += f"**Descripción:** {db_info['comment']}\n\n"
    
    # Schemas
    schemas = db_info.get('schemas', {})
    if schemas:
        doc += "## Schemas\n\n"
        for schema_name, schema_info in schemas.items():
            doc += f"### Schema: `{schema_name}`\n\n"
            
            if schema_info.get('comment'):
                doc += f"**Descripción:** {schema_info['comment']}\n\n"
            
            # Tablas en el schema
            tables = schema_info.get('tables', {})
            if tables:
                doc += "#### Tablas\n\n"
                for table_name, table_info in tables.items():
                    doc += generate_snowflake_table_doc(table_name, table_info)
            
            # Stages en el schema
            stages = schema_info.get('stages', {})
            if stages:
                doc += "#### Stages\n\n"
                for stage_name, stage_info in stages.items():
                    doc += f"**Stage:** `{stage_name}`\n"
                    if stage_info.get('url'):
                        doc += f"- **URL:** {stage_info['url']}\n"
                    if stage_info.get('type'):
                        doc += f"- **Tipo:** {stage_info['type']}\n"
                    if stage_info.get('cloud'):
                        doc += f"- **Cloud:** {stage_info['cloud']}\n"
                    doc += "\n"
    
    return doc

def generate_summary_doc(postgres_dict, snowflake_dict):
    """Generar documento de resumen general"""
    doc = "# Diccionario de Datos - Resumen General\n\n"
    
    doc += f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    doc += "## Resumen de Sistemas\n\n"
    
    # PostgreSQL
    if postgres_dict:
        postgres_stats = postgres_dict.get('summary', {})
        doc += "### PostgreSQL (Supabase + RDS)\n\n"
        doc += f"- **Bases de datos:** {postgres_stats.get('total_databases', 0)}\n"
        doc += f"- **Total de tablas:** {postgres_stats.get('total_tables', 0)}\n"
        doc += f"- **Total de columnas:** {postgres_stats.get('total_columns', 0)}\n\n"
    
    # Snowflake
    if snowflake_dict:
        snowflake_stats = snowflake_dict.get('statistics', {})
        doc += "### Snowflake (Data Warehouse)\n\n"
        doc += f"- **Warehouses:** {snowflake_stats.get('total_warehouses', 0)}\n"
        doc += f"- **Bases de datos:** {snowflake_stats.get('total_databases', 0)}\n"
        doc += f"- **Schemas:** {snowflake_stats.get('total_schemas', 0)}\n"
        doc += f"- **Tablas:** {snowflake_stats.get('total_tables', 0)}\n"
        doc += f"- **Columnas:** {snowflake_stats.get('total_columns', 0)}\n"
        doc += f"- **Stages:** {snowflake_stats.get('total_stages', 0)}\n\n"
    
    doc += "## Arquitectura de Datos\n\n"
    doc += "```mermaid\n"
    doc += "graph TB\n"
    doc += "    subgraph \"PostgreSQL\"\n"
    doc += "        A[Supabase] --> C[S3]\n"
    doc += "        B[RDS] --> C\n"
    doc += "    end\n"
    doc += "    C --> D[Snowflake Bronze Layer]\n"
    doc += "    D --> E[Snowflake Silver Layer]\n"
    doc += "    E --> F[Reportes Gerenciales]\n"
    doc += "```\n\n"
    
    doc += "## Flujo de Datos\n\n"
    doc += "1. **Extracción:** Datos desde Supabase y RDS\n"
    doc += "2. **Transformación:** Conversión a formato Parquet\n"
    doc += "3. **Carga:** Almacenamiento en S3\n"
    doc += "4. **Ingesta:** Carga a Snowflake Bronze Layer\n"
    doc += "5. **Procesamiento:** Creación de Silver Layer\n"
    doc += "6. **Reportes:** Generación de reportes gerenciales\n\n"
    
    doc += "## Documentación Detallada\n\n"
    doc += "- [Diccionario PostgreSQL - Supabase](supabase_dictionary.md)\n"
    doc += "- [Diccionario PostgreSQL - RDS](rds_dictionary.md)\n"
    doc += "- [Diccionario Snowflake](snowflake_dictionary.md)\n\n"
    
    return doc

def main():
    """Función principal"""
    logger.info("Iniciando generación de documentación Markdown")
    
    # Directorios - usar directorio temporal
    docs_dir = '/tmp/data_dictionaries'
    
    try:
        # Cargar diccionarios JSON
        logger.info("Cargando diccionarios JSON...")
        
        postgres_unified = load_json_dictionary(os.path.join(docs_dir, 'postgres_unified_dictionary.json'))
        snowflake_dict = load_json_dictionary(os.path.join(docs_dir, 'snowflake_dictionary.json'))
        
        if not postgres_unified or not snowflake_dict:
            logger.error("No se pudieron cargar los diccionarios JSON")
            sys.exit(1)
        
        # Generar documentación PostgreSQL
        logger.info("Generando documentación PostgreSQL...")
        
        if 'databases' in postgres_unified:
            for db_name, db_dict in postgres_unified['databases'].items():
                doc_content = generate_postgres_database_doc(db_name, db_dict)
                output_file = os.path.join(docs_dir, f'{db_name.lower()}_dictionary.md')
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(doc_content)
                
                logger.info(f"Documentación {db_name} guardada en: {output_file}")
        
        # Generar documentación Snowflake
        logger.info("Generando documentación Snowflake...")
        
        if 'databases' in snowflake_dict:
            output_file = os.path.join(docs_dir, 'snowflake_dictionary.md')
            
            # Limpiar archivo antes de escribir
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("# Diccionario de Datos - Snowflake\n\n")
                f.write(f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write("---\n\n")
            
            # Agregar cada base de datos
            for db_name, db_dict in snowflake_dict['databases'].items():
                doc_content = generate_snowflake_database_doc(db_name, db_dict)
                
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(doc_content)
                    f.write("\n---\n\n")
                
                logger.info(f"Documentación para base de datos {db_name} agregada a: {output_file}")
        
        # Generar documento de resumen
        logger.info("Generando documento de resumen...")
        summary_doc = generate_summary_doc(postgres_unified, snowflake_dict)
        summary_file = os.path.join(docs_dir, 'data_dictionary_summary.md')
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary_doc)
        
        logger.info(f"Documento de resumen guardado en: {summary_file}")
        
        logger.info("Documentación Markdown generada exitosamente")
        logger.info("Archivos creados:")
        logger.info("- data_dictionary_summary.md (resumen general)")
        logger.info("- supabase_dictionary.md")
        logger.info("- rds_dictionary.md")
        logger.info("- snowflake_dictionary.md")
        
    except Exception as e:
        logger.error(f"Error generando documentación Markdown: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
