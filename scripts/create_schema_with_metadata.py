#!/usr/bin/env python3
"""
Script para crear esquemas PostgreSQL con metadatos (comentarios) en tablas y columnas
Incluye descripciones detalladas del propósito de cada tabla y columna
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log(msg):
    """Logs a message to both console and file."""
    print(msg)
    logger.info(msg)

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

def create_tables_with_metadata(conn, db_name):
    """Create tables with detailed metadata comments"""
    log(f"Creating tables with metadata for {db_name}...")
    
    try:
        # Define table schemas with metadata
        tables_metadata = {
            'banco': {
                'description': 'Tabla maestra de bancos que participan en el programa de puntos',
                'columns': {
                    'bankid': {
                        'type': 'INTEGER',
                        'description': 'Identificador único del banco en el sistema'
                    },
                    'issuer': {
                        'type': 'VARCHAR',
                        'description': 'Nombre del emisor o entidad financiera del banco'
                    },
                    'name': {
                        'type': 'VARCHAR',
                        'description': 'Nombre comercial del banco'
                    },
                    'creation_date_time': {
                        'type': 'TIMESTAMP',
                        'description': 'Fecha y hora de creación del registro del banco en el sistema'
                    },
                    'status': {
                        'type': 'VARCHAR',
                        'description': 'Estado actual del banco (activo, inactivo, suspendido)'
                    }
                }
            },
            'programa': {
                'description': 'Tabla maestra de programas de puntos disponibles en el sistema',
                'columns': {
                    'programid': {
                        'type': 'INTEGER',
                        'description': 'Identificador único del programa de puntos'
                    },
                    'name': {
                        'type': 'VARCHAR',
                        'description': 'Nombre del programa de puntos'
                    },
                    'redemption_value_per_point': {
                        'type': 'DECIMAL(10,6)',
                        'description': 'Valor de redención por punto en moneda local'
                    },
                    'creation_date_time': {
                        'type': 'TIMESTAMP',
                        'description': 'Fecha y hora de creación del programa en el sistema'
                    }
                }
            },
            'usuario': {
                'description': 'Tabla de usuarios registrados en el sistema de puntos',
                'columns': {
                    'id_user': {
                        'type': 'INTEGER',
                        'description': 'Identificador único del usuario en el sistema'
                    },
                    'firstname': {
                        'type': 'VARCHAR',
                        'description': 'Nombre del usuario'
                    },
                    'lastname': {
                        'type': 'VARCHAR',
                        'description': 'Apellido del usuario'
                    },
                    'identificationnumber': {
                        'type': 'VARCHAR',
                        'description': 'Número de identificación (cédula, pasaporte, etc.)'
                    },
                    'emailaddress': {
                        'type': 'VARCHAR',
                        'description': 'Dirección de correo electrónico del usuario'
                    },
                    'streetaddress': {
                        'type': 'VARCHAR',
                        'description': 'Dirección física del usuario'
                    },
                    'idauthuserstatus': {
                        'type': 'INTEGER',
                        'description': 'Estado de autenticación del usuario (1=activo, 0=inactivo)'
                    },
                    'createdat': {
                        'type': 'TIMESTAMP',
                        'description': 'Fecha y hora de creación del usuario en el sistema'
                    },
                    'channel_id': {
                        'type': 'INTEGER',
                        'description': 'Identificador del canal por el cual se registró el usuario'
                    }
                }
            },
            'cardholder': {
                'description': 'Tabla de titulares de tarjetas de puntos',
                'columns': {
                    'cardholder_id': {
                        'type': 'INTEGER',
                        'description': 'Identificador único del titular de la tarjeta'
                    },
                    'first_name': {
                        'type': 'VARCHAR',
                        'description': 'Nombre del titular de la tarjeta'
                    },
                    'last_name': {
                        'type': 'VARCHAR',
                        'description': 'Apellido del titular de la tarjeta'
                    },
                    'email': {
                        'type': 'VARCHAR',
                        'description': 'Correo electrónico del titular de la tarjeta'
                    },
                    'creation_date_time': {
                        'type': 'TIMESTAMP',
                        'description': 'Fecha y hora de creación del titular en el sistema'
                    },
                    'user_id': {
                        'type': 'INTEGER',
                        'description': 'Referencia al usuario asociado (FK a usuario.id_user)'
                    },
                    'status': {
                        'type': 'VARCHAR',
                        'description': 'Estado del titular (activo, inactivo, suspendido)'
                    }
                }
            },
            'tarjeta_puntos': {
                'description': 'Tabla de transacciones de puntos en tarjetas',
                'columns': {
                    'points_card_id': {
                        'type': 'INTEGER',
                        'description': 'Identificador único de la tarjeta de puntos'
                    },
                    'cardholder_id': {
                        'type': 'INTEGER',
                        'description': 'Referencia al titular de la tarjeta (FK a cardholder.cardholder_id)'
                    },
                    'program_id': {
                        'type': 'INTEGER',
                        'description': 'Referencia al programa de puntos (FK a programa.programid)'
                    },
                    'bank_id': {
                        'type': 'INTEGER',
                        'description': 'Referencia al banco emisor (FK a banco.bankid)'
                    },
                    'transaction_date_time': {
                        'type': 'TIMESTAMP',
                        'description': 'Fecha y hora de la transacción de puntos'
                    },
                    'type': {
                        'type': 'VARCHAR',
                        'description': 'Tipo de transacción (CREDIT=acumulación, DEBIT=redención)'
                    },
                    'points': {
                        'type': 'INTEGER',
                        'description': 'Cantidad de puntos involucrados en la transacción'
                    },
                    'operation': {
                        'type': 'VARCHAR',
                        'description': 'Descripción de la operación realizada'
                    }
                }
            }
        }
        
        # Create tables with metadata
        for table_name, table_info in tables_metadata.items():
            log(f"Creating table {table_name} with metadata...")
            
            # Build column definitions
            columns = []
            for col_name, col_info in table_info['columns'].items():
                columns.append(f"{col_name} {col_info['type']}")
            
            # Add primary key constraints
            if table_name == 'banco':
                columns.append("PRIMARY KEY (bankid)")
            elif table_name == 'programa':
                columns.append("PRIMARY KEY (programid)")
            elif table_name == 'usuario':
                columns.append("PRIMARY KEY (id_user)")
            elif table_name == 'cardholder':
                columns.append("PRIMARY KEY (cardholder_id)")
            elif table_name == 'tarjeta_puntos':
                columns.append("PRIMARY KEY (points_card_id)")
            
            # Create table
            create_sql = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(columns) + "\n)"
            
            log(f"Executing: {create_sql}")
            with conn.cursor() as cursor:
                cursor.execute(create_sql)
                conn.commit()
            
            # Add table comment
            table_comment_sql = f"COMMENT ON TABLE {table_name} IS '{table_info['description']}'"
            with conn.cursor() as cursor:
                cursor.execute(table_comment_sql)
                conn.commit()
            log(f"Added table comment for {table_name}")
            
            # Add column comments
            for col_name, col_info in table_info['columns'].items():
                column_comment_sql = f"COMMENT ON COLUMN {table_name}.{col_name} IS '{col_info['description']}'"
                with conn.cursor() as cursor:
                    cursor.execute(column_comment_sql)
                    conn.commit()
                log(f"Added column comment for {table_name}.{col_name}")
            
            log(f"Table {table_name} created successfully with metadata")
        
        log(f"All tables created successfully with metadata for {db_name}")
        return True
        
    except Exception as e:
        log(f"Error creating tables with metadata for {db_name}: {e}")
        return False

def create_constraints_with_metadata(conn, db_name):
    """Create foreign key constraints with metadata"""
    log(f"Creating constraints with metadata for {db_name}...")
    
    try:
        # Define foreign key constraints
        constraints = [
            {
                'table': 'cardholder',
                'column': 'user_id',
                'ref_table': 'usuario',
                'ref_column': 'id_user',
                'name': 'cardholder_user_id_fkey',
                'description': 'Relación entre titular de tarjeta y usuario del sistema'
            },
            {
                'table': 'tarjeta_puntos',
                'column': 'cardholder_id',
                'ref_table': 'cardholder',
                'ref_column': 'cardholder_id',
                'name': 'tarjeta_puntos_cardholder_id_fkey',
                'description': 'Relación entre transacción de puntos y titular de tarjeta'
            },
            {
                'table': 'tarjeta_puntos',
                'column': 'program_id',
                'ref_table': 'programa',
                'ref_column': 'programid',
                'name': 'tarjeta_puntos_program_id_fkey',
                'description': 'Relación entre transacción de puntos y programa de puntos'
            },
            {
                'table': 'tarjeta_puntos',
                'column': 'bank_id',
                'ref_table': 'banco',
                'ref_column': 'bankid',
                'name': 'tarjeta_puntos_bank_id_fkey',
                'description': 'Relación entre transacción de puntos y banco emisor'
            }
        ]
        
        # Create constraints
        for constraint in constraints:
            constraint_sql = f"""
                ALTER TABLE {constraint['table']} 
                ADD CONSTRAINT {constraint['name']} 
                FOREIGN KEY ({constraint['column']}) 
                REFERENCES {constraint['ref_table']}({constraint['ref_column']})
            """
            
            log(f"Creating constraint: {constraint['name']}")
            with conn.cursor() as cursor:
                cursor.execute(constraint_sql)
                conn.commit()
            
            # Add constraint comment
            comment_sql = f"COMMENT ON CONSTRAINT {constraint['name']} ON {constraint['table']} IS '{constraint['description']}'"
            with conn.cursor() as cursor:
                cursor.execute(comment_sql)
                conn.commit()
            
            log(f"Constraint {constraint['name']} created successfully with metadata")
        
        log(f"All constraints created successfully with metadata for {db_name}")
        return True
        
    except Exception as e:
        log(f"Error creating constraints with metadata for {db_name}: {e}")
        return False

def create_schema_with_metadata(db_type):
    """Create complete schema with metadata for specified database"""
    log(f"Starting schema creation with metadata for {db_type.upper()}")
    
    try:
        # Get connection
        conn = get_postgres_connection(db_type)
        log(f"Connected to {db_type.upper()}")
        
        # Drop existing tables (in reverse order due to foreign keys)
        tables_to_drop = ['tarjeta_puntos', 'cardholder', 'usuario', 'programa', 'banco']
        for table in tables_to_drop:
            try:
                drop_sql = f"DROP TABLE IF EXISTS {table} CASCADE"
                with conn.cursor() as cursor:
                    cursor.execute(drop_sql)
                    conn.commit()
                log(f"Dropped table {table} if exists")
            except Exception as e:
                log(f"Warning: Could not drop table {table}: {e}")
        
        # Create tables with metadata
        if not create_tables_with_metadata(conn, db_type.upper()):
            return False
        
        # Create constraints with metadata
        if not create_constraints_with_metadata(conn, db_type.upper()):
            return False
        
        # Close connection
        conn.close()
        
        log(f"Schema creation with metadata completed successfully for {db_type.upper()}")
        return True
        
    except Exception as e:
        log(f"Error in schema creation with metadata for {db_type.upper()}: {e}")
        return False

def main():
    """Main function"""
    if len(sys.argv) != 2:
        log("Usage: python create_schema_with_metadata.py <database_type>")
        log("Database types: supabase, rds")
        sys.exit(1)
    
    db_type = sys.argv[1].lower()
    
    if db_type not in ['supabase', 'rds']:
        log("Error: Database type must be 'supabase' or 'rds'")
        sys.exit(1)
    
    success = create_schema_with_metadata(db_type)
    
    if success:
        log(f"Schema creation with metadata completed successfully for {db_type.upper()}")
        sys.exit(0)
    else:
        log(f"Schema creation with metadata failed for {db_type.upper()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
