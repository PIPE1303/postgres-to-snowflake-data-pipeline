# 📋 Sistema de Metadatos (Comentarios) en Bases de Datos

## 🎯 **Descripción**

Sistema completo para agregar metadatos descriptivos (comentarios) a tablas y columnas en PostgreSQL y Snowflake, mejorando la documentación y comprensión del esquema de datos.

## 🏗️ **Arquitectura de Metadatos**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │    Snowflake    │    │   Diccionarios  │
│                 │    │                 │    │                 │
│ • Tabla Comments│───▶│ • Table Comments│───▶│ • Metadatos     │
│ • Column Comments│    │ • Column Comments│    │ • Documentación │
│ • Constraint Comments│ │ • Schema Comments│    │ • Auto-generada │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📁 **Archivos del Sistema de Metadatos**

### **Scripts de Creación:**
- `scripts/create_schema_with_metadata.py` - Crea esquemas PostgreSQL con metadatos completos
- `dags/data_pipeline_with_metadata_dag.py` - DAG para automatizar creación con metadatos

### **DAGs Actualizados:**
- `dags/s3_to_snowflake_dag.py` - Tablas Snowflake con comentarios descriptivos
- `dags/silver_layer_dag.py` - Silver layer con metadatos en Snowflake y PostgreSQL

## 🗃️ **Metadatos Implementados**

### **PostgreSQL (Supabase + RDS)**

#### **Comentarios de Tabla:**
```sql
COMMENT ON TABLE banco IS 'Tabla maestra de bancos que participan en el programa de puntos';
COMMENT ON TABLE usuario IS 'Tabla de usuarios registrados en el sistema de puntos';
COMMENT ON TABLE tarjeta_puntos IS 'Tabla de transacciones de puntos en tarjetas';
```

#### **Comentarios de Columna:**
```sql
COMMENT ON COLUMN banco.bankid IS 'Identificador único del banco en el sistema';
COMMENT ON COLUMN usuario.emailaddress IS 'Dirección de correo electrónico del usuario';
COMMENT ON COLUMN tarjeta_puntos.type IS 'Tipo de transacción (CREDIT=acumulación, DEBIT=redención)';
```

#### **Comentarios de Constraints:**
```sql
COMMENT ON CONSTRAINT cardholder_user_id_fkey ON cardholder 
IS 'Relación entre titular de tarjeta y usuario del sistema';
```

### **Snowflake (Bronze + Silver Layers)**

#### **Comentarios de Tabla:**
```sql
CREATE TABLE banco (
    -- columnas...
) COMMENT = 'Tabla maestra de bancos que participan en el programa de puntos';
```

#### **Comentarios de Columna:**
```sql
bankid INTEGER COMMENT 'Identificador único del banco',
issuer VARCHAR COMMENT 'Nombre del emisor del banco',
name VARCHAR COMMENT 'Nombre comercial del banco'
```

## 🚀 **Cómo Usar el Sistema de Metadatos**

### **1. Crear Esquemas con Metadatos (PostgreSQL)**

```bash
# Crear esquema Supabase con metadatos
python scripts/create_schema_with_metadata.py supabase

# Crear esquema RDS con metadatos
python scripts/create_schema_with_metadata.py rds
```

### **2. Ejecutar DAG con Metadatos**

```bash
# Ejecutar pipeline completo con metadatos
docker exec genius-xyz_5b3fe4-scheduler-1 airflow dags trigger data_pipeline_with_metadata_dag
```

### **3. Actualizar Snowflake con Metadatos**

```bash
# Ejecutar DAG de Snowflake con metadatos
docker exec genius-xyz_5b3fe4-scheduler-1 airflow dags trigger s3_to_snowflake_dag

# Ejecutar Silver Layer con metadatos
docker exec genius-xyz_5b3fe4-scheduler-1 airflow dags trigger silver_layer_dag
```

## 📊 **Ejemplos de Metadatos Implementados**

### **Tabla: `banco`**
```sql
-- PostgreSQL
COMMENT ON TABLE banco IS 'Tabla maestra de bancos que participan en el programa de puntos';
COMMENT ON COLUMN banco.bankid IS 'Identificador único del banco en el sistema';
COMMENT ON COLUMN banco.issuer IS 'Nombre del emisor o entidad financiera del banco';
COMMENT ON COLUMN banco.name IS 'Nombre comercial del banco';
COMMENT ON COLUMN banco.creation_date_time IS 'Fecha y hora de creación del registro del banco en el sistema';
COMMENT ON COLUMN banco.status IS 'Estado actual del banco (activo, inactivo, suspendido)';

-- Snowflake
CREATE TABLE banco (
    bankid INTEGER COMMENT 'Identificador único del banco',
    issuer VARCHAR COMMENT 'Nombre del emisor del banco',
    name VARCHAR COMMENT 'Nombre comercial del banco',
    creation_date_time TIMESTAMP_NTZ COMMENT 'Fecha y hora de creación del registro del banco',
    status VARCHAR COMMENT 'Estado actual del banco (activo, inactivo, suspendido)',
    source_database VARCHAR COMMENT 'Base de datos origen (SUPABASE o RDS)',
    export_timestamp TIMESTAMP_NTZ COMMENT 'Timestamp de cuando se exportó el registro a Snowflake'
) COMMENT = 'Tabla maestra de bancos que participan en el programa de puntos';
```

### **Tabla: `tarjeta_puntos`**
```sql
-- PostgreSQL
COMMENT ON TABLE tarjeta_puntos IS 'Tabla de transacciones de puntos en tarjetas';
COMMENT ON COLUMN tarjeta_puntos.points_card_id IS 'Identificador único de la tarjeta de puntos';
COMMENT ON COLUMN tarjeta_puntos.cardholder_id IS 'Referencia al titular de la tarjeta (FK a cardholder.cardholder_id)';
COMMENT ON COLUMN tarjeta_puntos.program_id IS 'Referencia al programa de puntos (FK a programa.programid)';
COMMENT ON COLUMN tarjeta_puntos.bank_id IS 'Referencia al banco emisor (FK a banco.bankid)';
COMMENT ON COLUMN tarjeta_puntos.transaction_date_time IS 'Fecha y hora de la transacción de puntos';
COMMENT ON COLUMN tarjeta_puntos.type IS 'Tipo de transacción (CREDIT=acumulación, DEBIT=redención)';
COMMENT ON COLUMN tarjeta_puntos.points IS 'Cantidad de puntos involucrados en la transacción';
COMMENT ON COLUMN tarjeta_puntos.operation IS 'Descripción de la operación realizada';

-- Snowflake
CREATE TABLE tarjeta_puntos (
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
```

## 🔍 **Consultar Metadatos**

### **PostgreSQL:**
```sql
-- Ver comentarios de tablas
SELECT schemaname, tablename, obj_description(c.oid) as table_comment
FROM pg_tables t
LEFT JOIN pg_class c ON c.relname = t.tablename
WHERE schemaname = 'public';

-- Ver comentarios de columnas
SELECT 
    table_name,
    column_name,
    col_description(pgc.oid, c.ordinal_position) as column_comment
FROM information_schema.columns c
LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;
```

### **Snowflake:**
```sql
-- Ver comentarios de tablas
SHOW TABLES;
SELECT TABLE_NAME, COMMENT 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'GENNIUS_XYZ.COMPANY_BRONZE_LAYER';

-- Ver comentarios de columnas
DESCRIBE TABLE banco;
SELECT COLUMN_NAME, DATA_TYPE, COMMENT 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'GENNIUS_XYZ.COMPANY_BRONZE_LAYER'
AND TABLE_NAME = 'BANCO';
```

## 📈 **Beneficios del Sistema de Metadatos**

### **1. Documentación Automática**
- ✅ Comentarios descriptivos en cada tabla y columna
- ✅ Explicación del propósito de cada campo
- ✅ Contexto de negocio para cada entidad

### **2. Mejor Comprensión**
- ✅ Nuevos desarrolladores entienden el esquema rápidamente
- ✅ Relaciones entre tablas claramente documentadas
- ✅ Tipos de datos y restricciones explicados

### **3. Mantenimiento Fácil**
- ✅ Cambios futuros son más seguros
- ✅ Impacto de modificaciones es claro
- ✅ Documentación siempre actualizada

### **4. Integración con Herramientas**
- ✅ Diccionarios de datos automáticos
- ✅ Documentación generada automáticamente
- ✅ Herramientas de BI pueden usar metadatos

## 🛠️ **Personalización de Metadatos**

### **Agregar Nuevos Comentarios:**
1. Editar `scripts/create_schema_with_metadata.py`
2. Modificar el diccionario `tables_metadata`
3. Agregar descripciones para nuevas tablas/columnas

### **Modificar Comentarios Existentes:**
1. Actualizar descripciones en el script
2. Re-ejecutar el DAG correspondiente
3. Los comentarios se actualizarán automáticamente

## 📝 **Estándares de Metadatos**

### **Formato de Comentarios:**
- **Tablas:** Descripción del propósito de la tabla en el negocio
- **Columnas:** Explicación del contenido y formato del campo
- **Constraints:** Descripción de la relación o restricción

### **Convenciones:**
- Comentarios en español para facilitar comprensión
- Incluir referencias a claves foráneas cuando aplique
- Especificar formatos de datos cuando sea relevante
- Explicar valores posibles para campos categóricos

---

**🎉 ¡Sistema de metadatos completamente implementado y funcional!**

**Todos los esquemas ahora incluyen documentación completa y descriptiva.**
