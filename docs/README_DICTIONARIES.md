# 📚 Sistema de Diccionarios de Datos

## 🎯 **Descripción**

Sistema automatizado para generar diccionarios de datos completos para PostgreSQL (Supabase y RDS) y Snowflake (Data Warehouse). Incluye documentación en formato Markdown y JSON estructurado.

## 🏗️ **Arquitectura**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │    Snowflake    │    │   Documentación │
│                 │    │                 │    │                 │
│ • Supabase      │───▶│ • Bronze Layer  │───▶│ • Markdown      │
│ • RDS           │    │ • Silver Layer  │    │ • JSON          │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📁 **Estructura de Archivos**

```
docs/data_dictionaries/
├── supabase_dictionary.json          # Diccionario Supabase (JSON)
├── rds_dictionary.json               # Diccionario RDS (JSON)
├── postgres_unified_dictionary.json  # Diccionario unificado PostgreSQL
├── snowflake_dictionary.json         # Diccionario Snowflake (JSON)
├── data_dictionary_summary.md        # Resumen general (Markdown)
├── supabase_dictionary.md            # Documentación Supabase (Markdown)
├── rds_dictionary.md                 # Documentación RDS (Markdown)
└── snowflake_dictionary.md           # Documentación Snowflake (Markdown)
```

## 🚀 **Cómo Usar**

### **1. Ejecución Manual**

```bash
# Generar diccionarios PostgreSQL
python scripts/generate_postgres_dictionary.py

# Generar diccionario Snowflake
python scripts/generate_snowflake_dictionary.py

# Generar documentación Markdown
python scripts/generate_markdown_docs.py
```

### **2. Ejecución Automatizada (Airflow)**

```bash
# Ejecutar DAG completo
docker exec genius-xyz_5b3fe4-scheduler-1 airflow dags trigger data_dictionary_dag

# Ver estado del DAG
docker exec genius-xyz_5b3fe4-scheduler-1 airflow dags list-runs --dag-id data_dictionary_dag
```

### **3. Programación Automática**

El DAG `data_dictionary_dag` está configurado para ejecutarse **semanalmente** (`@weekly`) y mantener los diccionarios actualizados automáticamente.

## 📊 **Contenido de los Diccionarios**

### **PostgreSQL (Supabase + RDS)**

- **Información de tablas:** Nombre, descripción, tamaño
- **Columnas:** Tipo de dato, nullable, valor por defecto, posición
- **Constraints:** Claves primarias, foráneas, únicas, check
- **Relaciones:** Mapeo completo de relaciones entre tablas
- **Estadísticas:** Conteos totales de tablas, columnas, constraints

### **Snowflake (Data Warehouse)**

- **Warehouses:** Configuración y estado
- **Databases:** Información de bases de datos
- **Schemas:** Organización por schemas
- **Tablas:** Metadatos completos incluyendo filas, tamaño, propietario
- **Columnas:** Tipos de dato, constraints, descripciones
- **Stages:** Configuración de stages externos
- **Estadísticas:** Conteos totales por categoría

## 📋 **Ejemplo de Uso**

### **Consultar Tabla Específica**

```python
import json

# Cargar diccionario
with open('docs/data_dictionaries/supabase_dictionary.json', 'r') as f:
    dict_data = json.load(f)

# Obtener información de tabla
table_info = dict_data['tables']['usuario']
print(f"Tabla: {table_info['name']}")
print(f"Descripción: {table_info['description']}")
print(f"Columnas: {len(table_info['columns'])}")

# Listar columnas
for col_name, col_info in table_info['columns'].items():
    print(f"- {col_name}: {col_info['data_type']}")
```

### **Analizar Relaciones**

```python
# Obtener relaciones
relationships = dict_data['relationships']
for rel in relationships:
    print(f"{rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
```

## 🔧 **Configuración**

### **Variables de Entorno Requeridas**

```bash
# PostgreSQL
SUPABASE_CONN_ID=supabase_default
RDS_CONN_ID=rds_default

# Snowflake (configurado en Airflow Connections)
SNOWFLAKE_CONN_ID=snowflake_default
```

### **Conexiones Airflow**

- `supabase_default`: Conexión a Supabase
- `rds_default`: Conexión a RDS
- `snowflake_default`: Conexión a Snowflake

## 📈 **Beneficios**

1. **Documentación Automática:** Generación automática de documentación completa
2. **Mantenimiento Fácil:** Actualización semanal automática
3. **Múltiples Formatos:** JSON para programación, Markdown para lectura
4. **Metadatos Completos:** Información detallada de estructura y relaciones
5. **Integración Airflow:** Parte del pipeline de datos automatizado

## 🛠️ **Personalización**

### **Agregar Nuevas Bases de Datos**

1. Modificar `scripts/generate_postgres_dictionary.py`
2. Agregar nueva conexión en variables de entorno
3. Actualizar el DAG si es necesario

### **Modificar Formato de Documentación**

1. Editar `scripts/generate_markdown_docs.py`
2. Personalizar templates de Markdown
3. Agregar nuevos campos o secciones

## 📝 **Notas Importantes**

- Los diccionarios se generan en el directorio `/usr/local/airflow/docs/data_dictionaries/`
- El DAG se ejecuta semanalmente para mantener la documentación actualizada
- Los archivos JSON contienen metadatos estructurados para uso programático
- Los archivos Markdown están optimizados para lectura humana
- Se incluye un diagrama Mermaid de la arquitectura de datos

## 🔍 **Troubleshooting**

### **Error: "Dag id data_dictionary_dag not found"**

```bash
# Forzar reserialización de DAGs
docker exec genius-xyz_5b3fe4-scheduler-1 airflow dags reserialize
```

### **Error: "Connection not found"**

Verificar que las conexiones estén configuradas en Airflow:
- `supabase_default`
- `rds_default` 
- `snowflake_default`

### **Error: "Permission denied"**

Verificar permisos de escritura en el directorio de documentación:
```bash
docker exec genius-xyz_5b3fe4-scheduler-1 chmod 755 /usr/local/airflow/docs/data_dictionaries/
```

---

**🎉 ¡Sistema de diccionarios de datos completamente funcional y automatizado!**
