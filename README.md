# 🚀 PostgreSQL to Snowflake Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-green.svg)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)](https://postgresql.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-orange.svg)](https://snowflake.com)
[![AWS S3](https://img.shields.io/badge/AWS%20S3-Storage-yellow.svg)](https://aws.amazon.com/s3)

A comprehensive and automated data pipeline that orchestrates ETL processes between PostgreSQL databases (Supabase & RDS), AWS S3, and Snowflake, featuring automatic data dictionary generation and comprehensive documentation. This enterprise-grade solution implements modern data engineering practices with Bronze and Silver data layers, timezone-aware processing, and automated metadata management.

## 🏗️ Arquitectura

```
PostgreSQL (Supabase + RDS) → S3 (Parquet) → Snowflake (Bronze) → Snowflake (Silver) → Supabase (Reports)
```

### Componentes Principales

- **🔧 Apache Airflow**: Orquestación de workflows
- **🐘 PostgreSQL**: Bases de datos fuente (Supabase, RDS)
- **☁️ AWS S3**: Almacenamiento intermedio (formato Parquet)
- **❄️ Snowflake**: Data warehouse (capas Bronze y Silver)
- **📊 Python**: Scripts de procesamiento y transformación
- **📚 Documentación**: Generación automática de diccionarios de datos

## 🚀 Características

### ✨ Funcionalidades Principales

- **🔄 Pipeline ETL Completo**: Extracción, transformación y carga automatizada
- **📈 Capas de Datos**: Implementación de arquitectura Bronze → Silver
- **🌍 Zona Horaria**: Configuración para Colombia (UTC-5)
- **📋 Metadatos**: Comentarios automáticos en tablas y columnas
- **📖 Diccionarios**: Generación automática de documentación
- **🧹 Limpieza**: Gestión automática de archivos temporales
- **☁️ S3 Integration**: Subida automática de diccionarios a S3

### 🎯 DAGs Disponibles

| DAG | Descripción | Frecuencia |
|-----|-------------|------------|
| `data_pipeline_variables_dag` | Creación de esquemas y carga de datos | Manual |
| `export_to_s3_dag` | Exportación PostgreSQL → S3 (Parquet) | Manual |
| `s3_to_snowflake_dag` | Carga S3 → Snowflake (Bronze Layer) | Manual |
| `silver_layer_dag` | Generación de capa Silver + Reportes | Manual |
| `data_dictionary_dag` | Generación de diccionarios de datos | Semanal |

## 📁 Estructura del Proyecto

```
postgres-to-snowflake-data-pipeline/
├── 📁 dags/                          # DAGs de Airflow
│   ├── data_pipeline_variables_dag.py
│   ├── export_to_s3_dag.py
│   ├── s3_to_snowflake_dag.py
│   ├── silver_layer_dag.py
│   └── data_dictionary_dag.py
├── 📁 scripts/                       # Scripts de procesamiento
│   ├── create_schema_with_metadata.py
│   ├── load_data.py
│   ├── export_to_s3.py
│   ├── generate_postgres_dictionary.py
│   ├── generate_snowflake_dictionary.py
│   └── generate_markdown_docs.py
├── 📁 data/                          # Datos de ejemplo
│   ├── supabase/
│   └── rds/
├── 📁 docs/                          # Documentación
│   ├── README_DICTIONARIES.md
│   └── README_METADATA.md
├── 📁 .cursor/                       # Reglas de Cursor AI
│   └── rules/
├── 🐳 Dockerfile                     # Configuración Docker
├── 📋 requirements.txt               # Dependencias Python
├── 📦 packages.txt                   # Paquetes del sistema
└── ⚙️ airflow_settings.yaml         # Configuración Airflow
```

## 🛠️ Instalación y Configuración

### Prerrequisitos

- Docker y Docker Compose
- Python 3.12+
- Acceso a PostgreSQL (Supabase, RDS)
- Acceso a Snowflake
- Acceso a AWS S3

### 1. Clonar el Repositorio

```bash
git clone https://github.com/PIPE1303/postgres-to-snowflake-data-pipeline.git
cd postgres-to-snowflake-data-pipeline
```

### 2. Configurar Variables de Airflow

```bash
# Variables de PostgreSQL
airflow variables set supabase_host "tu-supabase-host"
airflow variables set supabase_name "tu-supabase-db"
airflow variables set supabase_user "tu-usuario"
airflow variables set supabase_password "tu-password"
airflow variables set supabase_port "5432"

airflow variables set rds_host "tu-rds-host"
airflow variables set rds_name "tu-rds-db"
airflow variables set rds_user "tu-usuario"
airflow variables set rds_password "tu-password"
airflow variables set rds_port "5432"

# Variables de AWS S3
airflow variables set S3_BUCKET "tu-bucket-name"

# Variables de Snowflake
airflow variables set SNOWFLAKE_STAGE "@GENNIUS_XYZ.COMPANY_BRONZE_LAYER.S3_STAGE"
```

### 3. Configurar Conexiones de Airflow

```bash
# Conexión PostgreSQL Supabase
airflow connections add supabase_default \
    --conn-type postgres \
    --conn-host tu-supabase-host \
    --conn-schema tu-supabase-db \
    --conn-login tu-usuario \
    --conn-password tu-password \
    --conn-port 5432

# Conexión PostgreSQL RDS
airflow connections add rds_default \
    --conn-type postgres \
    --conn-host tu-rds-host \
    --conn-schema tu-rds-db \
    --conn-login tu-usuario \
    --conn-password tu-password \
    --conn-port 5432

# Conexión Snowflake
airflow connections add snowflake_default \
    --conn-type snowflake \
    --conn-login tu-usuario \
    --conn-password tu-password \
    --conn-extra '{"account": "tu-account", "warehouse": "COMPUTE_WH", "database": "GENNIUS_XYZ", "schema": "COMPANY_BRONZE_LAYER", "role": "PUBLIC"}'

# Conexión AWS
airflow connections add aws_default \
    --conn-type aws \
    --conn-extra '{"aws_access_key_id": "tu-access-key", "aws_secret_access_key": "tu-secret-key", "region_name": "us-east-1"}'
```

### 4. Ejecutar con Docker

```bash
# Construir y ejecutar
docker-compose up -d

# Ver logs
docker-compose logs -f

# Acceder a Airflow UI
# http://localhost:8080 (admin/admin)
```

## 🚀 Uso

### Ejecución Manual de DAGs

```bash
# 1. Crear esquemas y cargar datos
airflow dags trigger data_pipeline_variables_dag

# 2. Exportar a S3
airflow dags trigger export_to_s3_dag

# 3. Cargar a Snowflake
airflow dags trigger s3_to_snowflake_dag

# 4. Generar capa Silver
airflow dags trigger silver_layer_dag

# 5. Generar diccionarios
airflow dags trigger data_dictionary_dag
```

### Flujo de Datos

1. **📊 Carga Inicial**: Datos CSV → PostgreSQL (Supabase + RDS)
2. **📤 Exportación**: PostgreSQL → S3 (formato Parquet)
3. **❄️ Bronze Layer**: S3 → Snowflake (datos raw)
4. **✨ Silver Layer**: Transformaciones y reportes en Snowflake
5. **📋 Documentación**: Generación automática de diccionarios

## 📊 Datos de Ejemplo

### ⚠️ **Importante: Datos No Incluidos**

**Los archivos de datos no se incluyen en este repositorio por razones de confidencialidad y seguridad.** La carpeta `data/` contiene solo la estructura de directorios como referencia.

### 📁 **Estructura de Datos Requerida**

Para ejecutar el pipeline, debes crear la siguiente estructura de datos en la carpeta `data/`:

```
data/
├── supabase/
│   ├── banco.csv
│   ├── card_holder.csv
│   ├── programa.csv
│   ├── tarjeta_puntos.csv
│   └── usuario.csv
└── rds/
    ├── banco.csv
    ├── card_holder.csv
    ├── programa.csv
    ├── tarjeta_puntos.csv
    └── usuario.csv
```

### 📋 **Formato de Archivos CSV**

Cada archivo CSV debe tener las siguientes columnas:

#### **banco.csv**
```csv
bankid,issuer,name,creation_date_time,status,source_database,export_timestamp
1,Banco Ejemplo,Banco Ejemplo S.A.,2025-01-01 10:00:00,activo,SUPABASE,2025-01-01 10:00:00
```

#### **card_holder.csv**
```csv
cardholderid,name,email,phone,creation_date_time,status,source_database,export_timestamp
1,Juan Pérez,juan.perez@email.com,+57-300-123-4567,2025-01-01 10:00:00,activo,SUPABASE,2025-01-01 10:00:00
```

#### **programa.csv**
```csv
programid,name,description,start_date,end_date,status,source_database,export_timestamp
1,Programa Puntos,Programa de lealtad con puntos,2025-01-01,2025-12-31,activo,SUPABASE,2025-01-01 10:00:00
```

#### **tarjeta_puntos.csv**
```csv
tarjetaid,cardholderid,programid,card_number,points_balance,creation_date_time,status,source_database,export_timestamp
1,1,1,****-****-****-1234,1000,2025-01-01 10:00:00,activo,SUPABASE,2025-01-01 10:00:00
```

#### **usuario.csv**
```csv
userid,username,email,role,creation_date_time,status,source_database,export_timestamp
1,admin,admin@empresa.com,administrador,2025-01-01 10:00:00,activo,SUPABASE,2025-01-01 10:00:00
```

### 🔧 **Generación de Datos de Prueba**

Para generar datos de ejemplo, ejecuta el script incluido:

```bash
# Generar datos de ejemplo
python scripts/generate_sample_data.py
```

Este script creará:
- ✅ Estructura de directorios necesaria
- ✅ Archivos CSV con datos de ejemplo para todas las tablas
- ✅ Datos diferenciados para Supabase y RDS
- ✅ Formato correcto para el pipeline

**Nota**: Los datos generados son solo para pruebas. Reemplaza con tus datos reales siguiendo el mismo formato.

### 🛡️ **Consideraciones de Seguridad**

- **Nunca subas datos reales** a repositorios públicos
- **Usa datos anonimizados** para pruebas
- **Configura .gitignore** para excluir archivos sensibles
- **Usa variables de entorno** para credenciales
- **Implementa encriptación** para datos en tránsito

### 📝 **Notas Importantes**

1. **Mismo formato**: Los archivos en `supabase/` y `rds/` deben tener el mismo formato
2. **Encoding**: Usa UTF-8 para caracteres especiales
3. **Fechas**: Formato `YYYY-MM-DD HH:MM:SS`
4. **IDs**: Deben ser únicos dentro de cada base de datos
5. **Status**: Valores válidos: `activo`, `inactivo`, `suspendido`

## 📚 Documentación

### Diccionarios de Datos

Los diccionarios se generan automáticamente y se suben a S3 en:
```
s3://tu-bucket/metadata/data-dictionaries/YYYY/MM/DD/
├── data_dictionary_summary.md
├── postgres_unified_dictionary.json
├── snowflake_dictionary.json
└── [otros archivos de documentación]
```

### Metadatos

- **Tablas**: Comentarios descriptivos en todas las tablas
- **Columnas**: Descripción del propósito de cada columna
- **Restricciones**: Documentación de claves primarias y foráneas
- **Relaciones**: Mapeo de relaciones entre tablas

## 🔧 Configuración Avanzada

### Zona Horaria

El sistema está configurado para Colombia (UTC-5):
- Archivos S3 organizados por fecha local
- Timestamps en formato local
- Procesamiento respetando horario comercial

### Formato de Datos

- **S3**: Parquet (optimizado para Snowflake)
- **Snowflake**: TIMESTAMP_NTZ para timestamps
- **PostgreSQL**: Timestamps con timezone

### Seguridad

- Credenciales gestionadas por Airflow Connections
- Variables sensibles encriptadas
- Acceso basado en roles (Snowflake)

## 🧪 Testing

```bash
# Ejecutar tests
python -m pytest tests/

# Test de DAGs
airflow dags test data_pipeline_variables_dag 2025-01-01
```

## 📈 Monitoreo

### Logs

- **Airflow UI**: Monitoreo de DAGs y tareas
- **S3**: Logs de procesamiento
- **Snowflake**: Query history y performance

### Métricas

- Tiempo de ejecución de DAGs
- Volumen de datos procesados
- Errores y reintentos
- Uso de recursos

## 🤝 Contribución

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## 📝 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

## 👥 Autores

- **Andrés Felipe Marciales Pardo** - *Desarrollo inicial* - [@PIPE1303](https://github.com/PIPE1303)
- **LinkedIn**: [Andrés Marciales](https://www.linkedin.com/in/andres-marciales-de/)

## 🙏 Agradecimientos

- Apache Airflow Community
- Snowflake Documentation
- PostgreSQL Community
- AWS Documentation

## 📞 Soporte

Para soporte técnico o preguntas:
- 📧 Email: amarciales56@gmail.com
- 🐛 Issues: [GitHub Issues](https://github.com/PIPE1303/postgres-to-snowflake-data-pipeline/issues)
- 📖 Wiki: [Documentación completa](https://github.com/PIPE1303/postgres-to-snowflake-data-pipeline/wiki)

---

⭐ **¡Si este proyecto te fue útil, no olvides darle una estrella!** ⭐