#!/bin/bash

# Script de inicialización para dbt
# Ejecutar este script para configurar dbt por primera vez

set -e

echo "🚀 Inicializando proyecto dbt..."

# Variables
DBT_PROJECT_PATH="/opt/airflow/include/dbt"
PROFILES_PATH="/opt/airflow/include/dbt"

# Configurar variables de entorno
export DBT_PROFILES_DIR=$PROFILES_PATH
export GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/shop-scan-ar-40e81820454a.json"

echo "📁 Directorio del proyecto dbt: $DBT_PROJECT_PATH"
echo "📁 Directorio de profiles: $PROFILES_PATH"
echo "🔐 Credenciales de GCP: $GOOGLE_APPLICATION_CREDENTIALS"

# Cambiar al directorio del proyecto
cd $DBT_PROJECT_PATH

echo "🔍 Verificando configuración de dbt..."
dbt debug

echo "📦 Instalando dependencias de dbt..."
dbt deps

echo "🏗️ Creando datasets en BigQuery si no existen..."
# Los datasets se crean mediante el DAG de Airflow

echo "✅ Inicialización completa!"
echo ""
echo "🎯 Próximos pasos:"
echo "   1. Ejecutar el DAG 'dbt_bigquery_transformation' en Airflow"
echo "   2. Verificar que los modelos se ejecuten correctamente"
echo "   3. Revisar la documentación generada con 'dbt docs'"
echo ""
echo "📖 Comandos útiles:"
echo "   dbt run              # Ejecutar todos los modelos"
echo "   dbt test             # Ejecutar tests"
echo "   dbt docs generate    # Generar documentación"
echo "   dbt docs serve       # Servir documentación en puerto 8080"
