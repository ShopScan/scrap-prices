#!/bin/bash

# =============================================================================
# GENERADOR DE CREDENCIALES SEGURAS
# =============================================================================
# Script para generar archivo .env con credenciales seguras automaticamente

echo "Generando credenciales seguras para Scrap Prices..."

# Verificar que openssl este disponible
if ! command -v openssl &> /dev/null; then
    echo "ERROR: openssl no esta instalado"
    echo "   Instala openssl: sudo apt install openssl (Ubuntu/Debian)"
    exit 1
fi

# Crear directorio de credenciales
mkdir -p credentials

# Generar archivo .env con valores seguros
cat > .env << EOF
# =============================================================================
# CONFIGURACION GENERADA AUTOMATICAMENTE
# =============================================================================
# Generado el: $(date)

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)
POSTGRES_DB=airflow

# Airflow Configuration
AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -base64 32)
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -base64 16 | tr -d "=+/" | cut -c1-12)
AIRFLOW_ADMIN_EMAIL=admin@localhost

# MinIO Object Storage
MINIO_ROOT_USER=minio$(openssl rand -hex 4)
MINIO_ROOT_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)

# Google Cloud Platform
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/credentials/gcp-service-account.json
GCP_PROJECT_ID=tu_project_id_aqui
EOF

echo "OK: Archivo .env generado con credenciales seguras"
echo ""
echo "Credenciales generadas:"
echo "   Usuario Admin: admin"
echo "   Password Admin: $(grep AIRFLOW_ADMIN_PASSWORD .env | cut -d'=' -f2)"
echo "   Usuario MinIO: $(grep MINIO_ROOT_USER .env | cut -d'=' -f2)"
echo ""
echo "IMPORTANTE:"
echo "   1. Coloca tu archivo de credenciales GCP en: ./credentials/gcp-service-account.json"
echo "   2. Edita .env y actualiza GCP_PROJECT_ID con tu Project ID real"
echo "   3. NUNCA subas el archivo .env a Git!"
echo ""
echo "Proximo paso: ./setup.sh"
