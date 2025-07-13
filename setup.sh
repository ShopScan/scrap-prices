#!/bin/bash

#=============================================================================
# SETUP AUTOMATIZADO DEL PROYECTO SCRAP PRICES
#=============================================================================

echo "Configurando proyecto Scrap Prices..."
echo ""

# Colores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Verificar prerrequisitos
echo -e "${BLUE}Verificando prerrequisitos...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}ERROR: Docker no está instalado${NC}"
    echo "Instala Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

if ! command -v docker compose &> /dev/null; then
    echo -e "${RED}ERROR: Docker Compose no está instalado${NC}"
    echo "Instala Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

echo -e "${GREEN}OK: Docker y Docker Compose están instalados${NC}"

# Configurar credenciales
echo ""
echo -e "${BLUE}Configurando credenciales...${NC}"

mkdir -p credentials

if [ ! -f "credentials/gcp-service-account.json" ]; then
    echo -e "${YELLOW}Archivo de credenciales GCP no encontrado${NC}"
    echo ""
    echo "Para obtener las credenciales:"
    echo "1. Ve a Google Cloud Console"
    echo "2. IAM & Admin > Service Accounts"
    echo "3. Crea un Service Account con rol BigQuery Admin"
    echo "4. Descarga el archivo JSON"
    echo "5. Guárdalo como: credentials/gcp-service-account.json"
    echo ""
    read -p "¿Has colocado el archivo de credenciales? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Setup cancelado${NC}"
        echo "Configura las credenciales y ejecuta: ./setup.sh"
        exit 1
    fi
fi

if [ ! -f "credentials/gcp-service-account.json" ]; then
    echo -e "${RED}Credenciales GCP no encontradas en credentials/gcp-service-account.json${NC}"
    exit 1
fi

echo -e "${GREEN}Credenciales GCP encontradas${NC}"

# Generar configuración
if [ ! -f ".env" ]; then
    echo ""
    echo -e "${BLUE}Generando configuración segura...${NC}"
    
    if [ -f "./generate_secrets.sh" ]; then
        ./generate_secrets.sh
    else
        echo -e "${YELLOW}generate_secrets.sh no encontrado, creando .env básico...${NC}"
        
        # Crear .env básico
        cat > .env << EOF
# Airflow Configuration
AIRFLOW_ADMIN_PASSWORD=admin123
AIRFLOW_UID=50000
AIRFLOW_GID=0

# MinIO Configuration
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow123
POSTGRES_DB=airflow

# GCP Configuration
GCP_PROJECT_ID=tu_project_id_aqui

# Other Configuration
PYTHONPATH=/opt/airflow
EOF
        echo -e "${GREEN}.env creado con valores por defecto${NC}"
    fi
else
    echo -e "${GREEN}Archivo .env ya existe${NC}"
fi

# Configurar Project ID automáticamente desde las credenciales
echo ""
echo -e "${BLUE}Configuración de Google Cloud Platform${NC}"
gcp_project=$(grep "project_id" credentials/gcp-service-account.json | cut -d'"' -f4)
current_project=$(grep "GCP_PROJECT_ID=" .env 2>/dev/null | cut -d'=' -f2)

if [ "$current_project" == "tu_project_id_aqui" ] || [ -z "$current_project" ]; then
    if [ -n "$gcp_project" ]; then
        sed -i "s/GCP_PROJECT_ID=.*/GCP_PROJECT_ID=$gcp_project/" .env
        echo -e "${GREEN}Project ID configurado automáticamente: $gcp_project${NC}"
    else
        read -p "Ingresa tu GCP Project ID: " GCP_PROJECT_ID
        if [ -n "$GCP_PROJECT_ID" ]; then
            sed -i "s/GCP_PROJECT_ID=.*/GCP_PROJECT_ID=$GCP_PROJECT_ID/" .env
            echo -e "${GREEN}Project ID configurado: $GCP_PROJECT_ID${NC}"
        fi
    fi
else
    echo -e "${GREEN}Project ID ya configurado: $current_project${NC}"
fi

# Construir e iniciar servicios
echo ""
echo -e "${BLUE}Construyendo contenedores...${NC}"
docker compose build

echo ""
echo -e "${BLUE}Iniciando servicios...${NC}"
docker compose up -d

# Esperar servicios
echo ""
echo -e "${BLUE}Esperando servicios (30s)...${NC}"
sleep 30

# Mostrar estado
echo ""
echo -e "${BLUE}Estado de servicios:${NC}"
docker compose ps

# Obtener credenciales
echo ""
echo -e "${BLUE}Obteniendo credenciales de servicios...${NC}"

if [ -f ".env" ]; then
    ADMIN_PASSWORD=$(grep "AIRFLOW_ADMIN_PASSWORD=" .env 2>/dev/null | cut -d'=' -f2 | tr -d '"' | tr -d ' ')
    MINIO_USER=$(grep "MINIO_ROOT_USER=" .env 2>/dev/null | cut -d'=' -f2 | tr -d '"' | tr -d ' ')
    MINIO_PASSWORD=$(grep "MINIO_ROOT_PASSWORD=" .env 2>/dev/null | cut -d'=' -f2 | tr -d '"' | tr -d ' ')
    
    # Verificar si las credenciales están vacías y asignar valores por defecto
    ADMIN_PASSWORD=${ADMIN_PASSWORD:-admin123}
    MINIO_USER=${MINIO_USER:-minioadmin}
    MINIO_PASSWORD=${MINIO_PASSWORD:-minioadmin123}
else
    echo -e "${YELLOW}Archivo .env no encontrado, usando valores por defecto${NC}"
    ADMIN_PASSWORD="admin123"
    MINIO_USER="minioadmin"
    MINIO_PASSWORD="minioadmin123"
fi

echo ""
echo -e "${GREEN}¡Setup completado exitosamente!${NC}"
echo ""
echo -e "${BLUE}Servicios disponibles:${NC}"
echo "• Airflow Web UI: http://localhost:8080"
echo "  Usuario: admin"
echo "  Password: $ADMIN_PASSWORD"
echo ""
echo "• MinIO Console: http://localhost:9001"
echo "  Usuario: $MINIO_USER"
echo "  Password: $MINIO_PASSWORD"
echo ""
echo -e "${YELLOW}Si Airflow no acepta las credenciales, ejecuta:${NC}"
echo "docker compose exec airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password $ADMIN_PASSWORD"
echo ""
echo -e "${BLUE}Próximos pasos:${NC}"
echo "1. Acceder a Airflow Web UI"
echo "2. Activar los DAGs necesarios"
echo "3. Monitorear ejecuciones"
echo ""
echo -e "${BLUE}Comandos útiles:${NC}"
echo "• Ver logs: docker compose logs -f"
echo "• Reiniciar: docker compose restart"
echo "• Detener: docker compose down"
echo "• Ver archivo .env: cat .env"
