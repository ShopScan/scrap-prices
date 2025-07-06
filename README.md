# Scraper de Precios - Supermercados Argentinos

Sistema automatizado de scraping de precios usando Apache Airflow, Docker y Google Cloud Platform.

## Arquitectura

- **Apache Airflow**: Orquestación de tareas  
- **Playwright**: Web scraping resistente  
- **PostgreSQL**: Base de datos de Airflow  
- **MinIO**: Almacenamiento de objetos  
- **BigQuery**: Data warehouse  
- **dbt**: Transformación de datos  
- **Docker**: Containerización  

## Instalación Rápida

### Prerrequisitos  
- Docker y Docker Compose  
- Cuenta de Google Cloud Platform  
- Service Account de GCP con permisos BigQuery  

### Setup Automático

```bash
# 1. Clonar repositorio
git clone https://github.com/tu-usuario/scrap-prices.git
cd scrap-prices

# 2. Configurar credenciales GCP
mkdir -p credentials
# Copia tu archivo JSON de GCP aquí: credentials/gcp-service-account.json

# 3. Setup completo automático
./setup.sh
```

El script `setup.sh` automáticamente:  
- Verifica prerrequisitos  
- Genera credenciales seguras  
- Configura Project ID de GCP  
- Construye e inicia servicios  

## Acceso a Servicios

Después del setup:

- **Airflow Web UI**: http://localhost:8080  
- **MinIO Console**: http://localhost:9001  

Las credenciales se muestran al final del setup.

## Uso

1. **Activar DAGs** en Airflow Web UI:  
   - `dulce_de_leche_unified`: Scraping unificado

2. **Monitorear** ejecuciones en la interfaz  
3. **Datos** almacenados automáticamente en BigQuery  

## Estructura del Proyecto

```
scrap-prices/
├── airflow/ # Docker config
├── dags/ # DAGs Airflow
│   ├── src/ # Código compartido
│   ├── dulce_de_leche/ # Scrapers específicos
│   └── utilidades/ # Utils
├── include/dbt/ # Transformaciones dbt
├── credentials/ # Credenciales (NO subir)
├── .env.example # Template variables
└── requirements.txt # Dependencias
```

## Comandos Útiles

```bash
# Ver logs
docker-compose logs -f

# Reiniciar servicios
docker-compose restart

# Detener todo
docker-compose down

# Debugging
docker-compose exec airflow-webserver bash
```

**AVISO**: Proyecto educativo. 