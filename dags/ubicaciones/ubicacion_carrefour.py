# === Standard Library ===
import os
import json
import tempfile
import logging
import asyncio
from datetime import datetime, timedelta

# === Third Party ===
from playwright.async_api import async_playwright
from google.cloud import bigquery
import os


# === Airflow ===
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Bucket Name ===
bucket_name = 'carrefour-ubication'

# === MinIO Config ===
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9001')
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

url = 'https://www.carrefour.com.ar/sucursales'
container_urls = 'list ph3 mt0 valtech-carrefourar-store-locator-0-x-addressList'
container_ubication = 'valtech-carrefourar-store-locator-0-x-addressListItem'
container_ubication_kind = 'valtech-carrefourar-store-locator-0-x-InfoWindowDesc'
container_ubication_schedules = 'valtech-carrefourar-store-locator-0-x-InfoWindowStatus'

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'carrefour_ubication',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval='0 6 * * 1',  # Weekly on Mondays at 6 AM
    description='Scraping de ubicaciones de sucursales Carrefour',
    tags=['scraping', 'carrefour', 'ubicaciones']
)

DBT_PROJECT_PATH = '/opt/airflow/include/dbt'
DBT_PROFILES_PATH = '/opt/airflow/include/dbt'
GCP_PROJECT_ID = 'shop-scan-ar'
DATASET_DEV = 'scrap_prices_dev'
DATASET_PROD = 'scrap_prices_prod'

def check_bigquery_connection():
    """Verificar conexión a BigQuery"""
    try:
        # Configurar credenciales
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/shop-scan-ar-40e81820454a.json'
        
        # Crear cliente de BigQuery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        # Intentar hacer una consulta simple
        query = "SELECT 1 as test"
        query_job = client.query(query)
        results = query_job.result()
        
        logging.info("Conexión a BigQuery exitosa")
        return True
    except Exception as e:
        logging.error(f"Error conectando a BigQuery: {e}")
        raise

async def fetch_carrefour_ubication():
    ubications = []

    async with async_playwright() as pw:
        chrome = await pw.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
        page_browser = await chrome.new_page()
            
        await page_browser.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

        page_browser.set_default_timeout(60000)

        logger.info("Navigating to Carrefour ubication page")

        response = await page_browser.goto(url,wait_until='load', timeout=60000)
        await asyncio.sleep(5)

        if response.status == 200:
            logger.info("Successfully loaded Carrefour ubication page")
            content = await page_browser.content()            
            if container_urls in content:
                logger.info("Found container_urls in content, proceeding to extract locations")
                
                ubication_elements = await page_browser.query_selector_all(f'ul.{container_urls.replace(" ", ".")} li.{container_ubication}')
                logger.info(f"Found {len(ubication_elements)} location elements")
                
                for i, element in enumerate(ubication_elements):
                    try:
                        full_text = await element.inner_text()
                        logger.info(f"Location {i+1} full text: {full_text}")
                        ubications.append(full_text.strip())
                        logger.info(f"Successfully extracted location {i+1}")
                    except Exception as e:
                        logger.error(f"Error extracting location {i+1}: {str(e)}")
            else:
                logger.error(f"Container '{container_urls}' not found in page content")
        else:
            logger.error(f"Failed to load Carrefour ubication page, status code: {response.status}")
    
    logger.info(f"Total ubications found: {len(ubications)}")
    if ubications:
        logger.info(f'First ubication: {ubications[0]}')
        logger.info(f'All ubications: {json.dumps(ubications, indent=2, ensure_ascii=False)}')
    else:
        logger.warning("No ubications found")
    
    await page_browser.close()
    await chrome.close()
    
    return ubications

def fetch_carrefour_ubication_sync(**context):
    return asyncio.run(fetch_carrefour_ubication())

def save_ubications_to_bigquery(**context):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/shop-scan-ar-40e81820454a.json'
    ubications = context['task_instance'].xcom_pull(task_ids='fetch_carrefour_ubication')

    if not ubications:
        logger.warning("No ubications data to save")
        return False
    
    try:
        # Crear cliente de BigQuery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        dataset_dev = bigquery.Dataset(f"{GCP_PROJECT_ID}.{DATASET_DEV}")
        dataset_prod = bigquery.Dataset(f"{GCP_PROJECT_ID}.{DATASET_PROD}")
        
        try:
            client.create_dataset(dataset_dev, exists_ok=True)
            logging.info(f"Dataset {DATASET_DEV} creado/verificado")
        except Exception as e:
            logging.info(f"Dataset {DATASET_DEV} ya existe o error: {e}")
            
        try:
            client.create_dataset(dataset_prod, exists_ok=True)
            logging.info(f"Dataset {DATASET_PROD} creado/verificado")
        except Exception as e:
            logging.info(f"Dataset {DATASET_PROD} ya existe o error: {e}")
        
        # Crear tabla de ubicaciones con estructura apropiada
        table_id = f"{GCP_PROJECT_ID}.{DATASET_DEV}.raw_carrefour_ubicaciones"
        
        # Esquema de la tabla
        schema = [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ubicacion_texto", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("fecha_extraccion", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("fuente", "STRING", mode="REQUIRED"),
        ]
        
        # Crear tabla si no existe
        try:
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table, exists_ok=True)
            logging.info(f"Tabla {table_id} creada/verificada")
        except Exception as e:
            logging.info(f"Tabla ya existe o error: {e}")
        
        # Eliminar datos anteriores de la tabla
        try:
            delete_query = f"DELETE FROM `{table_id}` WHERE TRUE"
            query_job = client.query(delete_query)
            query_job.result()  # Esperar a que termine
            logging.info(f"Datos anteriores eliminados de la tabla {table_id}")
        except Exception as e:
            logging.warning(f"Error eliminando datos anteriores (puede ser que la tabla esté vacía): {e}")
        
        # Preparar datos para insertar
        timestamp_now = datetime.now().isoformat()
        rows_to_insert = []
        
        for i, ubicacion in enumerate(ubications):
            row = {
                "id": i + 1,
                "ubicacion_texto": ubicacion,
                "fecha_extraccion": timestamp_now,
                "fuente": "carrefour.com.ar/sucursales"
            }
            rows_to_insert.append(row)
        
        # Insertar datos
        if rows_to_insert:
            errors = client.insert_rows_json(table, rows_to_insert)
            if errors:
                logging.error(f"Errores al insertar datos: {errors}")
                raise Exception(f"Errores en BigQuery: {errors}")
            else:
                logging.info(f"Se insertaron {len(rows_to_insert)} ubicaciones correctamente en BigQuery")
                return True
        else:
            logging.warning("No hay datos para insertar")
            return False
        
    except Exception as e:
        logging.error(f"Error guardando ubicaciones en BigQuery: {e}")
        raise

# === Tasks ===

check_bigquery_connection_task = PythonOperator(
    task_id='check_bigquery_connection',
    python_callable=check_bigquery_connection,
    dag=dag,
)

fetch_carrefour_ubication_task = PythonOperator(
    task_id='fetch_carrefour_ubication',
    python_callable=fetch_carrefour_ubication_sync,
    dag=dag,
    provide_context=True,
)

save_ubications_task = PythonOperator(
    task_id='save_ubications_to_bigquery',
    python_callable=save_ubications_to_bigquery,
    dag=dag,
    provide_context=True,
)

dbt_run_int = BashOperator(
    task_id='dbt_run_int',
    bash_command=f"""
    cd {DBT_PROJECT_PATH} && \
    export DBT_PROFILES_DIR={DBT_PROFILES_PATH} && \
    export GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/shop-scan-ar-40e81820454a.json && \
    mkdir -p /tmp/dbt_logs /tmp/dbt_target && \
    dbt run --select int_carrefour_ubications --log-path /tmp/dbt_logs --target-path /tmp/dbt_target
    """,
    dag=dag,
)

# === Task Dependencies ===
check_bigquery_connection_task >> fetch_carrefour_ubication_task >> save_ubications_task >> dbt_run_int
