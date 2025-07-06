from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
import json
import logging
import tempfile
import requests

# === Bucket Configuration ===
import os
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from google.cloud import bigquery

# === Custom Imports ===
from src.vea_product_configs import VeaProductConfigs
from src.scraper import scrape_product_category

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Bucket Name ===
bucket_name = 'vea-dulce-de-leche-prices'

# === MinIO Config ===
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')


# ====== Configuración del DAG ======
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'vea_dulce_de_leche_prices',
    default_args=default_args,
    max_active_runs=1,
    tags=['scraping', 'vea', 'dulce_de_leche']
) 
# ====== Funciones del dag ======

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

async def scrape_vea_dulce_de_leche():
    """
    Scrapear dulce de leche de Vea
    """
    logger.info("Iniciando scraping de Vea")
    
    try:
        config = VeaProductConfigs.get_dulce_de_leche_config()
        result = await scrape_product_category(config)
        
        logger.info(f"Vea - Páginas procesadas: {result.get('pages_processed', 0)}")
        logger.info(f"Vea - Total items encontrados: {result.get('total_items', 0)}")
        
        return result
    except Exception as e:
        logger.error(f"Error en scraping de Vea: {str(e)}")
        raise

def run_vea_scraping(**context):
    """
    Tarea de Airflow para scraping de Vea
    """
    logger.info("Ejecutando scraping de Vea...")
    
    try:
        # Ejecutar el scraping asíncrono
        result = asyncio.run(scrape_vea_dulce_de_leche())
        
        # Obtener los productos
        products = result.get('items', [])      
        logger.info(f"Scraping de Vea completado: {len(products)} productos encontrados")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(result, temp_file, indent=2, ensure_ascii=False)
            temp_file_path = temp_file.name
        
        logger.info(f"JSON data saved to temporary file: {temp_file_path}")
        context['task_instance'].xcom_push(key='json_file_path', value=temp_file_path)
        return temp_file_path
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON from {selected_api}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Error en scraping de Vea: {str(e)}")
        raise

def save_to_bigquery(**context):
    """
    Función para guardar los datos de dulce de leche en BigQuery.
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/shop-scan-ar-40e81820454a.json'
    
    # Obtener el path del archivo JSON del XCom
    json_file_path = context['task_instance'].xcom_pull(key='json_file_path')
    if not json_file_path:
        logger.error("No se encontró el archivo JSON en XCom.")
        return False
    
    try:
        # Leer los datos del archivo JSON
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        products = data.get('items', [])
        if not products:
            logger.warning("No hay productos para guardar en BigQuery.")
            return False
        
        # Crear cliente de BigQuery
        client = bigquery.Client(project=GCP_PROJECT_ID)
        dataset_dev = bigquery.Dataset(f"{GCP_PROJECT_ID}.{DATASET_DEV}")
        
        try:
            client.create_dataset(dataset_dev, exists_ok=True)
            logging.info(f"Dataset {DATASET_DEV} creado/verificado")
        except Exception as e:
            logging.info(f"Dataset {DATASET_DEV} ya existe o error: {e}")
        
        # Crear tabla de precios VEA dulce de leche
        table_id = f"{GCP_PROJECT_ID}.{DATASET_DEV}.raw_vea_dulce_leche_prices"
        
        # Esquema de la tabla - datos crudos tal como vienen del scraping
        schema = [
            bigquery.SchemaField("raw_data", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("scraped_datetime", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("store_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
        ]
        
        # Crear tabla si no existe
        try:
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table, exists_ok=True)
            logging.info(f"Tabla {table_id} creada/verificada")
        except Exception as e:
            logging.info(f"Tabla ya existe o error: {e}")
        
        # Verificar que la tabla existe antes de insertar
        try:
            table = client.get_table(table_id)
            logging.info(f"Tabla {table_id} confirmada existente")
        except Exception as e:
            logging.error(f"Error verificando tabla: {e}")
            # Intentar crear la tabla nuevamente
            try:
                table = bigquery.Table(table_id, schema=schema)
                table = client.create_table(table)
                logging.info(f"Tabla {table_id} creada en segundo intento")
            except Exception as e2:
                logging.error(f"Error creando tabla en segundo intento: {e2}")
                raise
        
        # Preparar datos para insertar - datos crudos sin procesamiento
        timestamp_now = datetime.now().isoformat()
        date_now = datetime.now().date().isoformat()
        rows_to_insert = []
        
        for product in products:
            try:
                # Guardar el producto completo como JSON crudo
                raw_product_data = json.dumps(product, ensure_ascii=False)
                
                row = {
                    "raw_data": raw_product_data,
                    "scraped_date": date_now,
                    "scraped_datetime": timestamp_now,
                    "store_name": "VEA",
                    "category": "dulce_de_leche",
                }
                rows_to_insert.append(row)
                
            except Exception as e:
                logger.error(f"Error procesando producto: {e}")
                continue
        
        # Insertar datos
        if rows_to_insert:
            try:
                logging.info(f"Intentando insertar {len(rows_to_insert)} filas en {table_id}")
                errors = client.insert_rows_json(table, rows_to_insert)
                if errors:
                    logging.error(f"Errores al insertar datos: {errors}")
                    raise Exception(f"Errores en BigQuery: {errors}")
                else:
                    logging.info(f"Se insertaron {len(rows_to_insert)} productos correctamente en BigQuery")
                    return True
            except Exception as insert_error:
                logging.error(f"Error durante la inserción: {insert_error}")
                # Intentar obtener más información sobre la tabla
                try:
                    table_info = client.get_table(table_id)
                    logging.info(f"Información de tabla: {table_info.table_id}, Schema: {table_info.schema}")
                except Exception as table_error:
                    logging.error(f"Error obteniendo información de tabla: {table_error}")
                raise insert_error
        else:
            logging.warning("No hay datos para insertar")
            return False
        
    except Exception as e:
        logger.error(f"Error general guardando en BigQuery: {e}")
        raise

# ====== Definición de las tareas del DAG ======

check_bigquery_connection_task = PythonOperator(
    task_id='check_bigquery_connection',
    python_callable=check_bigquery_connection,
    dag=dag,
)

scrape_vea_task = PythonOperator(
    task_id='scrape_vea',
    python_callable=run_vea_scraping,
    dag=dag,
)

save_to_bigquery_task = PythonOperator(
    task_id='save_to_bigquery',
    python_callable=save_to_bigquery,
    dag=dag,
    provide_context=True,
)

check_bigquery_connection_task >> scrape_vea_task >> save_to_bigquery_task
