# === Standard Library ===
import os
import json
import tempfile
import logging
import asyncio
from datetime import datetime, timedelta

# === Third Party ===
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

# === Airflow ===
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Custom Imports ===
from src.carrefour_product_configs import CarrefourProductConfigs
from src.scraper import scrape_product_category

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Bucket Name ===
bucket_name = 'carrefour-dulce-de-leche-prices'

# === MinIO Config ===
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')


# ====== Configuración del DAG ======


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
    'carrefour_dulce_de_leche_prices',
    default_args=default_args,
    schedule_interval='0 8 * * *',  # Ejecutar diariamente a las 8:00 AM
    max_active_runs=1,
    tags=['scraping', 'carrefour', 'precios']
)

# ====== Funciones del dag ======


async def scrape_carrefour_dulce_de_leche():
    """Scrapear dulce de leche de Carrefour"""
    logger.info("Iniciando scraping de Carrefour")
    
    try:
        config = CarrefourProductConfigs.get_dulce_de_leche_config()
        result = await scrape_product_category(config)
        
        logger.info(f"Carrefour - Páginas procesadas: {result.get('pages_processed', 0)}")
        logger.info(f"Carrefour - Total items encontrados: {result.get('total_items', 0)}")
        
        return result
    except Exception as e:
        logger.error(f"Error en scraping de Carrefour: {str(e)}")
        raise


def run_carrefour_scraping(**context):
    """Tarea de Airflow para scraping de Carrefour"""
    logger.info("Ejecutando scraping de Carrefour...")
    
    try:
        # Ejecutar el scraping asíncrono
        result = asyncio.run(scrape_carrefour_dulce_de_leche())
        
        # Obtener los productos
        products = result.get('items', [])      
        logger.info(f"Scraping de Carrefour completado: {len(products)} productos encontrados")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(result, temp_file, indent=2, ensure_ascii=False)
            temp_file_path = temp_file.name
        
        logger.info(f"JSON data saved to temporary file: {temp_file_path}")
        context['task_instance'].xcom_push(key='json_file_path', value=temp_file_path)
        return temp_file_path
        
    except Exception as e:
        logger.error(f"Error en scraping de Carrefour: {str(e)}")
        raise
    

def create_bucket_if_not_exists(**context):
    """
    Create the MinIO bucket if it does not exist
    """
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.error(f"Error checking/creating bucket: {str(e)}")
            raise


def upload_json_to_minio(**context):
    """
    Upload the json file to MinIO bucket using boto3 directly
    """
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )

    json_file_path = context['task_instance'].xcom_pull(key='json_file_path')
    if not json_file_path:
        logger.error("No JSON file path found in XCom.")
        return

    object_name = f"carrefour/dulce_de_leche/{datetime.now().strftime('%Y-%m-%d')}.json"

    try:
        s3_client.upload_file(json_file_path, bucket_name, object_name)
        logger.info(f"JSON file uploaded to MinIO: {object_name}")
    except (ClientError, EndpointConnectionError) as e:
        logger.error(f"Error uploading JSON file to MinIO: {str(e)}")
        raise


# ====== Definición de las tareas del DAG ======
create_bucket_task = PythonOperator(
    task_id='create_bucket_if_not_exists',
    python_callable=create_bucket_if_not_exists,
    dag=dag,
)

scrape_task = PythonOperator(
    task_id='scrape_products',
    python_callable=run_carrefour_scraping,
    dag=dag,
)

upload_file_to_minio = PythonOperator(
    task_id='upload_json_to_minio',
    python_callable=upload_json_to_minio,
    dag=dag,
)

# Dependencias: crear bucket -> scrape -> subir a minio
create_bucket_task >> scrape_task >> upload_file_to_minio