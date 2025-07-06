# === Standard Library ===
import os
import json
import tempfile
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List

# === Third Party ===
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from google.cloud import bigquery

# === Airflow ===
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Custom Imports ===
from src.jumbo_product_configs import JumboProductConfigs
from src.scraper import scrape_product_category

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Bucket Name ===
bucket_name = 'jumbo-dulce-de-leche-prices'

# === MinIO Config ===
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

# === BigQuery Config ===
GCP_PROJECT_ID = 'shop-scan-ar'
DATASET_DEV = 'scrap_prices_dev'

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
    'jumbo_dulce_de_leche_prices',
    default_args=default_args,
    max_active_runs=1,
    tags=['scraping', 'jumbo', 'dulce_de_leche']
)


# ====== Funciones del dag ======

async def scrape_jumbo_dulce_de_leche():
    """Scrapear dulce de leche de Jumbo"""
    logger.info("Iniciando scraping de Jumbo")
    
    try:
        config = JumboProductConfigs.get_dulce_de_leche_config()
        result = await scrape_product_category(config)

        logger.info(f"Jumbo - Páginas procesadas: {result.get('pages_processed', 0)}")
        logger.info(f"Jumbo - Total items encontrados: {result.get('total_items', 0)}")

        return result
    except Exception as e:
        logger.error(f"Error en scraping de Jumbo: {str(e)}")
        raise


def run_jumbo_scraping(**context):
    """Tarea de Airflow para scraping de Jumbo"""
    logger.info("Ejecutando scraping de Jumbo...")
    
    try:
        # Ejecutar el scraping asíncrono
        result = asyncio.run(scrape_jumbo_dulce_de_leche())
        
        # Obtener los productos
        products = result.get('items', [])      
        logger.info(f"Scraping de Jumbo completado: {len(products)} productos encontrados")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(result, temp_file, indent=2, ensure_ascii=False)
            temp_file_path = temp_file.name
        
        logger.info(f"JSON data saved to temporary file: {temp_file_path}")
        context['task_instance'].xcom_push(key='json_file_path', value=temp_file_path)
        return temp_file_path
        
    except Exception as e:
        logger.error(f"Error en scraping de Jumbo: {str(e)}")
        raise

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
        
        # Crear tabla de precios Jumbo dulce de leche
        table_id = f"{GCP_PROJECT_ID}.{DATASET_DEV}.raw_jumbo_dulce_leche_prices"
        
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
                    "store_name": "JUMBO",
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

    object_name = f"jumbo/dulce_de_leche/{datetime.now().strftime('%Y-%m-%d')}.json"

    try:
        s3_client.upload_file(json_file_path, bucket_name, object_name)
        logger.info(f"JSON file uploaded to MinIO: {object_name}")
    except (ClientError, EndpointConnectionError) as e:
        logger.error(f"Error uploading JSON file to MinIO: {str(e)}")
        raise



# ====== Definición de las tareas del DAG ======

check_bigquery_connection_task = PythonOperator(
    task_id='check_bigquery_connection',
    python_callable=check_bigquery_connection,
    dag=dag,
)

scrape_jumbo_task = PythonOperator(
    task_id='scrape_jumbo',
    python_callable=run_jumbo_scraping,
    dag=dag,
)

save_to_bigquery_task = PythonOperator(
    task_id='save_to_bigquery',
    python_callable=save_to_bigquery,
    dag=dag,
    provide_context=True,
)

# Dependencias: verificar BigQuery -> scrape -> subir a BigQuery
check_bigquery_connection_task >> scrape_jumbo_task >> save_to_bigquery_task
