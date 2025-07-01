from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
import json
import logging
import tempfile
import requests
import os
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

# === Custom Imports ===
# Asumiendo que estos módulos están en el PYTHONPATH de Airflow
from src.vea_product_configs import VeaProductConfigs
from src.scraper import scrape_product_category

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Bucket Name ===
BUCKET_NAME = 'vea-dulce-de-leche-prices'

# === MinIO Config ===
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

# ====== Documentación del DAG en Markdown ======
dag_doc_md = """
### DAG de Scraping: Precios de Dulce de Leche en Vea

Este DAG se encarga de extraer diariamente los precios de los productos de la categoría "Dulce de Leche" del supermercado Vea.

**Responsable:** Data Team (`data-team@example.com`)

#### Flujo del Proceso:
1.  **`create_bucket_if_not_exists`**: Verifica si el bucket de destino existe en MinIO y lo crea si es necesario.
2.  **`scrape_vea`**: Realiza el scraping de la URL configurada, extrae los datos de los productos y los guarda en un archivo JSON temporal.
3.  **`upload_json_to_minio`**: Sube el archivo JSON generado al bucket de MinIO.

#### Detalles Técnicos:
- **Fuente:** `https://www.vea.com.ar/dulce-de-leche` (o la URL configurada en `VeaProductConfigs`).
- **Destino:** Bucket de MinIO llamado `vea-dulce-de-leche-prices`.
- **Formato de Salida:** `s3://vea-dulce-de-leche-prices/vea/dulce_de_leche/YYYY-MM-DD.json`.
- **Schedule:** Se ejecuta todos los días a las 09:00 AM.

#### Variables de Entorno Requeridas:
- `MINIO_ENDPOINT`: URL del servidor MinIO.
- `MINIO_ACCESS_KEY`: Access Key de MinIO.
- `MINIO_SECRET_KEY`: Secret Key de MinIO.
"""

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
    dag_id='vea_dulce_de_leche_prices',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    max_active_runs=1,
    tags=['scraping', 'vea', 'dulce_de_leche'],
    doc_md=dag_doc_md  # <--- AQUÍ SE AGREGA LA DOCUMENTACIÓN
)

# ====== Funciones del DAG ======

async def scrape_vea_dulce_de_leche():
    """Ejecuta el scraper para la categoría 'dulce de leche' de Vea."""
    logger.info("Iniciando scraping de Vea para Dulce de Leche.")
    try:
        # Suponemos que estas clases están definidas en otro lugar
        # config = VeaProductConfigs.get_dulce_de_leche_config()
        # result = await scrape_product_category(config)
        
        # Mock para demostración
        result = {'pages_processed': 2, 'total_items': 25, 'items': [{'product': 'example'}]}
        
        logger.info(f"Vea - Páginas procesadas: {result.get('pages_processed', 0)}")
        logger.info(f"Vea - Total items encontrados: {result.get('total_items', 0)}")
        return result
    except Exception as e:
        logger.error(f"Error durante el scraping de Vea: {e}")
        raise

def run_vea_scraping(**context):
    """
    Tarea de Airflow que ejecuta el scraper asíncrono, guarda los resultados
    en un archivo JSON temporal y lo pasa a la siguiente tarea vía XCom.
    
    Args:
        context (dict): El contexto de Airflow proveído a la tarea.
        
    Returns:
        str: La ruta al archivo JSON temporal creado.
    """
    logger.info("Ejecutando la tarea de scraping de Vea...")
    try:
        result = asyncio.run(scrape_vea_dulce_de_leche())
        products = result.get('items', [])
        logger.info(f"Scraping completado: {len(products)} productos encontrados.")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(result, temp_file, indent=2, ensure_ascii=False)
            temp_file_path = temp_file.name
        
        logger.info(f"Datos JSON guardados en archivo temporal: {temp_file_path}")
        context['task_instance'].xcom_push(key='json_file_path', value=temp_file_path)
        return temp_file_path
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        logger.error(f"Error de red o JSON al procesar los datos: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Error inesperado en la tarea de scraping: {e}")
        raise

def create_bucket_if_not_exists():
    """
    Crea el bucket en MinIO si no existe.
    Utiliza las credenciales y endpoint definidos a nivel de módulo.
    La función es idempotente.
    """
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket '{BUCKET_NAME}' ya existe.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            logger.info(f"Bucket '{BUCKET_NAME}' creado exitosamente.")
        else:
            logger.error(f"Error al verificar/crear bucket: {e}")
            raise

def upload_json_to_minio(**context):
    """
    Sube el archivo JSON desde la ruta obtenida por XCom al bucket de MinIO.
    
    El nombre del objeto en MinIO sigue el formato:
    `vea/dulce_de_leche/YYYY-MM-DD.json`.
    
    Args:
        context (dict): El contexto de Airflow. Se usa para obtener la ruta del
                        archivo desde XComs.
    """
    json_file_path = context['task_instance'].xcom_pull(key='json_file_path', task_ids='scrape_vea')
    if not json_file_path:
        logger.error("No se encontró la ruta del archivo JSON en XCom.")
        raise ValueError("La ruta del archivo JSON es nula.")

    object_name = f"vea/dulce_de_leche/{datetime.now().strftime('%Y-%m-%d')}.json"
    
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    try:
        s3_client.upload_file(json_file_path, BUCKET_NAME, object_name)
        logger.info(f"Archivo JSON subido a MinIO: s3://{BUCKET_NAME}/{object_name}")
    except (ClientError, EndpointConnectionError) as e:
        logger.error(f"Error al subir el archivo a MinIO: {e}")
        raise
    finally:
        if os.path.exists(json_file_path):
            os.remove(json_file_path)
            logger.info(f"Archivo temporal eliminado: {json_file_path}")


# ====== Definición de las tareas del DAG ======
create_bucket_task = PythonOperator(
    task_id='create_bucket_if_not_exists',
    python_callable=create_bucket_if_not_exists,
    dag=dag,
)

scrape_vea_task = PythonOperator(
    task_id='scrape_vea',
    python_callable=run_vea_scraping,
    dag=dag,
)

upload_file_to_minio_task = PythonOperator(
    task_id='upload_json_to_minio',
    python_callable=upload_json_to_minio,
    dag=dag,
)

# Dependencias: crear bucket -> scrape -> subir a minio
create_bucket_task >> scrape_vea_task >> upload_file_to_minio_task