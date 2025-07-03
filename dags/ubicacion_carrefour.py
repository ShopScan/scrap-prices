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
from playwright.async_api import async_playwright


# === Airflow ===
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Bucket Name ===
bucket_name = 'carrefour-ubication'

# === MinIO Config ===
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
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
    tags=['scraping', 'carrefour', 'precios']
)

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

def create_bucket(**context):
    """Create MinIO bucket if it doesn't exist"""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name='us-east-1'
        )
        
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' already exists")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Created bucket '{bucket_name}'")
            else:
                raise e
        
        return True
        
    except EndpointConnectionError:
        logger.error(f"Cannot connect to MinIO endpoint: {minio_endpoint}")
        raise
    except Exception as e:
        logger.error(f"Error creating bucket: {str(e)}")
        raise

def save_ubications_to_minio(**context):
    """Save scraped ubications to MinIO"""
    try:
        ubications = context['task_instance'].xcom_pull(task_ids='fetch_carrefour_ubication')
        
        if not ubications:
            logger.warning("No ubications data to save")
            return False
        
        # Initialize MinIO client
        s3_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name='us-east-1'
        )
        
        
        # Prepare data with metadata
        data_to_save = {
            'timestamp': datetime.now().isoformat(),
            'total_ubications': len(ubications),
            'ubications': ubications
        }
        
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f'carrefour_ubications_{timestamp}.json'
        
        # Save to temporary file first
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as tmp_file:
            json.dump(data_to_save, tmp_file, indent=2, ensure_ascii=False)
            tmp_file_path = tmp_file.name
        
        try:
            # Upload to MinIO
            with open(tmp_file_path, 'rb') as data:
                s3_client.upload_fileobj(data, bucket_name, filename)
            logger.info(f"Successfully saved {len(ubications)} ubications to MinIO: {filename}")
            return True
        finally:
            # Clean up temporary file
            os.unlink(tmp_file_path)
            
    except Exception as e:
        logger.error(f"Error saving ubications to MinIO: {str(e)}")
        raise

# === Tasks ===
create_bucket_task = PythonOperator(
    task_id='create_bucket',
    python_callable=create_bucket,
    dag=dag,
)

fetch_carrefour_ubication_task = PythonOperator(
    task_id='fetch_carrefour_ubication',
    python_callable=fetch_carrefour_ubication_sync,
    dag=dag,
    provide_context=True,
)

save_ubications_task = PythonOperator(
    task_id='save_ubications',
    python_callable=save_ubications_to_minio,
    dag=dag,
    provide_context=True,
)

# === Task Dependencies ===
create_bucket_task >> fetch_carrefour_ubication_task >> save_ubications_task

