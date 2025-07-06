# === Standard Library ===
import os
import json
import time
import random
import tempfile
import logging
from datetime import datetime, timedelta

import requests
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

# === Airflow ===
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === MinIO Config ===
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')


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
    'save_random_json_to_minio',
    default_args=default_args,
    description='Fetch random JSON data and save to MinIO bucket',
    schedule_interval=timedelta(hours=1),  # Run every hour
    max_active_runs=1,
    tags=['data', 'bucket', 's3', 'minio', 'json'],
)

def fetch_random_json(**context):
    """
    Fetch random JSON data from a public API and save to a temp file.
    Returns the path to the saved JSON file.
    """
    random_apis = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://api.github.com/users/octocat",
        "https://httpbin.org/json",
        "https://official-joke-api.appspot.com/random_joke"
    ]
    selected_api = random.choice(random_apis)
    logger.info(f"Fetching JSON data from: {selected_api}")
    try:
        response = requests.get(selected_api, timeout=30)
        response.raise_for_status()
        json_data = response.json()
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(json_data, temp_file, indent=2, ensure_ascii=False)
            temp_file_path = temp_file.name
        logger.info(f"JSON data saved to temporary file: {temp_file_path}")
        # Store the file path in XCom for the next task
        context['task_instance'].xcom_push(key='json_file_path', value=temp_file_path)
        context['task_instance'].xcom_push(key='api_source', value=selected_api)
        return temp_file_path
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {selected_api}: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON from {selected_api}: {str(e)}")
        raise


def upload_json_to_minio(**context):
    """
    Upload the JSON file to MinIO bucket using boto3 directly
    """
    # Get the file path from the previous task
    json_file_path = context['task_instance'].xcom_pull(key='json_file_path', task_ids='fetch_json')
    api_source = context['task_instance'].xcom_pull(key='api_source', task_ids='fetch_json')
    if not json_file_path:
        raise ValueError("No JSON file path found in XCom")
    bucket_name = "random-json-data"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    api_name = api_source.split('/')[-1] if api_source else "unknown"
    s3_key = f"json_data/{timestamp}_{api_name}.json"
    logger.info(f"Uploading {json_file_path} to MinIO bucket: {bucket_name}/{s3_key}")
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name='us-east-1',
            config=boto3.session.Config(
                retries={'max_attempts': 3},
                read_timeout=60,
                connect_timeout=30
            )
        )
        # Test connection with retries
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"Testing MinIO connection (attempt {attempt + 1}/{max_retries})...")
                s3_client.list_buckets()
                logger.info("✅ MinIO connection successful!")
                break
            except EndpointConnectionError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection failed, retrying in 10 seconds... ({e})")
                    time.sleep(10)
                else:
                    logger.error(f"Failed to connect to MinIO after {max_retries} attempts")
                    raise
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"MinIO error, retrying in 10 seconds... ({e})")
                    time.sleep(10)
                else:
                    raise
        # Create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} already exists")
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logger.info(f"Creating bucket {bucket_name}")
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"✅ Bucket {bucket_name} created successfully")
            else:
                raise
        # Upload the file
        s3_client.upload_file(json_file_path, bucket_name, s3_key)
        logger.info(f"Successfully uploaded JSON to MinIO: s3://{bucket_name}/{s3_key}")
        # Clean up temporary file
        if os.path.exists(json_file_path):
            os.remove(json_file_path)
            logger.info(f"Cleaned up temporary file: {json_file_path}")
        return s3_key
    except Exception as e:
        logger.error(f"Error uploading to MinIO: {str(e)}")
        # Clean up temporary file even if upload fails
        if os.path.exists(json_file_path):
            os.remove(json_file_path)
        raise

def check_minio_health(**context):
    """
    Check if MinIO is healthy and accessible before running other tasks.
    """
    logger.info(f"🔍 Checking MinIO health at {minio_endpoint}")
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name='us-east-1',
        config=boto3.session.Config(
            retries={'max_attempts': 3},
            read_timeout=30,
            connect_timeout=15
        )
    )
    max_retries = 10
    for attempt in range(max_retries):
        try:
            logger.info(f"Health check attempt {attempt + 1}/{max_retries}...")
            buckets = s3_client.list_buckets()
            logger.info(f"✅ MinIO is healthy! Found {len(buckets.get('Buckets', []))} buckets")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"MinIO not ready yet, waiting 15 seconds... ({e})")
                time.sleep(15)
            else:
                logger.error(f"MinIO health check failed after {max_retries} attempts: {e}")
                raise
    return False


# Define tasks
check_minio_task = PythonOperator(
    task_id='check_minio_health',
    python_callable=check_minio_health,
    dag=dag,
)

fetch_json_task = PythonOperator(
    task_id='fetch_json',
    python_callable=fetch_random_json,
    dag=dag,
)

upload_to_minio_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_json_to_minio,
    dag=dag,
)

# Set task dependencies
check_minio_task >> fetch_json_task >> upload_to_minio_task

