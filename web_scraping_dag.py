from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def scrape_web_data(**context):
    """Web scraping task"""
    url = "https://example.com"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract data
    data = []
    for item in soup.find_all('div', class_='item'):
        data.append({
            'title': item.find('h2').text if item.find('h2') else '',
            'description': item.find('p').text if item.find('p') else '',
            'timestamp': datetime.now()
        })
    
    # Store in temporary location
    df = pd.DataFrame(data)
    df.to_csv('/tmp/scraped_data.csv', index=False)
    return '/tmp/scraped_data.csv'

def store_data(**context):
    """Data storage task"""
    file_path = context['task_instance'].xcom_pull(task_ids='web_scraping_task')
    
    # Read scraped data
    df = pd.read_csv(file_path)
    
    # Store in database
    conn = sqlite3.connect('/tmp/scraped_data.db')
    df.to_sql('web_data', conn, if_exists='append', index=False)
    conn.close()
    
    print(f"Stored {len(df)} records in database")

# DAG configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    'web_scraping_pipeline',
    default_args=default_args,
    description='Web scraping and data storage pipeline',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

# Define tasks
scraping_task = PythonOperator(
    task_id='web_scraping_task',
    python_callable=scrape_web_data,
    dag=dag
)

storage_task = PythonOperator(
    task_id='data_storage_task',
    python_callable=store_data,
    dag=dag
)

cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='rm -f /tmp/scraped_data.csv',
    dag=dag
)

# Set task dependencies
scraping_task >> storage_task >> cleanup_task