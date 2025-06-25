from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
import json
import os
import logging
# Importar la clase modular de scraping
from scrapers.carrefour_scraper import CarrefourScraper, ProductConfigs

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración por defecto del DAG
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

# Definición del DAG
dag = DAG(
    'dulce_de_leche_prices_scraper',
    default_args=default_args,
    description='Scraping de precios de dulce de leche de Carrefour',
    schedule_interval='0 8 * * *',  # Ejecutar diariamente a las 8:00 AM
    max_active_runs=1,
    tags=['scraping', 'carrefour', 'precios']
)

# Directorio base para datos
DATA_DIR = '/opt/airflow/data/carrefour'

# Función de compatibilidad que usa la clase CarrefourScraper
async def scrape_carrefour_products():
    """Función de compatibilidad para mantener la interfaz existente"""
    # Usar la configuración predefinida para dulce de leche
    config = ProductConfigs.get_dulce_de_leche_config()
    scraper = CarrefourScraper(config)
    return await scraper.scrape_products()

def run_scraping_task(**context):
    """Tarea de Airflow para ejecutar el scraping"""
    logger.info("Iniciando tarea de scraping...")
    
    try:
        # Ejecutar el scraping
        result = asyncio.run(scrape_carrefour_products())
        
        # Guardar resultado en XCom para la siguiente tarea
        context['task_instance'].xcom_push(key='scraping_result', value=result)
        
        logger.info(f"Scraping completado: {result['total_items']} items encontrados")
        return result
        
    except Exception as e:
        logger.error(f"Error en scraping: {e}")
        raise

def save_data_task(**context):
    """Tarea separada para guardar los datos"""
    logger.info("Iniciando tarea de guardado de datos...")
    
    # Crear directorio si no existe
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Obtener datos del scraping desde XCom
    scraping_result = context['task_instance'].xcom_pull(key='scraping_result', task_ids='scrape_products')
    
    if not scraping_result:
        raise ValueError("No se encontraron datos del scraping en XCom")
    
    # Preparar datos para guardar
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if scraping_result['items']:
        # Datos con productos
        data_to_save = {
            "metadata": {
                "total_items": scraping_result['total_items'],
                "pages_processed": scraping_result['pages_processed'],
                "timestamp": scraping_result['timestamp'],
                "execution_date": context['ds'],
                "dag_id": context['dag'].dag_id,
                "task_id": context['task'].task_id
            },
            "items": scraping_result['items']
        }
        filename = f'{DATA_DIR}/carrefour_products_{timestamp}_{scraping_result["total_items"]}_items.json'
    else:
        # Datos vacíos con metadata
        data_to_save = {
            "metadata": {
                "total_items": 0,
                "pages_processed": scraping_result['pages_processed'],
                "timestamp": scraping_result['timestamp'],
                "execution_date": context['ds'],
                "dag_id": context['dag'].dag_id,
                "task_id": context['task'].task_id,
                "reason": "No se encontraron productos o se detuvieron por páginas vacías consecutivas"
            },
            "items": []
        }
        filename = f'{DATA_DIR}/carrefour_products_{timestamp}_empty.json'
    
    # Guardar archivo JSON
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=4)
        
        logger.info(f"Datos guardados exitosamente en {filename}")
        
        # Devolver información sobre el archivo guardado
        file_info = {
            'filename': filename,
            'total_items': scraping_result['total_items'],
            'file_size': os.path.getsize(filename),
            'timestamp': timestamp
        }
        
        return file_info
        
    except Exception as e:
        logger.error(f"Error guardando archivo: {e}")
        raise

def send_notification_task(**context):
    """Tarea para enviar notificación del resultado"""
    logger.info("Enviando notificación...")
    
    # Obtener información del archivo guardado
    file_info = context['task_instance'].xcom_pull(task_ids='save_data')
    scraping_result = context['task_instance'].xcom_pull(key='scraping_result', task_ids='scrape_products')
    
    if file_info and scraping_result:
        message = f"""
        ✅ Scraping de Carrefour completado exitosamente
        
        📊 Resumen:
        - Items encontrados: {file_info['total_items']}
        - Páginas procesadas: {scraping_result['pages_processed']}
        - Archivo guardado: {file_info['filename']}
        - Tamaño del archivo: {file_info['file_size']} bytes
        - Timestamp: {file_info['timestamp']}
        
        🎯 Ejecución: {context['ds']} - DAG: {context['dag'].dag_id}
        """
        
        logger.info(message)
        
        # Aquí podrías agregar envío de email, Slack, etc.
        # Por ahora solo logueamos
        
        return {"status": "notification_sent", "message": message}
    else:
        error_msg = "Error: No se pudo obtener información para la notificación"
        logger.error(error_msg)
        raise ValueError(error_msg)

# Definición de las tareas (sin la tarea de instalación)

# Tarea 1: Ejecutar scraping
scrape_task = PythonOperator(
    task_id='scrape_products',
    python_callable=run_scraping_task,
    dag=dag
)

# Tarea 2: Guardar datos (separada del scraping)
save_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data_task,
    dag=dag
)

# Tarea 3: Enviar notificación
notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification_task,
    dag=dag
)

# Definir las dependencias de las tareas (sin la tarea de instalación)
scrape_task >> save_task >> notify_task