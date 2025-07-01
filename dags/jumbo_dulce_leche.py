from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
import json
import logging
from typing import Dict, List

from src.jumbo_product_configs import JumboProductConfigs
from src.scraper import scrape_product_category

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    schedule_interval='0 9 * * *',  # Ejecutar diariamente a las 9:00 AM
    max_active_runs=1,
    tags=['scraping', 'jumbo', 'dulce_de_leche']
)


# ====== Funciones del dag ======

async def scrape_jumbo_dulce_de_leche():
    """Scrapear dulce de leche de jumbo"""
    logger.info("=== Iniciando scraping de Jumbo ===")
    
    try:
        config = JumboProductConfigs.get_dulce_de_leche_config()
        result = await scrape_product_category(config)

        logger.info(f"Jumbo - Páginas procesadas: {result.get('pages_processed', 0)}")
        logger.info(f"Jumbo - Total items encontrados: {result.get('total_items', 0)}")

        return result
    except Exception as e:
        logger.error(f"Error en scraping de jumbo: {str(e)}")
        raise


def run_jumbo_scraping(**context):
    """Tarea de Airflow para scraping de jumbo"""
    logger.info("🏪 Ejecutando scraping de Jumbo...")
    
    try:
        # Ejecutar el scraping asíncrono
        result = asyncio.run(scrape_jumbo_dulce_de_leche())
        
        # Obtener los productos
        products = result.get('items', [])
        
        # Guardar resultado en XCom para la tarea de comparación
        context['ti'].xcom_push(key='jumbo_result', value=result)
        
        logger.info(f"✅ Scraping de jumbo completado: {len(products)} productos encontrados")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error en scraping de jumbo: {str(e)}")
        raise

def log_products_summary(**context):
    """Log de resumen de productos encontrados"""
    jumbo_result = context['ti'].xcom_pull(key='jumbo_result', task_ids='scrape_jumbo')

    if not jumbo_result:
        logger.info(f"No se encontraron resultados de jumbo.")
        return
    
    products = jumbo_result.get('items', [])
    
    if not products:
        logger.info(f"No se encontraron productos en jumbo.")
        return
    
    logger.info(f"Resumen de productos encontrados en jumbo:")
    logger.info(f"Total de productos: {len(products)}")
    logger.info(f"Páginas procesadas: {jumbo_result.get('pages_processed', 0)}")
    logger.info(f"Total items encontrados: {jumbo_result.get('total_items', 0)}")

    # Optionally log first few products for debugging
    for i, product in enumerate(products[:3]):  # Show first 3 products
        logger.info(f"Producto {i+1}: {product.get('name', 'Sin nombre')} - ${product.get('price', 'Sin precio')}")



# ====== Definición de las tareas del DAG ======

scrape_jumbo_task = PythonOperator(
    task_id='scrape_jumbo',
    python_callable=run_jumbo_scraping,
    dag=dag
)

log_summary = PythonOperator(
    task_id='log_products_summary',
    python_callable=log_products_summary,
    dag=dag
)

scrape_jumbo_task >> log_summary
