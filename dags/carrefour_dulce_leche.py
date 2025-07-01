from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
import json
import os
import logging

from src.carrefour_product_configs import CarrefourProductConfigs
from src.scraper import scrape_product_category

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    logger.info("=== Iniciando scraping de Carrefour ===")
    
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
    logger.info("🛒 Ejecutando scraping de Carrefour...")
    
    try:
        # Ejecutar el scraping asíncrono
        result = asyncio.run(scrape_carrefour_dulce_de_leche())
        
        # Obtener los productos
        products = result.get('items', [])
        
        # Guardar resultado en XCom para la tarea de comparación
        context['ti'].xcom_push(key='carrefour_result', value=result)
        
        logger.info(f"✅ Scraping de Carrefour completado: {len(products)} productos encontrados")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error en scraping de Carrefour: {str(e)}")
        raise
    
def log_products_summary(**context):
    """Log de resumen de productos encontrados"""
    carrefour_result = context['ti'].xcom_pull(key='carrefour_result', task_ids='scrape_products')

    if not carrefour_result:
        logger.info(f"No se encontraron resultados de Carrefour.")
        return
    
    products = carrefour_result.get('items', [])
    
    if not products:
        logger.info(f"No se encontraron productos en Carrefour.")
        return
    
    logger.info(f"Resumen de productos encontrados en Carrefour:")
    logger.info(f"Total de productos: {len(products)}")
    logger.info(f"Páginas procesadas: {carrefour_result.get('pages_processed', 0)}")
    logger.info(f"Total items encontrados: {carrefour_result.get('total_items', 0)}")

    # Optionally log first few products for debugging
    for i, product in enumerate(products[:3]):  # Show first 3 products
        logger.info(f"Producto {i+1}: {product.get('name', 'Sin nombre')} - ${product.get('price', 'Sin precio')}")


# ====== Definición de las tareas del DAG ======

scrape_task = PythonOperator(
    task_id='scrape_products',
    python_callable=run_carrefour_scraping,
    dag=dag
)
log_task = PythonOperator(
    task_id='save_data',
    python_callable=log_products_summary,
    dag=dag
)

scrape_task >> log_task