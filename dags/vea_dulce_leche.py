from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
import json
import logging
from typing import Dict, List

from src.vea_product_configs import VeaProductConfigs
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
    'vea_dulce_de_leche_prices',
    default_args=default_args,
    description='Scraping comparativo de precios de dulce de leche entre Carrefour y Vea',
    schedule_interval='0 9 * * *',  # Ejecutar diariamente a las 9:00 AM
    max_active_runs=1,
    tags=['scraping', 'vea', 'dulce_de_leche']
)


# ====== Funciones del dag ======

async def scrape_vea_dulce_de_leche():
    """Scrapear dulce de leche de Vea"""
    logger.info("=== Iniciando scraping de Vea ===")
    
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
    """Tarea de Airflow para scraping de Vea"""
    logger.info("🏪 Ejecutando scraping de Vea...")
    
    try:
        # Ejecutar el scraping asíncrono
        result = asyncio.run(scrape_vea_dulce_de_leche())
        
        # Obtener los productos
        products = result.get('items', [])
        
        # Guardar resultado en XCom para la tarea de comparación
        context['ti'].xcom_push(key='vea_result', value=result)
        
        logger.info(f"✅ Scraping de Vea completado: {len(products)} productos encontrados")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error en scraping de Vea: {str(e)}")
        raise

def log_products_summary(**context):
    """Log de resumen de productos encontrados"""
    vea_result = context['ti'].xcom_pull(key='vea_result', task_ids='scrape_vea')

    if not vea_result:
        logger.info(f"No se encontraron resultados de Vea.")
        return
    
    products = vea_result.get('items', [])
    
    if not products:
        logger.info(f"No se encontraron productos en Vea.")
        return
    
    logger.info(f"Resumen de productos encontrados en Vea:")
    logger.info(f"Total de productos: {len(products)}")
    logger.info(f"Páginas procesadas: {vea_result.get('pages_processed', 0)}")
    logger.info(f"Total items encontrados: {vea_result.get('total_items', 0)}")

    # Optionally log first few products for debugging
    for i, product in enumerate(products[:3]):  # Show first 3 products
        logger.info(f"Producto {i+1}: {product.get('name', 'Sin nombre')} - ${product.get('price', 'Sin precio')}")



# ====== Definición de las tareas del DAG ======

scrape_vea_task = PythonOperator(
    task_id='scrape_vea',
    python_callable=run_vea_scraping,
    dag=dag
)

log_summary = PythonOperator(
    task_id='log_products_summary',
    python_callable=log_products_summary,
    dag=dag
)

scrape_vea_task >> log_summary
