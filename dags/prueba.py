from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
from playwright.async_api import async_playwright
import json
import os
import logging

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
    'carrefour_meat_prices_scraper',
    default_args=default_args,
    description='Scraping de precios de dulce de leche de Carrefour',
    schedule_interval='0 8 * * *',  # Ejecutar diariamente a las 8:00 AM
    max_active_runs=1,
    tags=['scraping', 'carrefour', 'precios']
)

# Directorio base para datos
DATA_DIR = '/opt/airflow/data/carrefour'

async def page_has_products(page):
    """Determinar si una página tiene productos de manera más precisa"""
    product_selectors = [
        '.valtech-carrefourar-search-result-3-x-galleryItem',
        'div[class*="galleryItem"]',
        'article'
    ]
    
    total_products = 0
    for selector in product_selectors:
        elements = await page.query_selector_all(selector)
        if len(elements) > 0:
            for element in elements[:5]:
                try:
                    text = await element.inner_text()
                    if '$' in text and len(text.strip()) > 20:
                        total_products += 1
                except:
                    continue
    
    no_results_selectors = [
        'text="Sin resultados"',
        'text="No se encontraron productos"',
        'text="No hay productos"',
        'text="Página no encontrada"',
        'text="404"',
        '[class*="titleNotFound"]',
        '[class*="not-found"]',
        '[class*="no-results"]',
        '[class*="empty"]'
    ]
    
    visible_no_results = False
    for selector in no_results_selectors:
        try:
            element = await page.query_selector(selector)
            if element:
                is_visible = await element.is_visible()
                if is_visible:
                    visible_no_results = True
                    break
        except:
            pass
    
    current_url = page.url
    invalid_url_indicators = ['404', 'error', 'not-found', 'pagina-no-encontrada']
    url_indicates_no_page = any(indicator in current_url.lower() for indicator in invalid_url_indicators)
    
    logger.info(f"Productos encontrados: {total_products}, Mensaje 'sin resultados' visible: {visible_no_results}, URL sospechosa: {url_indicates_no_page}")
    
    return total_products > 0 and not visible_no_results and not url_indicates_no_page

async def scrape_carrefour_products():
    """Función principal de scraping"""
    meat_items = []
    
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
        
        page_num = 1
        consecutive_empty_pages = 0
        max_empty_pages = 2
        
        logger.info("🚀 Iniciando recolección automática de TODAS las páginas disponibles...")
        logger.info(f"📋 Criterio de parada: {max_empty_pages} páginas vacías consecutivas")
        
        try:
            while True:
                url = f"https://www.carrefour.com.ar/Lacteos-y-productos-frescos/Dulce-de-leche?page={page_num}"
                logger.info(f"Procesando página {page_num}: {url}")
                
                try:
                    try:
                        response = await page_browser.goto(url, wait_until='load', timeout=45000)
                        logger.info(f"Página cargada con 'load'")
                    except:
                        try:
                            response = await page_browser.goto(url, wait_until='domcontentloaded', timeout=30000)
                            logger.info(f"Página cargada con 'domcontentloaded'")
                        except:
                            response = await page_browser.goto(url, timeout=20000)
                            logger.info(f"Página cargada sin wait_until")

                    await asyncio.sleep(5)
                    
                    if response and response.status != 200:
                        logger.error(f"Error HTTP {response.status} en página {page_num}")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_empty_pages:
                            logger.info(f"Demasiados errores consecutivos. Finalizando...")
                            break
                        page_num += 1
                        continue

                    has_products = await page_has_products(page_browser)
                    
                    if not has_products:
                        logger.info(f"Página {page_num}: No hay productos disponibles")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_empty_pages:
                            logger.info(f"Se encontraron {consecutive_empty_pages} páginas sin productos consecutivas. Finalizando...")
                            break
                        page_num += 1
                        continue
                    
                    logger.info(f"Página {page_num}: Productos detectados inicialmente. Procediendo con scroll...")
                    
                    max_attempts = 8
                    target_elements = 16
                    
                    await asyncio.sleep(3)
                    
                    current_elements = []
                    for attempt in range(max_attempts):
                        selector = '.valtech-carrefourar-search-result-3-x-galleryItem'
                        current_elements = await page_browser.query_selector_all(selector)
                        elements_count = len(current_elements)
                        
                        logger.info(f"Intento {attempt + 1}/{max_attempts}: {elements_count} elementos encontrados")
                        
                        if elements_count >= target_elements:
                            logger.info(f"¡Objetivo alcanzado! Se encontraron {elements_count} elementos (>= {target_elements})")
                            break
                        
                        if attempt >= 3 and elements_count == 0:
                            logger.info(f"Después de {attempt + 1} intentos, no se encontraron elementos. Página podría estar vacía.")
                            break
                        
                        if attempt < max_attempts - 1:
                            logger.info(f"Haciendo scroll estratégico para cargar más elementos...")
                            if attempt < 3:
                                await page_browser.evaluate("window.scrollBy(0, 1500)")
                                await asyncio.sleep(4)
                            else:
                                await page_browser.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(5)
                                
                                await page_browser.evaluate("window.scrollBy(0, -1000)")
                                await asyncio.sleep(3)
                                await page_browser.evaluate("window.scrollBy(0, 2000)")
                                await asyncio.sleep(4)
                            
                            await page_browser.evaluate("""
                                window.dispatchEvent(new Event('scroll'));
                                window.dispatchEvent(new Event('resize'));
                            """)
                            await asyncio.sleep(2)

                    if current_elements:
                        logger.info(f"Procesando {len(current_elements)} elementos en página {page_num}!")
                        
                        valid_products_found = 0
                        for i, element in enumerate(current_elements, 1):
                            try:
                                element_text = await element.inner_text()
                                
                                if '$' in element_text and len(element_text.strip()) > 10:
                                    meat_items.append(element_text)
                                    valid_products_found += 1
                                else:
                                    logger.debug(f"Elemento #{i} no parece ser un producto válido")
                            
                            except Exception as e:
                                logger.error(f"Error al procesar elemento #{i}: {e}")
                        
                        if valid_products_found == 0:
                            logger.info(f"No se encontraron productos válidos en página {page_num}")
                            consecutive_empty_pages += 1
                            if consecutive_empty_pages >= max_empty_pages:
                                logger.info(f"Se encontraron {consecutive_empty_pages} páginas sin productos válidos consecutivas. Finalizando...")
                                break
                        else:
                            consecutive_empty_pages = 0
                    else:
                        logger.info(f"No se encontraron elementos en página {page_num}")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_empty_pages:
                            logger.info(f"Se encontraron {consecutive_empty_pages} páginas sin elementos consecutivas. Finalizando...")
                            break
                    
                    logger.info(f"Total de items recolectados hasta ahora: {len(meat_items)}")
                    
                    await asyncio.sleep(3)
                    page_num += 1
                    
                except Exception as e:
                    logger.error(f"Error al procesar página {page_num}: {e}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_empty_pages:
                        logger.info(f"Demasiados errores consecutivos. Finalizando...")
                        break
                    page_num += 1
                    continue
        
        except Exception as e:
            logger.error(f"Error general durante la ejecución: {e}")
            raise
        
        finally:
            logger.info(f"PROCESO COMPLETADO")
            logger.info(f"Total de páginas procesadas: {page_num - 1}")
            logger.info(f"Total de items recolectados: {len(meat_items)}")
            
            await chrome.close()
    
    return {
        'items': meat_items,
        'pages_processed': page_num - 1,
        'total_items': len(meat_items),
        'timestamp': datetime.now().isoformat()
    }

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