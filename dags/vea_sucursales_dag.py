# === Standard Library ===
import os
import json
import tempfile
import logging
import asyncio
import csv
from datetime import datetime, timedelta

# === Third Party ===
from playwright.async_api import async_playwright
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# === Airflow ===
from airflow import DAG
from airflow.operators.python import PythonOperator

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === MinIO Config ===
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

attempt_provincia = 5

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'catchup': False
}

dag = DAG(
    'vea_sucursales_unificado',
    default_args=default_args,
    description='Extractor unificado de sucursales VEA con procesamiento por lotes',
    schedule_interval=timedelta(days=7),
    max_active_runs=1,
    tags=['vea', 'sucursales', 'scraping', 'playwright'],
)

create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.raw_sucursales_vea (
        id SERIAL PRIMARY KEY,
        sucursal_id TEXT NOT NULL,
        nombre TEXT NOT NULL,
        provincia TEXT NOT NULL,
        raw_data TEXT NOT NULL,
        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""


async def fetch_all_provinces():
    """
    Función asíncrona para extraer todas las provincias disponibles de VEA.
    Utiliza el algoritmo simple y confiable que funciona correctamente.
    """
    provincias_disponibles = []

    logger.info("Iniciando extracción de provincias disponibles...")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            logger.info("Navegando a VEA sucursales...")
            await page.goto("https://www.vea.com.ar/sucursales", timeout=30000)
            await page.wait_for_load_state('domcontentloaded')
            await page.wait_for_timeout(10000)
            
            # Buscar elementos select directamente (algoritmo simple y efectivo)
            selects = await page.query_selector_all('select')
            logger.info(f"Elementos select encontrados: {len(selects)}")
            
            # Procesar cada select para encontrar el de provincias
            for i, select in enumerate(selects):
                options = await select.query_selector_all('option')
                logger.info(f"Select {i}: {len(options)} opciones")
                
                # Solo procesar el select que tiene más opciones (probablemente provincias)
                if len(options) > 10:  # El de provincias tiene ~20 opciones
                    logger.info(f"Procesando select {i} con {len(options)} opciones (selector de provincias)")
                    
                    for j, option in enumerate(options):
                        value = await option.get_attribute('value')
                        text = await option.text_content()
                        
                        # Filtrar el placeholder "Provincia" y opciones vacías
                        if value and text and value != 'Provincia':
                            provincia_data = {
                                'codigo': value.strip(),
                                'nombre': text.strip()
                            }
                            provincias_disponibles.append(provincia_data)
                            logger.info(f"  {j}: {provincia_data['nombre']} ({provincia_data['codigo']})")
                    
                    # Una vez encontrado el select de provincias, no necesitamos seguir
                    break
            
            await browser.close()
            
            if provincias_disponibles:
                logger.info(f"Extracción exitosa: {len(provincias_disponibles)} provincias encontradas")
                return { 'provincias': provincias_disponibles }
            else:
                logger.error("No se encontraron provincias en ningún selector")
                return {}
            
        except Exception as e:
            logger.error(f"Error durante la extracción: {e}")
            await browser.close()
            return {}
        finally:
            await browser.close()

def search_provinces(**context):
    """
    Función de búsqueda de provincias que se ejecuta en el DAG.
    Utiliza la función asíncrona fetch_all_provinces para obtener los datos.
    """
    result = asyncio.run(fetch_all_provinces())
    
    if result:
        logger.info("Provincias extraídas correctamente.")
        context['ti'].xcom_push(key='provincias', value=result['provincias'])
        return result
    else:
        logger.error("No se pudieron extraer provincias.")
        return {}



async def process_branches(provinces):
    """
    Función de procesamiento de sucursales que se ejecuta en el DAG.
    Procesa las provincias para extraer las sucursales disponibles.
    """
    nombres = [provincia['nombre'] for provincia in provinces]

    sucursales_data = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
        for nombre in nombres:
            try:
                logger.info(f"Conectando a la página para provincia: {nombre}")
                await page.goto("https://www.vea.com.ar/sucursales", timeout=30000)
                await page.wait_for_load_state('domcontentloaded')
                await page.wait_for_timeout(12000)

                # Buscar elementos select directamente (usando el mismo algoritmo simple)
                selects = await page.query_selector_all('select')
                logger.info(f"Elementos select encontrados: {len(selects)}")
                
                provincia_select = None
                tienda_select = None
                
                # Identificar los selects (el primero suele ser provincias, el segundo tiendas)
                if len(selects) >= 2:
                    provincia_select = selects[0]  # Primer select = provincias
                    tienda_select = selects[1]     # Segundo select = tiendas
                
                if not provincia_select:
                    logger.error(f"No se encontró selector de provincias para {nombre}")
                    sucursales_data[nombre] = []
                    continue
                
                # Seleccionando la provincia
                await provincia_select.select_option(value=nombre)
                await page.wait_for_timeout(5000)

                # Si no se encuentra el select de tiendas, continuar con la siguiente provincia
                if not tienda_select:
                    logger.warning(f"No se encontró selector de tiendas para {nombre}")
                    sucursales_data[nombre] = []
                    continue

                # Obteniendo las opciones de tiendas
                tienda_options = await tienda_select.query_selector_all('option')
                tiendas_validas = []

                # Contando las tiendas válidas
                for opt in tienda_options:
                    value = await opt.get_attribute('value')
                    text = await opt.text_content()
                    if value and text and value.lower() != "tienda":
                        tienda_info = {
                            'codigo': value.strip(),
                            'nombre': text.strip()
                        }
                        tiendas_validas.append(tienda_info)
                
                logger.info(f"Provincia: {nombre}, Tiendas encontradas: {len(tiendas_validas)}")
                sucursales_data[nombre] = tiendas_validas
            except Exception as e:
                logger.error(f"Error procesando la provincia {nombre}: {e}")
                sucursales_data[nombre] = []
        await browser.close()
    return sucursales_data


def fetch_all_branches(**context):
    """
    Función para extraer sucursales de las provincias obtenidas del task anterior.
    """
    provinces = context['ti'].xcom_pull(task_ids='search_provinces', key='provincias')
    if provinces:
        logger.info(f"Procesando {len(provinces)} provincias para extraer sucursales...")
        branches = asyncio.run(process_branches(provinces=provinces))
        return branches
    else:
        logger.error("No se encontraron provincias para procesar sucursales.")
        return {}

async def process_information(stores):
    """
    Función para extraer información detallada de cada sucursal.
    Basada en la lógica de vea_unificado_completo.py
    """
    sucursales_completas = {}
    
    logger.info(f"Iniciando procesamiento detallado de {len(stores)} provincias")
    
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        
        await page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            for provincia_codigo, sucursales_lista in stores.items():
                logger.info(f"Procesando provincia: {provincia_codigo} con {len(sucursales_lista)} sucursales")
                sucursales_detalladas = []
                
                if not sucursales_lista:
                    logger.warning(f"No hay sucursales para procesar en {provincia_codigo}")
                    sucursales_completas[provincia_codigo] = []
                    continue
                
                # Navegar a la página para esta provincia
                await page.goto("https://www.vea.com.ar/sucursales", timeout=30000)
                await page.wait_for_load_state('domcontentloaded')
                await page.wait_for_timeout(10000)
                
                # Buscar elementos select
                selects = await page.query_selector_all('select')
                if len(selects) < 2:
                    logger.error(f"No se encontraron selectores suficientes para {provincia_codigo}")
                    sucursales_completas[provincia_codigo] = []
                    continue
                
                provincia_select = selects[0]  # Primer select = provincias
                tienda_select = selects[1]     # Segundo select = tiendas
                
                # Seleccionar la provincia
                try:
                    await provincia_select.select_option(value=provincia_codigo)
                    await page.wait_for_timeout(3000)
                except Exception as e:
                    logger.error(f"Error seleccionando provincia {provincia_codigo}: {e}")
                    sucursales_completas[provincia_codigo] = []
                    continue
                
                # Procesar cada sucursal de la provincia
                for sucursal_info in sucursales_lista:
                    try:
                        tienda_id = sucursal_info['codigo']
                        tienda_nombre = sucursal_info['nombre']
                        
                        logger.info(f"  Procesando sucursal: {tienda_nombre} (ID: {tienda_id})")
                        
                        # Seleccionar la tienda específica
                        await tienda_select.select_option(value=tienda_id)
                        await page.wait_for_timeout(2000)
                        
                        # Estructura base de la sucursal con información simplificada
                        sucursal_detallada = {
                            'id': tienda_id,
                            'nombre': tienda_nombre.strip(),
                            'provincia': provincia_codigo,
                            'raw_data': []  # Para almacenar datos crudos sin parsear
                        }
                        
                        # Extraer información detallada del elemento ul li p
                        try:
                            info_elements = await page.query_selector_all('ul li p')
                            
                            for p_element in info_elements:
                                try:
                                    text_content = await p_element.text_content()
                                    if text_content and text_content.strip():
                                        # Solo agregar el dato crudo sin parsear
                                        sucursal_detallada['raw_data'].append(text_content.strip())
                                
                                except Exception as e:
                                    logger.debug(f"Error procesando elemento individual: {e}")
                                    continue
                            
                            # También buscar otros elementos que puedan contener información
                            otros_elementos = await page.query_selector_all('div.store-info, .branch-details, .sucursal-info')
                            for elemento in otros_elementos:
                                try:
                                    texto = await elemento.text_content()
                                    if texto and texto.strip():
                                        sucursal_detallada['raw_data'].append(f"EXTRA: {texto.strip()}")
                                except:
                                    continue
                            
                        except Exception as e:
                            logger.warning(f"Error extrayendo información de {tienda_nombre}: {e}")
                        
                        sucursales_detalladas.append(sucursal_detallada)
                        
                        # Log simplificado del estado de extracción
                        logger.info(f"    ✅ {tienda_nombre} - Datos crudos: {len(sucursal_detallada['raw_data'])}")
                        
                    except Exception as e:
                        logger.error(f"Error procesando sucursal {tienda_nombre}: {e}")
                        # Agregar sucursal con información básica al menos
                        sucursales_detalladas.append({
                            'id': sucursal_info.get('codigo', ''),
                            'nombre': sucursal_info.get('nombre', ''),
                            'provincia': provincia_codigo,
                            'provincia_nombre': provincia_codigo,
                            'raw_data': [],
                            'error': str(e)
                        })
                        continue
                
                sucursales_completas[provincia_codigo] = sucursales_detalladas
                logger.info(f"Provincia {provincia_codigo} completada: {len(sucursales_detalladas)} sucursales procesadas")
        
        except Exception as e:
            logger.error(f"Error general en process_information: {e}")
        
        finally:
            await browser.close()
    
    # Generar estadísticas finales
    total_sucursales = sum(len(sucursales) for sucursales in sucursales_completas.values())
    con_direccion = sum(1 for provincia in sucursales_completas.values() 
                       for sucursal in provincia if sucursal.get('direccion'))
    con_telefono = sum(1 for provincia in sucursales_completas.values() 
                      for sucursal in provincia if sucursal.get('telefono'))
    return sucursales_completas


def fetch_information(**context):
    """
    Función para procesar la información detallada de las sucursales.
    Obtiene los datos del task anterior y ejecuta la extracción detallada.
    """
    stores = context['ti'].xcom_pull(task_ids='search_branches')
    if stores:
        logger.info(f"Procesando información detallada de {len(stores)} provincias...")
        detailed_info = asyncio.run(process_information(stores=stores))
        
        # Guardar los datos detallados en XCom para posible uso posterior
        context['ti'].xcom_push(key='detailed_stores', value=detailed_info)
        
        return detailed_info
    else:
        logger.error("No se encontraron sucursales para procesar información detallada.")
        return {}

def save_to_database(**context):
    """
    Función para guardar los datos extraídos en PostgreSQL.
    """
    detailed_stores = context['ti'].xcom_pull(task_ids='process_information', key='detailed_stores')
    
    if not detailed_stores:
        logger.error("No se encontraron datos para guardar en la base de datos.")
        return
    
    try:
        # Conectar a PostgreSQL usando Airflow Hook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Limpiar datos anteriores (opcional)
        pg_hook.run("TRUNCATE TABLE raw.raw_sucursales_vea;")
        logger.info("Tabla limpiada correctamente")
        
        total_insertados = 0
        
        # Procesar cada provincia y sus sucursales
        for provincia_codigo, sucursales in detailed_stores.items():
            logger.info(f"Guardando sucursales de provincia: {provincia_codigo}")
            
            for sucursal in sucursales:
                try:
                    # Convertir raw_data a JSON string
                    raw_data_json = json.dumps(sucursal.get('raw_data', []), ensure_ascii=False)
                    
                    # Preparar SQL de inserción
                    insert_sql = """
                        INSERT INTO raw.raw_sucursales_vea (sucursal_id, nombre, provincia, raw_data)
                        VALUES (%s, %s, %s, %s);
                    """
                    
                    # Ejecutar inserción
                    pg_hook.run(
                        insert_sql,
                        parameters=(
                            sucursal.get('id', ''),
                            sucursal.get('nombre', ''),
                            provincia_codigo,
                            raw_data_json
                        )
                    )
                    
                    total_insertados += 1
                    
                except Exception as e:
                    logger.error(f"Error insertando sucursal {sucursal.get('nombre', 'unknown')}: {e}")
                    continue
        
        logger.info(f"Proceso completado: {total_insertados} sucursales guardadas en la base de datos")
        
        # Verificar inserción
        result = pg_hook.get_first("SELECT COUNT(*) FROM raw.raw_sucursales_vea;")
        logger.info(f"Total de registros en la tabla: {result[0]}")
        
    except Exception as e:
        logger.error(f"Error general guardando en base de datos: {e}")
        raise

create_table_task = PostgresOperator(
    task_id="crear_tabla",
    postgres_conn_id="postgres_default",
    sql=create_table_sql,
    dag=dag,
)

search_provinces_task = PythonOperator(
    task_id='search_provinces',
    python_callable=search_provinces,
    dag=dag,
    do_xcom_push=True,
)

search_branches_task = PythonOperator(
    task_id='search_branches',
    python_callable=fetch_all_branches,
    dag=dag,
    do_xcom_push=True,
)

process_information_task = PythonOperator(
    task_id='process_information',
    python_callable=fetch_information,
    dag=dag,
    do_xcom_push=True,
)

save_data_task = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database,
    dag=dag,
)

create_table_task >> search_provinces_task >> search_branches_task >> process_information_task >> save_data_task