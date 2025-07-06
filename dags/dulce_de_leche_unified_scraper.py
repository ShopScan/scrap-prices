from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====== Configuración del DAG ======
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'dulce_de_leche_unified',
    default_args=default_args,
    schedule_interval='0 10 * * *',  # Ejecutar diariamente a las 10:00 AM
    max_active_runs=1,
    tags=['scraping', 'dulce_de_leche', 'unified', 'all_stores'],
    description='DAG unificado para scraping de dulce de leche de todas las tiendas'
)

# ====== Funciones del DAG ======

def report_execution_status(**context):
    """
    Función que reporta el estado de todas las ejecuciones de dulce de leche
    """
    logger.info("=== REPORTE DE EJECUCIÓN DULCE DE LECHE ===")
    
    # Lista de DAGs que se ejecutaron y sus task_ids correspondientes
    executed_dags = {
        'vea_dulce_de_leche_prices': 'trigger_vea_scraping',
        'jumbo_dulce_de_leche_prices': 'trigger_jumbo_scraping', 
        'carrefour_dulce_de_leche_prices': 'trigger_carrefour_scraping'
    }
    
    # Obtener el estado de cada DAG desde los triggers
    dag_statuses = {}
    
    for dag_id, task_id in executed_dags.items():
        try:
            # Verificar si el trigger completó exitosamente
            from airflow.models import TaskInstance
            from airflow.utils.state import State
            
            # Obtener la instancia de tarea del trigger
            ti = context['task_instance']
            dag_run = ti.dag_run
            trigger_ti = dag_run.get_task_instance(task_id)
            
            if trigger_ti and trigger_ti.state == State.SUCCESS:
                dag_statuses[dag_id] = "COMPLETED"
            elif trigger_ti and trigger_ti.state == State.FAILED:
                dag_statuses[dag_id] = "FAILED"
            elif trigger_ti and trigger_ti.state == State.UP_FOR_RETRY:
                dag_statuses[dag_id] = "RETRYING"
            elif trigger_ti and trigger_ti.state == State.RUNNING:
                dag_statuses[dag_id] = "RUNNING"
            else:
                dag_statuses[dag_id] = "UNKNOWN"
                
        except Exception as e:
            logger.warning(f"No se pudo obtener estado para {dag_id}: {e}")
            dag_statuses[dag_id] = "ERROR"
    
    # Generar reporte
    logger.info("Estado de ejecución por tienda:")
    completed_count = 0
    failed_count = 0
    
    store_names = {
        'vea_dulce_de_leche_prices': 'VEA',
        'jumbo_dulce_de_leche_prices': 'JUMBO',
        'carrefour_dulce_de_leche_prices': 'CARREFOUR'
    }
    
    for dag_id, status in dag_statuses.items():
        store_name = store_names.get(dag_id, dag_id)
        if status in ["COMPLETED", "SUCCESS"]:
            logger.info(f"  ✅ {store_name}: COMPLETADO")
            completed_count += 1
        elif status in ["FAILED", "FAILURE"]:
            logger.info(f"  ❌ {store_name}: FALLÓ")
            failed_count += 1
        elif status == "RETRYING":
            logger.info(f"  🔄 {store_name}: REINTENTANDO")
        elif status == "RUNNING":
            logger.info(f"  ⏳ {store_name}: EJECUTÁNDOSE")
        else:
            logger.info(f"  ⚠️  {store_name}: {status}")
    
    # Resumen final
    total_stores = len(executed_dags)
    logger.info(f"\n=== RESUMEN FINAL ===")
    logger.info(f"Total de tiendas procesadas: {total_stores}")
    logger.info(f"Completadas exitosamente: {completed_count}")
    logger.info(f"Fallidas: {failed_count}")
    logger.info(f"Otras: {total_stores - completed_count - failed_count}")
    
    success_rate = (completed_count / total_stores) * 100 if total_stores > 0 else 0
    logger.info(f"Tasa de éxito: {success_rate:.1f}%")
    
    if completed_count == total_stores:
        logger.info("🎉 TODAS LAS TIENDAS COMPLETADAS EXITOSAMENTE")
    elif completed_count > 0:
        logger.info("⚠️ EJECUCIÓN PARCIALMENTE EXITOSA")
    else:
        logger.error("❌ TODAS LAS EJECUCIONES FALLARON")
    
    # Guardar resumen en XCom para posible uso posterior
    summary = {
        'total_stores': total_stores,
        'completed': completed_count,
        'failed': failed_count,
        'success_rate': success_rate,
        'execution_date': context['ds'],
        'detailed_status': dag_statuses
    }
    
    context['task_instance'].xcom_push(key='execution_summary', value=summary)
    
    return summary

def log_start_execution(**context):
    """
    Función que registra el inicio de la ejecución unificada
    """
    logger.info("🚀 INICIANDO SCRAPING UNIFICADO DE DULCE DE LECHE")
    logger.info("Tiendas a procesar:")
    logger.info("  • VEA")
    logger.info("  • JUMBO") 
    logger.info("  • CARREFOUR")
    logger.info(f"Fecha de ejecución: {context['ds']}")
    logger.info("=" * 50)
    
    return "STARTED"

# ====== Definición de las tareas del DAG ======

# Tarea inicial de logging
start_execution_task = PythonOperator(
    task_id='log_start_execution',
    python_callable=log_start_execution,
    dag=dag,
)

# Trigger para VEA - Espera a que termine antes de continuar
trigger_vea_dag = TriggerDagRunOperator(
    task_id='trigger_vea_scraping',
    trigger_dag_id='vea_dulce_de_leche_prices',
    wait_for_completion=True,  # Esperar a que termine para poder reportar estado
    execution_timeout=timedelta(hours=2),  # Timeout de 2 horas para el scraping
    dag=dag,
)

# Trigger para JUMBO - Espera a que termine antes de continuar
trigger_jumbo_dag = TriggerDagRunOperator(
    task_id='trigger_jumbo_scraping',
    trigger_dag_id='jumbo_dulce_de_leche_prices',
    wait_for_completion=True,  # Esperar a que termine para poder reportar estado
    execution_timeout=timedelta(hours=2),  # Timeout de 2 horas para el scraping
    dag=dag,
)

# Trigger para CARREFOUR - Espera a que termine antes de continuar
trigger_carrefour_dag = TriggerDagRunOperator(
    task_id='trigger_carrefour_scraping',
    trigger_dag_id='carrefour_dulce_de_leche_prices',
    wait_for_completion=True,  # Esperar a que termine para poder reportar estado
    execution_timeout=timedelta(hours=2),  # Timeout de 2 horas para el scraping
    dag=dag,
)

# Tarea final de reporte
report_status_task = PythonOperator(
    task_id='report_execution_status',
    python_callable=report_execution_status,
    dag=dag,
    provide_context=True,
)

# ====== Dependencias del DAG ======

# Inicio -> Triggers en paralelo (cada uno espera a que termine su DAG hijo)
start_execution_task >> [trigger_vea_dag, trigger_jumbo_dag, trigger_carrefour_dag]

# Todos los triggers -> Reporte final (ya que wait_for_completion=True)
[trigger_vea_dag, trigger_jumbo_dag, trigger_carrefour_dag] >> report_status_task
