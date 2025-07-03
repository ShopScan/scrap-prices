from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="dbt_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="DAG de prueba para ejecutar dbt con Postgres",
) as dag:

    init_dirs = BashOperator(
        task_id="init_dirs",
        bash_command="mkdir -p /opt/airflow/dbt/carrefour_dbt/logs /opt/airflow/dbt/carrefour_dbt/target && chmod -R 777 /opt/airflow/dbt/carrefour_dbt/logs /opt/airflow/dbt/carrefour_dbt/target 2>/dev/null || echo 'Warning: Could not change permissions (this is normal with mounted volumes)'",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/carrefour_dbt && dbt run --profiles-dir /opt/airflow/dbt 2>&1",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/carrefour_dbt && dbt test --profiles-dir /opt/airflow/dbt",
    )

    init_dirs >> dbt_run
    dbt_run >> dbt_test
