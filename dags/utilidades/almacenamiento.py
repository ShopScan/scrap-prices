from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

create_table_sql = """
CREATE TABLE IF NOT EXISTS ejemplo_tabla (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""

create_data_sql = """
INSERT INTO ejemplo_tabla (nombre, descripcion)
VALUES 
('Producto A', 'Descripción del Producto A'),
('Producto B', 'Descripción del Producto B'),
('Producto C', 'Descripción del Producto C');
"""

with DAG("insertar_datos_pg", start_date=datetime(2025, 6, 17), catchup=False) as dag:

    crear_tabla = PostgresOperator(
        task_id="crear_tabla",
        postgres_conn_id="postgres_default",
        sql=create_table_sql
    )

    insertar_datos = PostgresOperator(
        task_id="insertar_datos",
        postgres_conn_id="postgres_default",
        sql=create_data_sql
    )

    crear_tabla >> insertar_datos




