#!/bin/bash
set -e

# Crear directorios si no existen y dar permisos (solo si es posible)
mkdir -p /opt/airflow/dbt/carrefour_dbt/logs /opt/airflow/dbt/carrefour_dbt/target
chmod -R 777 /opt/airflow/dbt/carrefour_dbt/logs /opt/airflow/dbt/carrefour_dbt/target 2>/dev/null || echo "Warning: Could not change permissions on dbt directories (this is normal with mounted volumes)"

# Inicializar la base de datos de Airflow (solo si no está ya)
airflow db upgrade

# Esperar a que la base de datos esté disponible (mejor que sleep fijo)
echo "Esperando a que postgres esté disponible..."
while ! pg_isready -h postgres -p 5432 -U airflow > /dev/null 2>&1; do
  sleep 1
done

# Crear conexión si no existe
if ! airflow connections get postgres_default > /dev/null 2>&1; then
  echo "Creando conexión postgres_default..."
  airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-port '5432' \
    --conn-schema 'playground'
else
  echo "Conexión postgres_default ya existe."
fi

# Ejecutar el comando original (ej: webserver, scheduler, etc.)
exec /entrypoint "$@"
