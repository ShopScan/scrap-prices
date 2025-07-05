# dbt BigQuery Setup

Este directorio contiene el proyecto dbt configurado para trabajar con BigQuery.

## Estructura del Proyecto

```
include/dbt/
├── dbt_project.yml      # Configuración principal del proyecto dbt
├── profiles.yml         # Configuración de conexión a BigQuery
├── models/
│   ├── staging/         # Modelos de staging (views)
│   │   ├── stg_raw_prices.sql
│   │   └── _sources.yml
│   ├── marts/           # Modelos finales (tables)
│   │   ├── mart_price_analytics.sql
│   │   └── mart_store_comparison.sql
│   └── schema.yml       # Documentación y tests de modelos
├── macros/
│   └── utils.sql        # Macros reutilizables
└── tests/               # Tests personalizados
```

## Configuración

### 1. Credenciales de BigQuery
- Las credenciales se encuentran en `/opt/airflow/shop-scan-ar-40e81820454a.json`
- El proyecto de GCP configurado es: `shop-scan-ar`
- Datasets:
  - Desarrollo: `scrap_prices_dev`
  - Producción: `scrap_prices_prod`

### 2. Modelos incluidos

#### Staging (`models/staging/`)
- **stg_raw_prices**: Limpia y estandariza los datos raw de scraping

#### Marts (`models/marts/`)
- **mart_price_analytics**: Análisis de tendencias de precios con cambios diarios y semanales
- **mart_store_comparison**: Comparación de precios entre tiendas para cada producto

### 3. Macros (`macros/`)
- **get_current_timestamp()**: Obtiene timestamp actual
- **clean_product_name()**: Limpia nombres de productos
- **calculate_price_change()**: Calcula cambios porcentuales de precios

## Comandos dbt Útiles

```bash
# Verificar configuración
dbt debug

# Instalar dependencias
dbt deps

# Ejecutar todos los modelos
dbt run

# Ejecutar solo staging
dbt run --select staging

# Ejecutar solo marts
dbt run --select marts

# Ejecutar tests
dbt test

# Generar documentación
dbt docs generate

# Servir documentación
dbt docs serve
```

## Variables de Entorno Necesarias

```bash
export DBT_PROFILES_DIR=/opt/airflow/include/dbt
export GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/shop-scan-ar-40e81820454a.json
```

## Datos de Ejemplo

El DAG crea automáticamente datos de ejemplo en BigQuery para testing:
- Productos: Carne de Res, Pollo Entero
- Tiendas: Carrefour, Coto
- Categoría: Carnes

## Flujo de Transformación

1. **Raw Data** → Tabla `scraped_prices` en BigQuery
2. **Staging** → `stg_raw_prices` (limpieza y estandarización)
3. **Analytics** → `mart_price_analytics` (tendencias de precios)
4. **Comparison** → `mart_store_comparison` (comparación entre tiendas)

## Monitoreo y Tests

- Tests de calidad de datos en cada modelo
- Validación de not_null en campos críticos
- Documentación automática de todos los modelos
