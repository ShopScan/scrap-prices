WITH raw_sucursales AS (
    SELECT *
    FROM `shop-scan-ar.scrap_prices_dev.raw_vea_sucursales`
),

parsed_data AS (
    SELECT
        sucursal_id,
        nombre,
        provincia,
        JSON_EXTRACT_ARRAY(raw_data) AS raw_data_array,
        fecha_extraccion,
        fuente
    FROM raw_sucursales
),

extracted_info AS (
    SELECT
        sucursal_id,
        TRIM(nombre) AS nombre_sucursal,
        TRIM(provincia) AS provincia,
        
        -- Extraer información de direcciones y horarios de los datos crudos
        (
            SELECT STRING_AGG(
                CASE 
                    WHEN REGEXP_CONTAINS(raw_item, r'(?i)(calle|av\.|avenida|ruta|km|boulevard)') 
                    THEN TRIM(JSON_EXTRACT_SCALAR(raw_item, '$'))
                END, 
                ' | '
            )
            FROM UNNEST(raw_data_array) AS raw_item
            WHERE raw_item IS NOT NULL
        ) AS direcciones_encontradas,
        
        (
            SELECT STRING_AGG(
                CASE 
                    WHEN REGEXP_CONTAINS(raw_item, r'(?i)(lunes|martes|miércoles|jueves|viernes|sábado|domingo|\d{1,2}:\d{2})') 
                    THEN TRIM(JSON_EXTRACT_SCALAR(raw_item, '$'))
                END,
                ' | '
            )
            FROM UNNEST(raw_data_array) AS raw_item
            WHERE raw_item IS NOT NULL
        ) AS horarios_encontrados,
        
        (
            SELECT STRING_AGG(
                CASE 
                    WHEN REGEXP_CONTAINS(raw_item, r'(?i)(tel|teléfono|\d{3,4}[-\s]?\d{3,4}[-\s]?\d{3,4})') 
                    THEN TRIM(JSON_EXTRACT_SCALAR(raw_item, '$'))
                END,
                ' | '
            )
            FROM UNNEST(raw_data_array) AS raw_item
            WHERE raw_item IS NOT NULL
        ) AS telefonos_encontrados,
        
        -- Extraer primera dirección válida
        (
            SELECT TRIM(JSON_EXTRACT_SCALAR(raw_item, '$'))
            FROM UNNEST(raw_data_array) AS raw_item
            WHERE raw_item IS NOT NULL
                AND REGEXP_CONTAINS(raw_item, r'(?i)(calle|av\.|avenida|ruta|km|boulevard)')
            LIMIT 1
        ) AS direccion_principal,
        
        -- Extraer horario de apertura y cierre si está disponible
        REGEXP_EXTRACT(
            (
                SELECT TRIM(JSON_EXTRACT_SCALAR(raw_item, '$'))
                FROM UNNEST(raw_data_array) AS raw_item
                WHERE raw_item IS NOT NULL
                    AND REGEXP_CONTAINS(raw_item, r'\d{1,2}:\d{2}')
                LIMIT 1
            ), 
            r'(\d{1,2}:\d{2})'
        ) AS hora_apertura,
        
        REGEXP_EXTRACT(
            (
                SELECT TRIM(JSON_EXTRACT_SCALAR(raw_item, '$'))
                FROM UNNEST(raw_data_array) AS raw_item
                WHERE raw_item IS NOT NULL
                    AND REGEXP_CONTAINS(raw_item, r'\d{1,2}:\d{2}.*\d{1,2}:\d{2}')
                LIMIT 1
            ), 
            r'\d{1,2}:\d{2}.*?(\d{1,2}:\d{2})'
        ) AS hora_cierre,
        
        ARRAY_LENGTH(raw_data_array) AS cantidad_datos_crudos,
        fecha_extraccion,
        fuente
        
    FROM parsed_data
)

SELECT
    sucursal_id,
    nombre_sucursal,
    provincia,
    
    -- Campos de dirección
    direccion_principal,
    CASE 
        WHEN direccion_principal IS NOT NULL 
        THEN CONCAT(direccion_principal, ', ', provincia, ', Argentina')
        ELSE NULL
    END AS direccion_completa,
    
    -- Campos de horarios
    hora_apertura,
    hora_cierre,
    CASE 
        WHEN hora_apertura IS NOT NULL AND hora_cierre IS NOT NULL
        THEN CONCAT(hora_apertura, ' - ', hora_cierre)
        ELSE NULL
    END AS horario_completo,
    
    -- Información adicional encontrada
    direcciones_encontradas,
    horarios_encontrados,
    telefonos_encontrados,
    
    -- Metadatos
    cantidad_datos_crudos,
    DATE(fecha_extraccion) AS fecha_extraccion_date,
    TIME(fecha_extraccion) AS hora_extraccion,
    fuente,
    
    -- Indicadores de calidad de datos
    CASE 
        WHEN direccion_principal IS NOT NULL THEN 1 
        ELSE 0 
    END AS tiene_direccion,
    
    CASE 
        WHEN hora_apertura IS NOT NULL AND hora_cierre IS NOT NULL THEN 1 
        ELSE 0 
    END AS tiene_horarios,
    
    CASE 
        WHEN telefonos_encontrados IS NOT NULL THEN 1 
        ELSE 0 
    END AS tiene_telefono

FROM extracted_info
ORDER BY provincia, nombre_sucursal
