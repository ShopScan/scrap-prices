WITH raw_ubications AS (
    SELECT *
    FROM `shop-scan-ar.scrap_prices_dev.raw_carrefour_ubicaciones`
)

SELECT
    INITCAP(SPLIT(ubicacion_texto, '\n')[OFFSET(0)]) AS tipo,
    
    TRIM(SPLIT(SPLIT(ubicacion_texto, '\n')[OFFSET(1)], ',')[SAFE_OFFSET(0)]) AS direccion,
    TRIM(SPLIT(SPLIT(ubicacion_texto, '\n')[OFFSET(1)], ',')[SAFE_OFFSET(1)]) AS localidad,
    TRIM(SPLIT(SPLIT(ubicacion_texto, '\n')[OFFSET(1)], ',')[SAFE_OFFSET(2)]) AS provincia,
    
    REGEXP_EXTRACT(SPLIT(ubicacion_texto, '\n')[OFFSET(2)], r'(\d{2}:\d{2})') AS hora_apertura,
    REGEXP_EXTRACT(SPLIT(ubicacion_texto, '\n')[OFFSET(2)], r'\d{2}:\d{2}\s+a\s+(\d{2}:\d{2})') AS hora_cierre,
    
    CONCAT(
        TRIM(SPLIT(SPLIT(ubicacion_texto, '\n')[OFFSET(1)], ',')[SAFE_OFFSET(0)]),
        ' , ',
        TRIM(SPLIT(SPLIT(ubicacion_texto, '\n')[OFFSET(1)], ',')[SAFE_OFFSET(2)]),
        ' , Argentina'
    ) AS direccion_con_provincia,
    
    TIME(fecha_extraccion) AS hora_extraccion

FROM raw_ubications
