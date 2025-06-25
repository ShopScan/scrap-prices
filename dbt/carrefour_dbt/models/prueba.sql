{{ config(materialized='view') }}

SELECT 
    id,
    nombre,
    descripcion,
    fecha_creacion,
    CASE 
        WHEN nombre LIKE '%A%' THEN 'Categoria A'
        WHEN nombre LIKE '%B%' THEN 'Categoria B'
        WHEN nombre LIKE '%C%' THEN 'Categoria C'
        ELSE 'Otra categoria'
    END as categoria,
    EXTRACT(YEAR FROM fecha_creacion) as año_creacion,
    EXTRACT(MONTH FROM fecha_creacion) as mes_creacion,
    LENGTH(descripcion) as longitud_descripcion
FROM public.ejemplo_tabla