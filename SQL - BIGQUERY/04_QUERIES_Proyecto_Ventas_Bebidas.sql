-----------------------------------------------------------------------------------------------
-- CONSULTAS PARA ANÁLISIS DE VENTAS DE BEBIDAS
-- Este script contiene consultas SQL para analizar las ventas de bebidas, enfocándose en métricas de shopper insights y category management.
-----------------------------------------------------------------------------------------------

-- 1. MÉTRICAS DE SHOPPER INSIGHTS (HÁBITOS DE COMPRA)

-- A) TASA DE CONVERSIÓN POR CANALES:
SELECT CIUDAD, COUNT(DISTINCT(CLIENTE_ID)) AS CLIENTES FROM clientes GROUP BY CIUDAD;

SELECT 
    v.canal_id,
    p.marca,
    cl.ciudad,
    ca.nombre_canal,
    COUNT(DISTINCT v.cliente_id) AS CLIENTES_FACTURADOS,
    (COUNT(DISTINCT v.cliente_id) / (SELECT COUNT(DISTINCT(CLIENTE_ID)) FROM CLIENTES)) AS tasa_conversion
FROM BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.ventas v
LEFT JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.canales ca ON v.canal_id = ca.canal_id
LEFT JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.productos p ON v.producto_id = p.producto_id
LEFT JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.clientes cl ON v.cliente_id = cl.cliente_id
WHERE p.marca = 'Zulianita'
GROUP BY v.canal_id, ca.nombre_canal, p.marca, cl.ciudad;

-- B) TICKET PROMEDIO POR CANALES Y REGION:

SELECT
    r.nombre_region,
    c.nombre_canal,
    SUM(v.cantidad * hp.precio_base) / COUNT(DISTINCT v.venta_id) AS ticket_promedio
FROM   BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.ventas v
LEFT JOIN  BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.canales c ON v.canal_id = c.canal_id
LEFT JOIN  BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.regiones r ON v.region_id = r.region_id
LEFT JOIN  BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.historico_precios hp ON v.historico_precio_id = hp.historico_precio_id
LEFT JOIN  BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.productos p ON v.producto_id = p.producto_id
WHERE p.marca = 'Zulianita'
GROUP BY   r.nombre_region, c.nombre_canal;

-- C) FRECUENCIA DE COMPRA POR CLIENTES:

SELECT 
    c.cliente_id,
    c.nombre,
    COUNT(v.venta_id) AS frecuencia_compra
FROM BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.clientes c
LEFT JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.ventas v ON c.cliente_id = v.cliente_id
LEFT JOIN  BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.productos p ON v.producto_id = p.producto_id
WHERE p.marca = 'Zulianita'
GROUP BY c.cliente_id, c.nombre
ORDER BY FRECUENCIA_COMPRA DESC;


-- 2. MÉTRICAS DE CATEGORY MANAGEMENT

-- A) MARKET SHARE POR CATEGORÍA:

SELECT
    p.categoria,
    p.marca,
    SUM(v.cantidad * hp.precio_base) AS ventas_por_marca_categoria,
    SUM(SUM(v.cantidad * hp.precio_base)) OVER (PARTITION BY p.categoria) AS ventas_totales_categoria,
    (SUM(v.cantidad * hp.precio_base) * 1.0) / SUM(SUM(v.cantidad * hp.precio_base)) OVER (PARTITION BY p.categoria) AS market_share
FROM  BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.ventas v
LEFT JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.productos p ON v.producto_id = p.producto_id
LEFT JOIN  BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.historico_precios hp ON v.historico_precio_id = hp.historico_precio_id
GROUP BY
    p.categoria,
    p.marca
ORDER BY
    p.categoria,
    market_share DESC; 

-- B) ELASTICIDAD PRECIO-DEMANDA:

WITH VentasPorPeriodo AS (
    SELECT
        p.producto_id,
        p.nombre_producto,
        DATE_TRUNC('month', v.fecha) AS periodo_fecha,
        AVG(hp.precio_base) AS precio_promedio_periodo,
        SUM(v.cantidad) AS cantidad_total_periodo
    FROM BEBIDAS_ANALYTICS.ventas v
    LEFT JOIN BEBIDAS_ANALYTICS.productos p ON v.producto_id = p.producto_id
    LEFT JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.historico_precios hp ON v.historico_precio_id = hp.historico_precio_id
    WHERE p.marca = 'Zulianita'
    GROUP BY
        p.producto_id,
        p.nombre_producto,
        DATE_TRUNC('month', v.fecha)
),
DatosConPeriodoAnterior AS (
    SELECT
        producto_id,
        nombre_producto,
        periodo_fecha,
        precio_promedio_periodo AS P2,
        cantidad_total_periodo AS Q2,
        LAG(precio_promedio_periodo, 1) OVER (PARTITION BY producto_id ORDER BY periodo_fecha) AS P1,
        LAG(cantidad_total_periodo, 1) OVER (PARTITION BY producto_id ORDER BY periodo_fecha) AS Q1
    FROM
        VentasPorPeriodo
)
SELECT
    producto_id,
    nombre_producto,
    periodo_fecha,
    P1,
    Q1,
    P2,
    Q2,
  
    (Q2 - Q1) * 1.0 / NULLIF(((Q1 + Q2) / 2), 0) * 100.0 AS porcentaje_cambio_cantidad,
    
    (P2 - P1) * 1.0 / NULLIF(((P1 + P2) / 2), 0) * 100.0 AS porcentaje_cambio_precio,
  
    CASE

        WHEN ABS((P2 - P1) * 1.0 / NULLIF(((P1 + P2) / 2), 0)) < 0.0001 THEN NULL 
        WHEN NULLIF(((Q1 + Q2) / 2), 0) IS NULL OR NULLIF(((P1 + P2) / 2), 0) IS NULL THEN NULL
        ELSE ((Q2 - Q1) * 1.0 / ((Q1 + Q2) / 2)) / ((P2 - P1) * 1.0 / ((P1 + P2) / 2))
    END AS elasticidad_precio_demanda
FROM
    DatosConPeriodoAnterior
WHERE
    P1 IS NOT NULL
    AND Q1 IS NOT NULL
    AND P2 IS NOT NULL
    AND Q2 IS NOT NULL
ORDER BY
    producto_id,
    periodo_fecha;

 -- C)ROTACIÓN DE INVENTARIO:

SELECT
    r.nombre_region,
    p.producto_id,
    p.categoria,
    p.nombre_producto,
    SUM(v.cantidad) AS VENTAS,
    i.stock,
    CASE
        WHEN COALESCE(i.stock, 0) = 0 THEN NULL 
        ELSE COALESCE(SUM(v.cantidad), 0) / COALESCE(i.stock, 1) 
    END AS rotacion
FROM BEBIDAS_ANALYTICS.productos p
LEFT JOIN BEBIDAS_ANALYTICS.regiones r ON 1=1 
LEFT JOIN BEBIDAS_ANALYTICS.inventarios i
    ON p.producto_id = i.producto_id
    AND r.region_id = i.region_id
LEFT JOIN BEBIDAS_ANALYTICS.ventas v
    ON p.producto_id = v.producto_id
    AND r.region_id = v.region_id
    AND DATE_TRUNC('month', v.fecha) = DATE_TRUNC('month', i.fecha_actualizacion)
WHERE
    p.marca = 'Zulianita'
GROUP BY
    p.producto_id,
    p.nombre_producto,
    r.nombre_region,
    p.categoria,
    i.stock, 
    DATE_TRUNC('month', i.fecha_actualizacion)
HAVING i.stock IS NOT NULL
ORDER BY r.nombre_region, rotacion DESC;