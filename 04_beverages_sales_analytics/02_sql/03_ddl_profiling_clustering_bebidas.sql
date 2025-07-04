----------------------------------------------------------------------------------------------
-- DDL DE VISTA PARA PERFILAMIENTO DE CLIENTES (CLUSTERING)
----------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.VW_CLIENTES_PERFIL AS

WITH BaseMetrics AS (
    SELECT 
        v.cliente_id,
        v.fecha,
        v.venta_id,
        v.canal_id,
        cl.nombre_canal,
        v.producto_id,
        pr.nombre_producto,
        pr.categoria,
        v.cantidad,
        hp.precio_base,
        COALESCE(pro.descuento_porcentaje, 0) AS descuento_porcentaje,
        ROUND(
            CASE
                WHEN v.promocion_id IS NOT NULL THEN
                    hp.precio_base * (100 - pro.descuento_porcentaje) / 100 * v.cantidad
                ELSE
                    hp.precio_base * v.cantidad
            END, 2
        ) AS total_facturado,
        hp.costo_variable * v.cantidad AS costo_total,
        ROUND(
            ((ROUND(
                CASE
                    WHEN v.promocion_id IS NOT NULL THEN
                        hp.precio_base * (100 - pro.descuento_porcentaje) / 100 * v.cantidad
                    ELSE
                        hp.precio_base * v.cantidad
                END, 2) - hp.costo_variable * v.cantidad) /
                ROUND(
                    CASE
                        WHEN v.promocion_id IS NOT NULL THEN
                            hp.precio_base * (100 - pro.descuento_porcentaje) / 100 * v.cantidad
                        ELSE
                            hp.precio_base * v.cantidad
                    END, 2
                )) * 100, 1
        ) AS margen_porcentaje_neto
    FROM VENTAS v
    LEFT JOIN HISTORICO_PRECIOS hp ON v.historico_precio_id = hp.historico_precio_id
    LEFT JOIN PROMOCIONES pro ON v.promocion_id = pro.promocion_id
    LEFT JOIN CANALES cl ON v.canal_id = cl.canal_id
    LEFT JOIN PRODUCTOS pr ON v.producto_id = pr.producto_id
    WHERE v.producto_id IN (SELECT producto_id FROM PRODUCTOS WHERE MARCA = 'Zulianita')
),
CanalMasFrecuente AS (
    SELECT 
        cliente_id,
        nombre_canal AS canal_mas_frecuente
    FROM (
        SELECT 
            cliente_id,
            nombre_canal,
            COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY COUNT(*) DESC) AS rn
        FROM BaseMetrics
        GROUP BY cliente_id, nombre_canal
    ) t
    WHERE rn = 1
),
ProductoMasFrecuente AS (
    SELECT 
        cliente_id,
        nombre_producto AS producto_mas_frecuente
    FROM (
        SELECT 
            cliente_id,
            nombre_producto,
            COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY COUNT(*) DESC) AS rn
        FROM BaseMetrics
        GROUP BY cliente_id, nombre_producto
    ) t
    WHERE rn = 1
),
CategoriaMasFrecuente AS (
    SELECT 
        cliente_id,
        categoria AS categoria_mas_frecuente
    FROM (
        SELECT 
            cliente_id,
            categoria,
            COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY COUNT(*) DESC) AS rn
        FROM BaseMetrics
        GROUP BY cliente_id, categoria
    ) t
    WHERE rn = 1
),
AggregatedMetrics AS (
    SELECT 
        cliente_id,
        COUNT(venta_id) AS tickets_totales,
        COUNT(venta_id) / NULLIF(COUNT(DISTINCT DATE_TRUNC('MONTH', fecha)), 0) AS promedio_tickets_por_mes,
        SUM(cantidad) AS total_cantidades,
        SUM(cantidad) / NULLIF(COUNT(DISTINCT DATE_TRUNC('MONTH', fecha)), 0) AS promedio_cantidades_por_mes,
        AVG(descuento_porcentaje) AS promedio_descuentos_totales,
        SUM(total_facturado) AS total_facturado,
        SUM(total_facturado) / NULLIF(COUNT(DISTINCT DATE_TRUNC('MONTH', fecha)), 0) AS total_facturado_promedio_por_mes,
        AVG(margen_porcentaje_neto) AS promedio_margen_total,
        COUNT(DISTINCT canal_id) AS numero_canales_distintos,
        COUNT(DISTINCT producto_id) AS numero_productos_distintos,
        COUNT(DISTINCT categoria) AS numero_categorias_distintas,
        (DATEDIFF('DAY', MIN(fecha), MAX(fecha)) / NULLIF(COUNT(venta_id) - 1, 0)) AS frecuencia_compra_dias,
        (COUNT(CASE WHEN descuento_porcentaje > 0 THEN 1 END) / NULLIF(COUNT(venta_id), 0)) * 100 AS porcentaje_transacciones_con_descuento
    FROM BaseMetrics
    GROUP BY cliente_id
)
SELECT 
    a.cliente_id,
    clt.ciudad,
    clt.tiempo_cliente_meses,
    clt.dias_ultima_compra,
    a.tickets_totales,
    a.promedio_tickets_por_mes,
    c.canal_mas_frecuente,
    p.producto_mas_frecuente,
    cat.categoria_mas_frecuente,
    a.total_cantidades,
    a.promedio_cantidades_por_mes,
    a.promedio_descuentos_totales,
    a.total_facturado,
    a.total_facturado_promedio_por_mes,
    a.promedio_margen_total,
    a.numero_canales_distintos,
    a.numero_productos_distintos,
    a.numero_categorias_distintas,
    a.frecuencia_compra_dias,
    a.porcentaje_transacciones_con_descuento
FROM AggregatedMetrics a
LEFT JOIN CanalMasFrecuente c ON a.cliente_id = c.cliente_id
LEFT JOIN ProductoMasFrecuente p ON a.cliente_id = p.cliente_id
LEFT JOIN CategoriaMasFrecuente cat ON a.cliente_id = cat.cliente_id
LEFT JOIN vw_clientes_full clt ON a.cliente_id = clt.cliente_id
ORDER BY a.cliente_id;