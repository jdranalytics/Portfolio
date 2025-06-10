-- DDL PARA CARGAR TABLAS EN SNOWFLAKE PARA EL PROYECTO DE ANALÍTICA DE VENTAS DE BEBIDAS (SIMULACIÓN DE DATOS)
-- Este script crea un esquema y varias tablas necesarias para el análisis de ventas de Bebidas.

-- Crear esquema
CREATE SCHEMA IF NOT EXISTS bebidas_analytics;

-- Tabla: Clientes
CREATE TABLE IF NOT EXISTS bebidas_analytics.clientes (
    cliente_id INTEGER PRIMARY KEY,
    nombre VARCHAR(100),
    edad INTEGER,
    genero VARCHAR(1),
    ciudad VARCHAR(50),
    frecuencia_compra INTEGER,
    ultima_compra DATE
);

-- Tabla: Productos
CREATE TABLE IF NOT EXISTS bebidas_analytics.productos (
    producto_id INTEGER PRIMARY KEY,
    nombre_producto VARCHAR(50),
    categoria VARCHAR(50),
    precio_base FLOAT,
    costo_variable FLOAT,
    marca VARCHAR(50)
);

-- Tabla: Canales
CREATE TABLE IF NOT EXISTS bebidas_analytics.canales (
    canal_id INTEGER PRIMARY KEY,
    nombre_canal VARCHAR(50),
    tipo_canal VARCHAR(20)
);

-- Tabla: Regiones
CREATE TABLE IF NOT EXISTS bebidas_analytics.regiones (
    region_id INTEGER PRIMARY KEY,
    nombre_region VARCHAR(50),
    ciudad VARCHAR(50)
);

-- Tabla: Promociones
CREATE TABLE IF NOT EXISTS bebidas_analytics.promociones (
    promocion_id INTEGER PRIMARY KEY,
    nombre_promocion VARCHAR(50),
    descuento_porcentaje INTEGER,
    fecha_inicio DATE,
    fecha_fin DATE
);

-- Tabla: Inventarios
CREATE TABLE IF NOT EXISTS bebidas_analytics.inventarios (
    inventario_id INTEGER PRIMARY KEY,
    producto_id INTEGER,
    region_id INTEGER,
    stock INTEGER,
    fecha_actualizacion DATE,
    FOREIGN KEY (producto_id) REFERENCES bebidas_analytics.productos(producto_id),
    FOREIGN KEY (region_id) REFERENCES bebidas_analytics.regiones(region_id)
);

-- Tabla: Ventas
CREATE TABLE IF NOT EXISTS bebidas_analytics.ventas (
    venta_id INTEGER PRIMARY KEY,
    fecha TIMESTAMP,
    cliente_id INTEGER,
    producto_id INTEGER,
    cantidad INTEGER,
    canal_id INTEGER,
    region_id INTEGER,
    promocion_id INTEGER,
    FOREIGN KEY (cliente_id) REFERENCES bebidas_analytics.clientes(cliente_id),
    FOREIGN KEY (producto_id) REFERENCES bebidas_analytics.productos(producto_id),
    FOREIGN KEY (canal_id) REFERENCES bebidas_analytics.canales(canal_id),
    FOREIGN KEY (region_id) REFERENCES bebidas_analytics.regiones(region_id),
    FOREIGN KEY (promocion_id) REFERENCES bebidas_analytics.promociones(promocion_id)
);

-- Vista para Predicción de Ventas
CREATE OR REPLACE VIEW BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.vw_ventas_ml AS
SELECT 
    DATE_TRUNC('WEEK', v.fecha) AS semana,
    v.producto_id,
    v.region_id,
    SUM(v.cantidad) AS cantidad,
    p.categoria,
    p.marca,
    EXTRACT(WEEK FROM v.fecha) AS semana_del_ano,
    EXTRACT(DAYOFWEEK FROM v.fecha) AS dia_semana_promedio,
    AVG(v.precio_unitario) AS precio_promedio,
    -- Ventas de la semana anterior (lag)
    LAG(SUM(v.cantidad), 1) OVER (
        PARTITION BY v.producto_id, v.region_id 
        ORDER BY DATE_TRUNC('WEEK', v.fecha)
    ) AS ventas_lag1,
    -- Promedio móvil de las últimas 4 semanas
    AVG(SUM(v.cantidad)) OVER (
        PARTITION BY v.producto_id, v.region_id 
        ORDER BY DATE_TRUNC('WEEK', v.fecha)
        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
    ) AS ventas_promedio_movil,
    -- Indicador de festividad (simplificado: diciembre es festivo)
    CASE 
        WHEN EXTRACT(MONTH FROM v.fecha) = 12 THEN 1 
        ELSE 0 
    END AS es_festividad
FROM BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.ventas v
JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.productos p 
    ON v.producto_id = p.producto_id
GROUP BY 
    DATE_TRUNC('WEEK', v.fecha),
    v.producto_id,
    v.region_id,
    p.categoria,
    p.marca,
    EXTRACT(WEEK FROM v.fecha),
    EXTRACT(DAYOFWEEK FROM v.fecha),
    CASE WHEN EXTRACT(MONTH FROM v.fecha) = 12 THEN 1 ELSE 0 END;
    
-- Vista para Optimización de Precios
CREATE OR REPLACE VIEW BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.vw_precios_ml AS
SELECT 
    v.producto_id,
    v.precio_unitario AS precio,
    p.categoria,
    p.marca,
    CASE WHEN v.cantidad > 0 THEN 1 ELSE 0 END AS compra,
    MONTH(v.fecha) AS mes
FROM BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.ventas v
JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.productos p ON v.producto_id = p.producto_id;

-- Vista para Predicción de Rotación de Inventario
CREATE OR REPLACE VIEW BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.vw_inventario_ml AS
SELECT 
    i.producto_id,
    i.region_id,
    i.stock,
    SUM(v.cantidad) / AVG(i.stock) AS rotacion,
    AVG(v.cantidad) AS ventas_historicas,
    MONTH(v.fecha) AS mes
FROM BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.inventarios i
LEFT JOIN BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.ventas v ON i.producto_id = v.producto_id AND i.region_id = v.region_id
GROUP BY i.producto_id, i.region_id, i.stock, MONTH(v.fecha);