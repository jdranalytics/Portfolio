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
    precio_unitario FLOAT,
    canal_id INTEGER,
    region_id INTEGER,
    promocion_id INTEGER,
    FOREIGN KEY (cliente_id) REFERENCES bebidas_analytics.clientes(cliente_id),
    FOREIGN KEY (producto_id) REFERENCES bebidas_analytics.productos(producto_id),
    FOREIGN KEY (canal_id) REFERENCES bebidas_analytics.canales(canal_id),
    FOREIGN KEY (region_id) REFERENCES bebidas_analytics.regiones(region_id),
    FOREIGN KEY (promocion_id) REFERENCES bebidas_analytics.promociones(promocion_id)
);