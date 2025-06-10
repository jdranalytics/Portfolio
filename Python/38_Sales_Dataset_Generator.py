import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Inicializar Faker para datos realistas
fake = Faker('es_CO')  # Localización para Colombia
np.random.seed(42)
random.seed(42)

# Parámetros
n_ventas = 10000  # Número de transacciones
n_clientes = 2000  # Número de clientes
n_productos = 20   # Número de productos
n_canales = 5      # Número de canales
n_promociones = 10 # Número de promociones
n_regiones = 10    # Número de regiones
n_inventarios = 100 # Registros de inventario

# 1. Tabla Clientes
clientes = {
    'cliente_id': range(1, n_clientes + 1),
    'nombre': [fake.name() for _ in range(n_clientes)],
    'edad': np.random.randint(18, 80, n_clientes),
    'genero': np.random.choice(['M', 'F'], n_clientes),
    'ciudad': [fake.city() for _ in range(n_clientes)],
    'frecuencia_compra': np.random.randint(1, 20, n_clientes),
    'ultima_compra': [fake.date_between(start_date='-2y', end_date='today') for _ in range(n_clientes)]
}
df_clientes = pd.DataFrame(clientes)

# 2. Tabla Productos
volumenes = [250, 600, 1000, 2000]  # mL
cajas = [6, 12, 24]  # Unidades por caja
categorias = {
    'Cola': 'Gaseosa',
    'Naranja': 'Jugo',
    'Manzana': 'Jugo',
    'Lima': 'Jugo',
    'Energía': 'Bebida Energética',
    'Agua': 'Agua'
}
marcas = ['Mi Marca', 'Competidor1', 'Competidor2']

productos = []
base_precio = 3000  # Precio base para 1L
base_costo = 2000  # Costo base para 1L

for i in range(n_productos):
    sabor = random.choice(list(categorias.keys()))
    volumen = random.choice(volumenes)
    caja = random.choice(cajas)
    marca = random.choice(marcas)
    categoria = categorias[sabor]
    
    # Escalar precio y costo proporcionalmente al volumen (1L = 100%)
    precio_factor = volumen / 1000  # 250 mL = 0.25, 600 mL = 0.6, 2L = 2.0
    costo_factor = precio_factor * 0.8  # Costo es ~80% del precio proporcional
    precio_base = base_precio * precio_factor
    costo_variable = base_costo * costo_factor
    
    # Ajustar para caja (precio y costo por unidad, luego multiplicar por caja)
    caja_factor = caja / 12  # 12 como base
    precio_base *= caja_factor
    costo_variable *= caja_factor
    
    productos.append({
        'producto_id': i + 1,
        'nombre_producto': f"{sabor} {volumen} mL x {caja}",
        'categoria': categoria,
        'precio_base': round(precio_base, 2),
        'costo_variable': round(costo_variable, 2) if costo_variable < precio_base else round(precio_base * 0.7, 2),
        'marca': marca
    })

df_productos = pd.DataFrame(productos)

# 3. Tabla Canales
canales = {
    'canal_id': range(1, n_canales + 1),
    'nombre_canal': ['Supermercado', 'Tienda de Conveniencia', 'E-commerce', 'Vending Machine', 'Hipermercado'],
    'tipo_canal': ['Físico', 'Físico', 'Online', 'Físico', 'Físico']
}
df_canales = pd.DataFrame(canales)

# 4. Tabla Regiones
regiones = {
    'region_id': range(1, n_regiones + 1),
    'nombre_region': [fake.department() for _ in range(n_regiones)],
    'ciudad': [fake.city() for _ in range(n_regiones)]
}
df_regiones = pd.DataFrame(regiones)

# 5. Tabla Promociones
promociones = {
    'promocion_id': range(1, n_promociones + 1),
    'nombre_promocion': [f"Promo {i}" for i in range(1, n_promociones + 1)],
    'descuento_porcentaje': np.random.randint(5, 30, n_promociones),
    'fecha_inicio': [fake.date_between(start_date='-1y', end_date='today') for _ in range(n_promociones)],
    'fecha_fin': [(fake.date_between(start_date='-1y', end_date='today') + timedelta(days=30)) for _ in range(n_promociones)]
}
df_promociones = pd.DataFrame(promociones)

# 6. Tabla Inventarios
inventarios = {
    'inventario_id': range(1, n_inventarios + 1),
    'producto_id': np.random.choice(df_productos['producto_id'], n_inventarios),
    'region_id': np.random.choice(df_regiones['region_id'], n_inventarios),
    'stock': np.random.randint(50, 1000, n_inventarios),
    'fecha_actualizacion': [fake.date_between(start_date='-6m', end_date='today') for _ in range(n_inventarios)]
}
df_inventarios = pd.DataFrame(inventarios)

# 7. Tabla Ventas (sin precio_unitario en datos crudos)
start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 6, 8)
ventas = {
    'venta_id': range(1, n_ventas + 1),
    'fecha': [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n_ventas)],
    'cliente_id': np.random.choice(df_clientes['cliente_id'], n_ventas),
    'producto_id': np.random.choice(df_productos['producto_id'], n_ventas),
    'cantidad': np.random.randint(1, 10, n_ventas),
    'canal_id': np.random.choice(df_canales['canal_id'], n_ventas),
    'region_id': np.random.choice(df_regiones['region_id'], n_ventas),
    'promocion_id': np.random.choice([None] + list(df_promociones['promocion_id']), n_ventas, p=[0.7] + [0.3/n_promociones]*n_promociones)
}
df_ventas = pd.DataFrame(ventas)

# Calcular precio_unitario dinámicamente solo cuando se necesite
def calcular_precio_unitario(row):
    precio_base = df_productos.loc[df_productos['producto_id'] == row['producto_id'], 'precio_base'].iloc[0]
    descuento = 0
    if pd.notna(row['promocion_id']):
        descuento = df_promociones.loc[df_promociones['promocion_id'] == row['promocion_id'], 'descuento_porcentaje'].iloc[0] / 100
    precio_final = precio_base * (1 - random.uniform(0, 0.1) - descuento)  # Descuento aleatorio + promoción
    return round(precio_final, 2)

# Añadir precio_unitario como columna calculada (opcional para análisis, pero no en raw data)
df_ventas['precio_unitario'] = df_ventas.apply(calcular_precio_unitario, axis=1)

# Guardar los datasets como CSV
df_clientes.to_csv('clientes.csv', index=False)
df_productos.to_csv('productos.csv', index=False)
df_canales.to_csv('canales.csv', index=False)
df_regiones.to_csv('regiones.csv', index=False)
df_promociones.to_csv('promociones.csv', index=False)
df_inventarios.to_csv('inventarios.csv', index=False)
# Guardar ventas sin precio_unitario en raw data (opcional incluirlo como columna calculada)
df_ventas.drop(columns=['precio_unitario'], inplace=True)  # Eliminar de raw data
df_ventas.to_csv('ventas.csv', index=False)

print("Datasets generados y guardados como CSV.")