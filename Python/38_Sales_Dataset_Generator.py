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

# 1. Tabla Clientes (agregando nombres)
clientes = {
    'cliente_id': range(1, n_clientes + 1),
    'nombre': [fake.name() for _ in range(n_clientes)],  # Nombres realistas
    'edad': np.random.randint(18, 80, n_clientes),
    'genero': np.random.choice(['M', 'F'], n_clientes),
    'ciudad': [fake.city() for _ in range(n_clientes)],
    'frecuencia_compra': np.random.randint(1, 20, n_clientes),
    'ultima_compra': [fake.date_between(start_date='-2y', end_date='today') for _ in range(n_clientes)]
}
df_clientes = pd.DataFrame(clientes)

# 2. Tabla Productos
categorias = ['Gaseosa', 'Jugo', 'Bebida Energética', 'Agua']
marcas = ['Postobón', 'Competidor1', 'Competidor2']
productos = {
    'producto_id': range(1, n_productos + 1),
    'nombre_producto': [f"{random.choice(['Cola', 'Naranja', 'Manzana', 'Lima', 'Energía', 'Agua'])} {i}" for i in range(1, n_productos + 1)],
    'categoria': np.random.choice(categorias, n_productos),
    'precio_base': np.round(np.random.uniform(1000, 5000, n_productos), 2),
    'costo_variable': np.round(np.random.uniform(500, 3000, n_productos), 2),
    'marca': np.random.choice(marcas, n_productos)
}
df_productos = pd.DataFrame(productos)

# 3. Tabla Canales
canales = {
    'canal_id': range(1, n_canales + 1),
    'nombre_canal': ['Supermercado', 'Tienda de Conveniencia', 'E-commerce', 'Vending Machine', 'Hipermercado'],
    'tipo_canal': ['Físico', 'Físico', 'Online', 'Físico', 'Físico']
}
df_canales = pd.DataFrame(canales)

# 4. Tabla Regiones (usando department en lugar de state)
regiones = {
    'region_id': range(1, n_regiones + 1),
    'nombre_region': [fake.department() for _ in range(n_regiones)],  # Departamentos colombianos
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

# 7. Tabla Ventas
start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 6, 8)
ventas = {
    'venta_id': range(1, n_ventas + 1),
    'fecha': [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n_ventas)],
    'cliente_id': np.random.choice(df_clientes['cliente_id'], n_ventas),
    'producto_id': np.random.choice(df_productos['producto_id'], n_ventas),
    'cantidad': np.random.randint(1, 10, n_ventas),
    'precio_unitario': [df_productos.loc[df_productos['producto_id'] == pid, 'precio_base'].iloc[0] * (1 - random.uniform(0, 0.2)) for pid in np.random.choice(df_productos['producto_id'], n_ventas)],
    'canal_id': np.random.choice(df_canales['canal_id'], n_ventas),
    'region_id': np.random.choice(df_regiones['region_id'], n_ventas),
    'promocion_id': np.random.choice(df_promociones['promocion_id'], n_ventas, p=[0.1] * n_promociones)
}
df_ventas = pd.DataFrame(ventas)
df_ventas['precio_unitario'] = df_ventas['precio_unitario'].round(2)

# Guardar los datasets como CSV
df_clientes.to_csv('clientes.csv', index=False)
df_productos.to_csv('productos.csv', index=False)
df_canales.to_csv('canales.csv', index=False)
df_regiones.to_csv('regiones.csv', index=False)
df_promociones.to_csv('promociones.csv', index=False)
df_inventarios.to_csv('inventarios.csv', index=False)
df_ventas.to_csv('ventas.csv', index=False)

print("Datasets generados y guardados como CSV.")