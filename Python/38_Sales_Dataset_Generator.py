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
n_ventas = 10000  # Número de transacciones totales
n_clientes = 2000  # Número de clientes
n_productos = 20   # Número de productos
n_canales = 5      # Número de canales
n_promociones = 10 # Número de promociones
n_regiones = 10    # Número de regiones
n_inventarios = 100 # Registros de inventario

# 1. Tabla Regiones (generar ciudades primero)
regiones = {
    'region_id': range(1, n_regiones + 1),
    'nombre_region': [fake.department() for _ in range(n_regiones)],
    'ciudad': [fake.city() for _ in range(n_regiones)]
}
df_regiones = pd.DataFrame(regiones)

# 2. Tabla Clientes (usar ciudades de df_regiones para asegurar cobertura)
ciudades_regiones = df_regiones['ciudad'].tolist()
n_clientes_por_ciudad = max(1, n_clientes // n_regiones)  # Mínimo 1 cliente por ciudad
clientes = []
for ciudad in ciudades_regiones:
    n_clientes_ciudad = n_clientes_por_ciudad + random.randint(-50, 50)  # Variación aleatoria
    for _ in range(n_clientes_ciudad):
        clientes.append({
            'cliente_id': len(clientes) + 1,
            'nombre': fake.name(),
            'edad': np.random.randint(18, 80),
            'genero': np.random.choice(['M', 'F']),
            'ciudad': ciudad,
            'frecuencia_compra': np.random.randint(1, 20),
            'ultima_compra': fake.date_between(start_date='-2y', end_date='today')
        })
# Añadir clientes adicionales si n_clientes no se alcanza
if len(clientes) < n_clientes:
    n_extra = n_clientes - len(clientes)
    for _ in range(n_extra):
        clientes.append({
            'cliente_id': len(clientes) + 1,
            'nombre': fake.name(),
            'edad': np.random.randint(18, 80),
            'genero': np.random.choice(['M', 'F']),
            'ciudad': random.choice(ciudades_regiones),
            'frecuencia_compra': np.random.randint(1, 20),
            'ultima_compra': fake.date_between(start_date='-2y', end_date='today')
        })
df_clientes = pd.DataFrame(clientes).head(n_clientes)  # Limitar a n_clientes

# 3. Tabla Productos
volumenes = [250, 600, 1000, 2000]  # mL
cajas = [6, 12, 24]  # Unidades por caja
categorias = {
    'Cola': 'Gaseosa',
    'Naranja': 'Gaseosa',
    'Limón': 'Gaseosa',
    'Piña': 'Gaseosa',
    'Manzanita': 'Gaseosa',
    'Uva': 'Gaseosa',
    'Té': 'Bebida',
    'Naranja': 'Jugo',
    'Manzana': 'Jugo',
    'Lima': 'Jugo',
    'Energía': 'Bebida Energética',
    'Agua con Gas': 'Agua',
    'Agua Sin Gas': 'Agua',
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
    precio_factor = volumen / 1000
    costo_factor = precio_factor * 0.8
    precio_base = base_precio * precio_factor
    costo_variable = base_costo * costo_factor
    
    # Ajustar para caja
    caja_factor = caja / 12
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

# 4. Tabla Canales
canales = {
    'canal_id': range(1, n_canales + 1),
    'nombre_canal': ['Supermercado', 'Tienda de Conveniencia', 'E-commerce', 'Vending Machine', 'Hipermercado'],
    'tipo_canal': ['Físico', 'Físico', 'Online', 'Físico', 'Físico']
}
df_canales = pd.DataFrame(canales)

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

# 7. Tabla Ventas con cobertura mínima semanal
start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 6, 8)
n_weeks = (end_date - start_date).days // 7 + 1  # Número de semanas (aprox. 126)

# Generar ventas mínimas por ciudad y producto (una por semana)
ventas_minimas = []
for region_id, region in df_regiones.iterrows():
    ciudad = region['ciudad']
    # Filtrar clientes de la ciudad específica
    clientes_ciudad = df_clientes[df_clientes['ciudad'] == ciudad]['cliente_id']
    if len(clientes_ciudad) == 0:
        print(f"Advertencia: No hay clientes para la ciudad {ciudad} (region_id {region_id + 1}). Agregando cliente ficticio.")
        nuevo_cliente_id = df_clientes['cliente_id'].max() + 1
        df_clientes = pd.concat([df_clientes, pd.DataFrame({
            'cliente_id': [nuevo_cliente_id],
            'nombre': fake.name(),
            'edad': np.random.randint(18, 80),
            'genero': np.random.choice(['M', 'F']),
            'ciudad': ciudad,
            'frecuencia_compra': np.random.randint(1, 20),
            'ultima_compra': fake.date_between(start_date='-2y', end_date='today')
        })], ignore_index=True)
        clientes_ciudad = [nuevo_cliente_id]
    for producto_id in df_productos['producto_id']:
        for week in range(n_weeks):
            fecha = start_date + timedelta(days=week * 7)
            ventas_minimas.append({
                'fecha': fecha,
                'cliente_id': np.random.choice(clientes_ciudad),
                'producto_id': producto_id,
                'cantidad': np.random.randint(1, 10),
                'canal_id': np.random.choice(df_canales['canal_id']),
                'region_id': region_id + 1,
                'promocion_id': np.random.choice([None] + list(df_promociones['promocion_id']), p=[0.7] + [0.3/n_promociones]*n_promociones)
            })

df_ventas_minimas = pd.DataFrame(ventas_minimas)
n_min_ventas = len(df_ventas_minimas)

# Completar con ventas aleatorias hasta n_ventas
n_random_ventas = n_ventas - n_min_ventas
if n_random_ventas > 0:
    ventas_aleatorias = {
        'fecha': [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n_random_ventas)],
        'cliente_id': np.random.choice(df_clientes['cliente_id'], n_random_ventas),
        'producto_id': np.random.choice(df_productos['producto_id'], n_random_ventas),
        'cantidad': np.random.randint(1, 10, n_random_ventas),
        'canal_id': np.random.choice(df_canales['canal_id'], n_random_ventas),
        'region_id': np.random.choice(df_regiones['region_id'], n_random_ventas),
        'promocion_id': np.random.choice([None] + list(df_promociones['promocion_id']), n_random_ventas, p=[0.7] + [0.3/n_promociones]*n_promociones)
    }
    df_ventas_aleatorias = pd.DataFrame(ventas_aleatorias)
    df_ventas = pd.concat([df_ventas_minimas, df_ventas_aleatorias], ignore_index=True)
else:
    df_ventas = df_ventas_minimas

# Asegurar que el número total de ventas no exceda n_ventas
df_ventas = df_ventas.head(n_ventas)

# Guardar los datasets como CSV
df_clientes.to_csv('clientes.csv', index=False)
df_productos.to_csv('productos.csv', index=False)
df_canales.to_csv('canales.csv', index=False)
df_regiones.to_csv('regiones.csv', index=False)
df_promociones.to_csv('promociones.csv', index=False)
df_inventarios.to_csv('inventarios.csv', index=False)
df_ventas.to_csv('ventas.csv', index=False)

print(f"Datasets generados y guardados como CSV. Total ventas: {len(df_ventas)}")