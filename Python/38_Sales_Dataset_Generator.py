import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from tqdm import tqdm # Import tqdm

# Init Faker
fake = Faker('es_CO')
np.random.seed(42)
random.seed(42)

# Parameters
n_ventas = 200000  
n_clientes = 2000
n_productos = 40
n_canales = 5
n_promociones = 20 
n_regiones = 10
n_inventarios = 100

start_date_data = datetime(2022, 1, 1)
end_date_data = datetime(2025, 12, 31) 

# Elasticity parameters
elasticidades_por_categoria = {
    'Agua': {'min': -0.2, 'max': -0.6},
    'Gaseosa': {'min': -0.5, 'max': -1.0},
    'Jugo': {'min': -0.6, 'max': -1.2},
    'Bebida de Té': {'min': -0.8, 'max': -1.8},
    'Bebida Energética': {'min': -1.2, 'max': -2.5}
}
prob_elasticidad_positiva = 0.02
rango_elasticidad_positiva = {'min': 0.05, 'max': 0.3}

# 1. Regions
ciudades_colombia = [
    {"nombre_region": "Cundinamarca", "ciudad": "Bogotá", "latitud": 4.7110, "longitud": -74.0721},
    {"nombre_region": "Antioquia", "ciudad": "Medellín", "latitud": 6.2442, "longitud": -75.5812},
    {"nombre_region": "Valle del Cauca", "ciudad": "Cali", "latitud": 3.4516, "longitud": -76.5320},
    {"nombre_region": "Atlántico", "ciudad": "Barranquilla", "latitud": 10.9685, "longitud": -74.7813},
    {"nombre_region": "Bolívar", "ciudad": "Cartagena", "latitud": 10.3910, "longitud": -75.4794},
    {"nombre_region": "Santander", "ciudad": "Bucaramanga", "latitud": 7.1254, "longitud": -73.1198},
    {"nombre_region": "Norte de Santander", "ciudad": "Cúcuta", "latitud": 7.8939, "longitud": -72.5078},
    {"nombre_region": "Tolima", "ciudad": "Ibagué", "latitud": 4.4389, "longitud": -75.2114},
    {"nombre_region": "Meta", "ciudad": "Villavicencio", "latitud": 4.1415, "longitud": -73.6268},
    {"nombre_region": "Boyacá", "ciudad": "Tunja", "latitud": 5.5352, "longitud": -73.3677}
]

regiones_data = []
for i in range(n_regiones):
    if i < len(ciudades_colombia):
        region_info = ciudades_colombia[i]
    else:
        region_info = random.choice(ciudades_colombia)
    
    regiones_data.append({
        'region_id': i + 1,
        'nombre_region': region_info['nombre_region'],
        'ciudad': region_info['ciudad'],
        'latitud': region_info['latitud'],
        'longitud': region_info['longitud']
    })
df_regiones = pd.DataFrame(regiones_data)

# Region weights
pesos_regiones = {
    "Bogotá": 0.25, "Medellín": 0.20, "Cali": 0.15, "Barranquilla": 0.15,
    "Cartagena": 0.10, "Bucaramanga": 0.05, "Cúcuta": 0.04,
    "Ibagué": 0.03, "Villavicencio": 0.02, "Tunja": 0.01
}
prob_regiones_para_seleccion = np.array([pesos_regiones.get(row['ciudad'], 0.01) for _, row in df_regiones.iterrows()])
prob_regiones_para_seleccion /= sum(prob_regiones_para_seleccion)
regiones_para_seleccion = df_regiones['region_id'].tolist()

# 2. Clients
ciudades_regiones = df_regiones['ciudad'].tolist()
clientes = []
for i in range(1, n_clientes + 1):
    clientes.append({
        'cliente_id': i,
        'nombre': fake.name(),
        'edad': np.random.randint(18, 80),
        'genero': np.random.choice(['M', 'F']),
        'ciudad': random.choice(ciudades_regiones),
        'frecuencia_compra': np.random.randint(1, 20),
        'ultima_compra': fake.date_between(start_date=start_date_data, end_date=end_date_data)
    })
df_clientes = pd.DataFrame(clientes)

# 3. Products
volumenes_gaseosa_jugo_ml = [250, 600, 1000, 2000]
unidades_por_caja_gaseosa_jugo = [6, 12, 24]
volumenes_energia_ml = [250, 500]
unidades_por_caja_energia = [4, 6]
volumenes_agua_ml = [500, 1000, 2000, 5000] 
unidades_por_caja_agua = [1, 6, 12] 

categorias_marcas_sabor_base = [
    {'categoria_base': 'Gaseosa', 'sabores': ['Cola', 'Naranja', 'Limón', 'Piña', 'Manzanita', 'Uva'], 'marcas': ['Zulianita', 'Competidor1']},
    {'categoria_base': 'Bebida de Té', 'sabores': ['Té Negro', 'Té Verde'], 'marcas': ['Zulianita', 'Competidor2']},
    {'categoria_base': 'Jugo', 'sabores': ['Jugo Naranja', 'Jugo Manzana', 'Jugo Lima'], 'marcas': ['Zulianita', 'Competidor1']},
    {'categoria_base': 'Bebida Energética', 'sabores': ['Energía Extrema', 'Power Up'], 'marcas': ['Zulianita', 'Competidor2']},
    {'categoria_base': 'Agua', 'sabores': ['Agua con Gas', 'Agua Sin Gas'], 'marcas': ['Zulianita', 'Competidor1']}
]

productos = []
for i in range(n_productos):
    tipo_prod_info = random.choice(categorias_marcas_sabor_base)
    categoria = tipo_prod_info['categoria_base']
    sabor = random.choice(tipo_prod_info['sabores'])
    marca = random.choice(tipo_prod_info['marcas'])

    nombre_producto = sabor
    volumen_ml_val = 0
    unidades_caja_val = 1
    
    if categoria == 'Gaseosa' or categoria == 'Jugo':
        volumen_ml_val = random.choice(volumenes_gaseosa_jugo_ml)
        unidades_caja_val = random.choice(unidades_por_caja_gaseosa_jugo)
        nombre_producto = f"{sabor} {volumen_ml_val}mL x {unidades_caja_val}uds"
    elif categoria == 'Bebida Energética':
        volumen_ml_val = random.choice(volumenes_energia_ml)
        unidades_caja_val = random.choice(unidades_por_caja_energia)
        nombre_producto = f"{sabor} {volumen_ml_val}mL x {unidades_caja_val}uds"
    elif categoria == 'Agua':
        volumen_ml_val = random.choice(volumenes_agua_ml)
        unidades_caja_val = random.choice(unidades_por_caja_agua)
        if volumen_ml_val >= 1000:
            nombre_producto = f"{sabor} {volumen_ml_val // 1000}L x {unidades_caja_val}uds"
        else:
            nombre_producto = f"{sabor} {volumen_ml_val}mL x {unidades_caja_val}uds"
    elif categoria == 'Bebida de Té': 
        volumen_ml_val = random.choice([300, 500, 1000])
        unidades_caja_val = random.choice([1, 6, 12])
        nombre_producto = f"{sabor} {volumen_ml_val}mL x {unidades_caja_val}uds"

    productos.append({
        'producto_id': i + 1,
        'nombre_producto': nombre_producto,
        'categoria': categoria,
        'marca': marca,
        'volumen_ml_base': volumen_ml_val,
        'unidades_caja_base': unidades_caja_val
    })
df_productos = pd.DataFrame(productos)

# 4. Price History
historico_precios = []
hist_precio_id_counter = 1

precios_por_ml_base_categoria = {
    'Agua': 2.5,
    'Gaseosa': 3.5,
    'Jugo': 4.0,
    'Bebida Energética': 10.0,
    'Bebida de Té': 6.0
}
costos_por_ml_base_categoria = {
    'Agua': 1.0,
    'Gaseosa': 1.8,
    'Jugo': 2.2,
    'Bebida Energética': 4.5,
    'Bebida de Té': 3.0
}

for _, producto in df_productos.iterrows():
    current_date = start_date_data.replace(day=1)
    
    volumen_para_calculo = producto['volumen_ml_base'] if producto['volumen_ml_base'] > 0 else 1000
    unidades_para_calculo = producto['unidades_caja_base'] if producto['unidades_caja_base'] > 0 else 1

    categoria = producto['categoria']
    
    base_precio_ml_actual = precios_por_ml_base_categoria.get(categoria, 3.0)
    base_costo_ml_actual = costos_por_ml_base_categoria.get(categoria, 1.8)

    initial_precio_unitario = round(base_precio_ml_actual * volumen_para_calculo * unidades_para_calculo, 2)
    initial_costo_variable = round(base_costo_ml_actual * volumen_para_calculo * unidades_para_calculo, 2)
    
    if initial_costo_variable >= initial_precio_unitario:
        initial_costo_variable = round(initial_precio_unitario * 0.7, 2)

    while current_date <= end_date_data:
        historico_precios.append({
            'historico_precio_id': hist_precio_id_counter,
            'producto_id': producto['producto_id'],
            'fecha_actualizacion': current_date.date(),
            'precio_base': initial_precio_unitario,
            'costo_variable': initial_costo_variable
        })
        hist_precio_id_counter += 1

        incremento_porcentaje_precio = random.uniform(0.00, 0.02)
        incremento_porcentaje_costo = random.uniform(0.00, 0.015)

        initial_precio_unitario = round(initial_precio_unitario * (1 + incremento_porcentaje_precio), 2)
        initial_costo_variable = round(initial_costo_variable * (1 + incremento_porcentaje_costo), 2)

        if initial_costo_variable >= initial_precio_unitario:
            initial_costo_variable = round(initial_precio_unitario * 0.7, 2)

        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

df_historico_precios = pd.DataFrame(historico_precios)
df_historico_precios['fecha_actualizacion'] = pd.to_datetime(df_historico_precios['fecha_actualizacion'])

# Index df_historico_precios for faster lookups
df_historico_precios_indexed = df_historico_precios.set_index('historico_precio_id')


# 5. Channels
canales = {
    'canal_id': range(1, n_canales + 1),
    'nombre_canal': ['Supermercado', 'Tienda de Conveniencia', 'E-commerce', 'Vending Machine', 'Hipermercado'],
    'tipo_canal': ['Físico', 'Físico', 'Online', 'Físico', 'Físico']
}
df_canales = pd.DataFrame(canales)

# 6. Promotions
promociones_data = [] 
for i in range(1, n_promociones + 1):
    fecha_inicio = fake.date_between(start_date=start_date_data - timedelta(days=180), end_date=end_date_data - timedelta(days=90))
    fecha_fin = fecha_inicio + timedelta(days=random.randint(30, 180))
    
    if fecha_fin > end_date_data.date():
        fecha_fin = end_date_data.date()

    promociones_data.append({
        'promocion_id': i,
        'nombre_promocion': f"Promo {i}",
        'descuento_porcentaje': np.random.randint(5, 30),
        'fecha_inicio': fecha_inicio,
        'fecha_fin': fecha_fin
    })
df_promociones = pd.DataFrame(promociones_data) 

# 7. Inventory
inventarios = {
    'inventario_id': range(1, n_inventarios + 1),
    'producto_id': np.random.choice(df_productos['producto_id'], n_inventarios),
    'region_id': np.random.choice(df_regiones['region_id'], n_inventarios),
    'stock': np.random.randint(50, 1000, n_inventarios),
    'fecha_actualizacion': [fake.date_between(start_date='-6m', end_date='today') for _ in range(n_inventarios)]
}
df_inventarios = pd.DataFrame(inventarios)


# 8. Sales
ventas_data_final = []
venta_id_counter_final = 1

# Store monthly product state for EPD calculation
last_product_monthly_state = {} # {(producto_id, 'YYYY-MM'): {'precio': P_avg, 'cantidad': Q_total}}

current_sale_date = start_date_data

# Calculate total days for tqdm progress bar
total_days_simulation = (end_date_data - start_date_data).days + 1

# Wrap the main date loop with tqdm for progress visualization
for day_offset in tqdm(range(total_days_simulation), desc="Generating Sales Data"):
    current_sale_date = start_date_data + timedelta(days=day_offset)
    
    month = current_sale_date.month
    year = current_sale_date.year
    
    total_days_in_period = (end_date_data - start_date_data).days + 1
    base_sales_per_day = n_ventas / total_days_in_period
    
    # Seasonal sales multiplier
    if month in [7, 8]:
        sales_multiplier = random.uniform(1.3, 1.6) 
    elif month == 12:
        sales_multiplier = random.uniform(2.0, 3.0) 
    else:
        sales_multiplier = random.uniform(0.8, 1.2) 

    num_sales_today = int(base_sales_per_day * sales_multiplier * random.uniform(0.9, 1.1)) 
    num_sales_today = max(1, num_sales_today) 

    # Product weights by season
    productos_pesos = []
    for _, prod in df_productos.iterrows():
        peso = 1.0 
        if month in [7, 8]: 
            if 'Gaseosa' in prod['categoria'] or 'Agua' in prod['categoria'] or 'Jugo' in prod['categoria']:
                peso *= 1.5 
            elif 'Bebida Energética' in prod['categoria']:
                peso *= 1.2 
        elif month == 12: 
            if 'Gaseosa' in prod['categoria'] or 'Jugo' in prod['categoria']:
                peso *= 1.8 
        productos_pesos.append(peso)
    productos_pesos = np.array(productos_pesos)
    if productos_pesos.sum() == 0:
        productos_pesos = np.ones_like(productos_pesos)
    productos_pesos /= productos_pesos.sum() 

    for _ in range(num_sales_today):
        cliente_id = np.random.choice(df_clientes['cliente_id'])
        producto_id = np.random.choice(df_productos['producto_id'], p=productos_pesos) 
        region_id = np.random.choice(regiones_para_seleccion, p=prob_regiones_para_seleccion)
        canal_id = np.random.choice(df_canales['canal_id'])
        
        promociones_validas_hoy = df_promociones[
            (df_promociones['fecha_inicio'] <= current_sale_date.date()) &
            (df_promociones['fecha_fin'] >= current_sale_date.date())
        ]
        
        promocion_id = None
        if not promociones_validas_hoy.empty and random.random() < 0.5: 
            promocion_id = np.random.choice(promociones_validas_hoy['promocion_id'])

        precio_registro = df_historico_precios[
            (df_historico_precios['producto_id'] == producto_id) &
            (df_historico_precios['fecha_actualizacion'].dt.date <= current_sale_date.date())
        ].sort_values(by='fecha_actualizacion', ascending=False).head(1)

        if precio_registro.empty:
            continue

        precio_actual = precio_registro['precio_base'].iloc[0]
        historico_precio_id = precio_registro['historico_precio_id'].iloc[0]

        # Quantity based on EPD
        categoria_producto = df_productos[df_productos['producto_id'] == producto_id]['categoria'].iloc[0]
        rango_epd = elasticidades_por_categoria.get(categoria_producto, {'min': -0.5, 'max': -1.5}) 
        
        epd_objetivo = random.uniform(rango_epd['min'], rango_epd['max'])
        if random.random() < prob_elasticidad_positiva:
             epd_objetivo = random.uniform(rango_elasticidad_positiva['min'], rango_elasticidad_positiva['max'])

        # Previous month's state for EPD
        prev_month_key = (producto_id, (current_sale_date - timedelta(days=current_sale_date.day)).replace(day=1).strftime('%Y-%m'))
        
        cantidad = 0
        if prev_month_key in last_product_monthly_state and last_product_monthly_state[prev_month_key]['cantidad'] > 0:
            P1_mes_anterior = last_product_monthly_state[prev_month_key]['precio']
            Q1_mes_anterior = last_product_monthly_state[prev_month_key]['cantidad']
            
            if (P1_mes_anterior + precio_actual) / 2 != 0:
                porcentaje_cambio_precio = (precio_actual - P1_mes_anterior) / ((P1_mes_anterior + precio_actual) / 2)
            else:
                porcentaje_cambio_precio = 0
            
            porcentaje_cambio_cantidad = epd_objetivo * porcentaje_cambio_precio

            base_cantidad_individual = np.random.randint(1, 5) 
            cantidad = int(base_cantidad_individual * (1 + porcentaje_cambio_cantidad * random.uniform(0.5, 1.5)))
            
            cantidad = max(1, cantidad)
        else:
            cantidad = np.random.randint(1, 10) 
            cantidad = max(1, int(cantidad * random.uniform(0.8, 1.2)))

        ventas_data_final.append({
            'venta_id': venta_id_counter_final,
            'fecha': current_sale_date.strftime('%Y-%m-%d'),
            'cliente_id': cliente_id,
            'producto_id': producto_id,
            'cantidad': cantidad, 
            'canal_id': canal_id,
            'region_id': region_id,
            'promocion_id': promocion_id,
            'historico_precio_id': historico_precio_id
        })
        venta_id_counter_final += 1
    
    # Update monthly state for EPD calculation for the NEXT month
    # This block runs at the end of each simulated month
    if current_sale_date.day == current_sale_date.replace(day=28).day: # Check for end of month
        prev_month_start = (current_sale_date - timedelta(days=current_sale_date.day - 1)).replace(day=1)
        prev_month_end = current_sale_date
        
        current_month_sales_data = [v for v in ventas_data_final if pd.to_datetime(v['fecha']).date() >= prev_month_start.date() and pd.to_datetime(v['fecha']).date() <= prev_month_end.date()]
        
        df_current_month_sales = pd.DataFrame(current_month_sales_data)
        if not df_current_month_sales.empty:
            df_current_month_sales_merged = df_current_month_sales.merge(
                df_historico_precios_indexed[['precio_base']],
                left_on='historico_precio_id',
                right_index=True,
                how='left'
            )
            df_current_month_sales_merged['precio_base'] = df_current_month_sales_merged['precio_base'].fillna(0) # Handle missing prices if any

            monthly_summary = df_current_month_sales_merged.groupby('producto_id').agg(
                cantidad_total=('cantidad', 'sum'),
                precio_promedio=('precio_base', 'mean')
            ).reset_index()

            for _, row in monthly_summary.iterrows():
                key = (row['producto_id'], current_sale_date.strftime('%Y-%m'))
                last_product_monthly_state[key] = {
                    'precio': row['precio_promedio'],
                    'cantidad': row['cantidad_total']
                }

df_ventas = pd.DataFrame(ventas_data_final).head(n_ventas)

# Save to CSV
df_clientes.to_csv('clientes.csv', index=False)
df_productos.to_csv('productos.csv', index=False)
df_historico_precios.to_csv('historico_precios.csv', index=False)
df_canales.to_csv('canales.csv', index=False)
df_regiones.to_csv('regiones.csv', index=False)
df_promociones.to_csv('promociones.csv', index=False)
df_inventarios.to_csv('inventarios.csv', index=False)
df_ventas.to_csv('ventas.csv', index=False)

print(f"Datasets generated and saved as CSV.")
print(f"Total sales: {len(df_ventas)}")
print(f"Total products: {len(df_productos)}")
print(f"Total price history entries: {len(df_historico_precios)}")
print(f"Total clients: {len(df_clientes)}")
print(f"Total regions: {len(df_regiones)}")
print(f"Total channels: {len(df_canales)}")
print(f"Total promotions: {len(df_promociones)}")
print(f"Total inventory entries: {len(df_inventarios)}")