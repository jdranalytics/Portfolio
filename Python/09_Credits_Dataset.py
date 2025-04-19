import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Configurar Faker para Colombia
fake = Faker(['es_CO'])  # Usar localización colombiana
random.seed(42)
np.random.seed(42)

n_registros = 20000
fecha_inicio = datetime(2022, 1, 1)
fecha_fin = datetime(2025, 3, 27)

def generar_fecha_aleatoria(inicio, fin):
    delta = fin - inicio
    dias_aleatorios = random.randint(0, delta.days)
    return inicio + timedelta(days=dias_aleatorios)

def generar_dataset_credito(n):
    # Primero generamos todas las fechas y las ordenamos
    fechas = [generar_fecha_aleatoria(fecha_inicio, fecha_fin) for _ in range(n)]
    fechas.sort()  # Ordenamos las fechas
    
    # Generamos los IDs secuenciales basados en las fechas
    ids = []
    contador_diario = {}  # Diccionario para mantener conteo por día
    
    for fecha in fechas:
        fecha_str = fecha.strftime('%Y%m%d')
        if fecha_str not in contador_diario:
            contador_diario[fecha_str] = 1
        else:
            contador_diario[fecha_str] += 1
        
        # Crear ID con formato RA-YYYYMMDDXX donde XX es el contador del día
        id_solicitud = f"RA-{fecha_str}{contador_diario[fecha_str]:02d}"
        ids.append(id_solicitud)
    
    data = {
        'id_solicitud': ids,
        'fecha_solicitud': fechas,
        'cedula_ciudadania': [fake.ssn() for _ in range(n)],  
        'nombre': [fake.first_name() for _ in range(n)],
        'apellido': [fake.last_name() for _ in range(n)],
        'edad': [random.randint(18, 80) for _ in range(n)],
        'ingresos_anuales': [round(random.uniform(15000, 250000), 2) for _ in range(n)],
        'puntaje_crediticio': [random.randint(300, 850) for _ in range(n)],
        'historial_pagos': [random.choice(['Bueno', 'Regular', 'Malo']) for _ in range(n)],
        'deuda_actual': [round(random.uniform(0, 50000), 2) for _ in range(n)],
        'antiguedad_laboral': [random.randint(0, 40) for _ in range(n)],
        'estado_civil': [random.choice(['Soltero', 'Casado', 'Divorciado', 'Viudo']) for _ in range(n)],
        'numero_dependientes': [random.randint(0, 5) for _ in range(n)],
        'tipo_empleo': [random.choice(['Fijo', 'Temporal', 'Autonomo', 'Desempleado']) for _ in range(n)],
    }
    
    df = pd.DataFrame(data)
    
    # Generar decisión inicial (0 o 1) para solicitud_credito
    df['solicitud_credito'] = df.apply(
        lambda row: 1 if (row['puntaje_crediticio'] > 650 and 
                         row['ingresos_anuales'] > 30000 and 
                         row['historial_pagos'] in ['Bueno', 'Regular'] and 
                         row['deuda_actual'] < row['ingresos_anuales'] * 0.4) 
        else 0, axis=1)
    
    # Definir el rango de los últimos dos meses
    fecha_limite = fecha_fin - timedelta(days=60)  # 60 días antes de fecha_fin (20 de enero de 2025)
    
    # Crear máscara para los últimos dos meses
    mask_ultimos_dos_meses = df['fecha_solicitud'] >= fecha_limite
    
    # Generar máscara aleatoria para nulos solo en los últimos dos meses (25-30% de nulos)
    mask_nulls = np.random.random(n) < 0.25  # 25% de probabilidad de ser null
    mask_final = mask_ultimos_dos_meses & mask_nulls  # Combinar máscaras
    
    # Asignar NaN a los registros seleccionados en los últimos dos meses
    df.loc[mask_final, 'solicitud_credito'] = np.nan
    
    # Procesar fechas
    df['fecha_solicitud'] = pd.to_datetime(df['fecha_solicitud'])
    df['inicio_mes'] = df['fecha_solicitud'].dt.to_period('M').dt.to_timestamp()
    df['inicio_semana'] = df['fecha_solicitud'] - pd.to_timedelta(df['fecha_solicitud'].dt.dayofweek + 1, unit='D') + pd.to_timedelta(1, unit='D')
    
    # Ordenar por fecha_solicitud y resetear índice
    df = df.sort_values('fecha_solicitud').reset_index(drop=True)
    
    return df

# Generar dataset
dataset = generar_dataset_credito(n_registros)

# Mostrar primeras 5 filas
print("Primeras 5 filas del dataset:")
print(dataset.head())

# Guardar dataset
dataset.to_csv('dataset_credito_sintetico_temporal.csv', index=False)
print("\nDataset guardado como 'dataset_credito_sintetico_temporal.csv'")

# Calcular proporciones incluyendo valores null
proporcion = dataset['solicitud_credito'].value_counts(dropna=False, normalize=True) * 100
print("\nProporción en el dataset:")
print(f"En espera (NaN): {proporcion.get(np.nan, 0):.2f}%")
print(f"Créditos Aprobados (1): {proporcion.get(1, 0):.2f}%")
print(f"Créditos No Aprobados (0): {proporcion.get(0, 0):.2f}%")

# Estadísticas básicas
print("\nEstadísticas básicas del dataset:")
print(dataset.describe())

# Conteo de nulos por mes
print("\nConteo de valores nulos por mes en 'solicitud_credito':")
print(dataset.groupby(dataset['fecha_solicitud'].dt.to_period('M'))['solicitud_credito'].apply(lambda x: x.isna().sum()))