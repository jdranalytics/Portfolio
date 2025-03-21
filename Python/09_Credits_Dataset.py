import pandas as pd
import numpy as np
from faker import Faker
import random


fake = Faker()


n_registros = 10000  
random.seed(42)  


def generar_dataset_credito(n):
    data = {
        'ID_Cliente': [fake.uuid4() for _ in range(n)],
        'Edad': [random.randint(18, 80) for _ in range(n)],
        'Ingresos_Anuales': [round(random.uniform(15000, 150000), 2) for _ in range(n)],
        'Puntaje_Crediticio': [random.randint(300, 850) for _ in range(n)],
        'Historial_Pagos': [random.choice(['Bueno', 'Regular', 'Malo']) for _ in range(n)],
        'Deuda_Actual': [round(random.uniform(0, 50000), 2) for _ in range(n)],
        'Antiguedad_Laboral': [random.randint(0, 40) for _ in range(n)],
        'Estado_Civil': [random.choice(['Soltero', 'Casado', 'Divorciado', 'Viudo']) for _ in range(n)],
        'Numero_Dependientes': [random.randint(0, 5) for _ in range(n)],
        'Tipo_Empleo': [random.choice(['Fijo', 'Temporal', 'Autonomo', 'Desempleado']) for _ in range(n)],
        'Solicitud_Credito': [random.choice([0, 1]) for _ in range(n)],  # 0: No aprobado, 1: Aprobado
    }
    

    df = pd.DataFrame(data)
    

    df['Solicitud_Credito'] = df.apply(
        lambda row: 1 if (row['Puntaje_Crediticio'] > 650 and 
                         row['Ingresos_Anuales'] > 30000 and 
                         row['Historial_Pagos'] in ['Bueno', 'Regular'] and 
                         row['Deuda_Actual'] < row['Ingresos_Anuales'] * 0.4) 
        else 0, axis=1)
    
    return df


dataset = generar_dataset_credito(n_registros)

print("Primeras 5 filas del dataset:")
print(dataset.head())


dataset.to_csv('dataset_credito_sintetico.csv', index=False)
print("\nDataset guardado como 'dataset_credito_sintetico.csv'")


print("\nEstadísticas básicas del dataset:")
print(dataset.describe())