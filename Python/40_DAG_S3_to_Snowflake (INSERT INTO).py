import logging
import pandas as pd
import boto3
import snowflake.connector
import tempfile
import os
import requests
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance
from typing import List, Dict, Optional

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de AWS S3
S3_BUCKET = "XXXXXX-XXXXX"
S3_PREFIX = ""
AWS_ACCESS_KEY = "XXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXX/XXXXXXXXXXXXXXXXX"
AWS_REGION = "us-east-2"

# Configuración de Snowflake (placeholders)
SNOWFLAKE_USER = "XXXXXXXXXXXXX"
SNOWFLAKE_PASSWORD = "XXXXXXXXXXXXXXXX"
SNOWFLAKE_ACCOUNT = "XXXXXXXXXXXXX"
SNOWFLAKE_WAREHOUSE = "XXXXXXXXXXXXX"
SNOWFLAKE_DATABASE = "XXXXXXXXXXXX"
SNOWFLAKE_SCHEMA = "XXXXXXXXXXXXXXXXXXXX"

# Configuración de Discord (placeholder)
DISCORD_WEBHOOK_URL = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


def send_discord_message(message: str, success: bool = True, error_details: Optional[str] = None) -> None:
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = 0x00FF00 if success else 0xFF0000
        emoji = "✅" if success else "❌"
        description = message
        if error_details:
            description += f"\n\nDetalles del error:\n```\n{error_details}\n```"
        payload = {
            "embeds": [{
                "title": f"{emoji} Notificación: Ingesta de datos desde S3 a Snowflake",
                "description": description,
                "color": color,
                "fields": [{"name": "Timestamp", "value": timestamp, "inline": True}],
                "footer": {"text": "Sistema de Monitoreo"}
            }]
        }
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            logging.info(f"Mensaje enviado a Discord: {message}")
        else:
            logging.error(f"Error al enviar mensaje a Discord: {response.text}")
    except Exception as e:
        logging.error(f"Error al enviar mensaje a Discord: {e}")

def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        return conn
    except Exception as e:
        error_msg = f"Error al conectar con Snowflake: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise

def download_s3_files(**context):
    s3_client = None
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        if "Contents" not in response:
            message = f"No se encontraron archivos en el bucket S3: {S3_BUCKET}/{S3_PREFIX}"
            send_discord_message(message, success=False)
            return []
        csv_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].lower().endswith('.csv')]
        if not csv_files:
            message = f"No se encontraron archivos CSV en el bucket S3: {S3_BUCKET}/{S3_PREFIX}"
            send_discord_message(message, success=False)
            return []
        temp_dir = tempfile.gettempdir()
        downloaded_files = []
        for file_key in csv_files:
            temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=".csv", delete=False)
            local_path = temp_file.name
            temp_file.close()
            s3_client.download_file(S3_BUCKET, file_key, local_path)
            downloaded_files.append({
                "key": file_key,
                "local_path": local_path,
                "filename": os.path.basename(file_key)
            })
        context['task_instance'].xcom_push(key='downloaded_files', value=downloaded_files)
        send_discord_message(f"Se descargaron {len(downloaded_files)} archivos de S3", success=True)
        return downloaded_files
    except Exception as e:
        error_msg = f"Error al descargar archivos de S3: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise

def process_clientes(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        file_info = next((f for f in downloaded_files if f["filename"] == "clientes.csv"), None)
        if not file_info:
            send_discord_message("No se encontró archivo de clientes.csv", success=False)
            return []
        df = pd.read_csv(file_info["local_path"], na_values=['nan', 'NaN', 'NULL'])
        logging.info(f"Columnas leídas de clientes.csv: {df.columns.tolist()}")
        column_mapping = {col.lower(): expected_col for col, expected_col in zip(df.columns, expected_columns['clientes'])}
        df = df.rename(columns=lambda x: column_mapping.get(x.lower(), x))
        df = df[expected_columns['clientes']]
        missing_columns = [col for col in expected_columns['clientes'] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Faltan columnas en clientes.csv: {missing_columns}")
        df['ULTIMA_COMPRA'] = pd.to_datetime(df['ULTIMA_COMPRA'], errors='coerce').dt.date
        df = df.replace([pd.NA, 'nan', 'NaN'], None)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.clientes")
        new_records = []
        for _, row in df.iterrows():
            columns = ', '.join(expected_columns['clientes'])
            placeholders = ', '.join(['%s'] * len(expected_columns['clientes']))
            values = [row[col] for col in expected_columns['clientes']]
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.clientes ({columns}) VALUES ({placeholders})",
                tuple(values)
            )
            new_records.append(f"ID: {row.get('CLIENTE_ID', 'N/A')}")
        conn.commit()
        context['task_instance'].xcom_push(key='clientes_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros en clientes", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar clientes: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_productos(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        file_info = next((f for f in downloaded_files if f["filename"] == "productos.csv"), None)
        if not file_info:
            send_discord_message("No se encontró archivo de productos.csv", success=False)
            return []
        df = pd.read_csv(file_info["local_path"], na_values=['nan', 'NaN', 'NULL'])
        logging.info(f"Columnas leídas de productos.csv: {df.columns.tolist()}")
        column_mapping = {col.lower(): expected_col for col, expected_col in zip(df.columns, expected_columns['productos'])}
        df = df.rename(columns=lambda x: column_mapping.get(x.lower(), x))
        df = df[expected_columns['productos']]
        missing_columns = [col for col in expected_columns['productos'] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Faltan columnas en productos.csv: {missing_columns}")
        df = df.replace([pd.NA, 'nan', 'NaN'], None)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.productos")
        new_records = []
        for _, row in df.iterrows():
            columns = ', '.join(expected_columns['productos'])
            placeholders = ', '.join(['%s'] * len(expected_columns['productos']))
            values = [row[col] for col in expected_columns['productos']]
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.productos ({columns}) VALUES ({placeholders})",
                tuple(values)
            )
            new_records.append(f"ID: {row.get('PRODUCTO_ID', 'N/A')}")
        conn.commit()
        context['task_instance'].xcom_push(key='productos_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros en productos", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar productos: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_canales(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        file_info = next((f for f in downloaded_files if f["filename"] == "canales.csv"), None)
        if not file_info:
            send_discord_message("No se encontró archivo de canales.csv", success=False)
            return []
        df = pd.read_csv(file_info["local_path"], na_values=['nan', 'NaN', 'NULL'])
        logging.info(f"Columnas leídas de canales.csv: {df.columns.tolist()}")
        column_mapping = {col.lower(): expected_col for col, expected_col in zip(df.columns, expected_columns['canales'])}
        df = df.rename(columns=lambda x: column_mapping.get(x.lower(), x))
        df = df[expected_columns['canales']]
        missing_columns = [col for col in expected_columns['canales'] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Faltan columnas en canales.csv: {missing_columns}")
        df = df.replace([pd.NA, 'nan', 'NaN'], None)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.canales")
        new_records = []
        for _, row in df.iterrows():
            columns = ', '.join(expected_columns['canales'])
            placeholders = ', '.join(['%s'] * len(expected_columns['canales']))
            values = [row[col] for col in expected_columns['canales']]
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.canales ({columns}) VALUES ({placeholders})",
                tuple(values)
            )
            new_records.append(f"ID: {row.get('CANAL_ID', 'N/A')}")
        conn.commit()
        context['task_instance'].xcom_push(key='canales_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros en canales", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar canales: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_regiones(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        file_info = next((f for f in downloaded_files if f["filename"] == "regiones.csv"), None)
        if not file_info:
            send_discord_message("No se encontró archivo de regiones.csv", success=False)
            return []
        df = pd.read_csv(file_info["local_path"], na_values=['nan', 'NaN', 'NULL'])
        logging.info(f"Columnas leídas de regiones.csv: {df.columns.tolist()}")
        column_mapping = {col.lower(): expected_col for col, expected_col in zip(df.columns, expected_columns['regiones'])}
        df = df.rename(columns=lambda x: column_mapping.get(x.lower(), x))
        df = df[expected_columns['regiones']]
        missing_columns = [col for col in expected_columns['regiones'] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Faltan columnas en regiones.csv: {missing_columns}")
        df = df.replace([pd.NA, 'nan', 'NaN'], None)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.regiones")
        new_records = []
        for _, row in df.iterrows():
            columns = ', '.join(expected_columns['regiones'])
            placeholders = ', '.join(['%s'] * len(expected_columns['regiones']))
            values = [row[col] for col in expected_columns['regiones']]
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.regiones ({columns}) VALUES ({placeholders})",
                tuple(values)
            )
            new_records.append(f"ID: {row.get('REGION_ID', 'N/A')}")
        conn.commit()
        context['task_instance'].xcom_push(key='regiones_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros en regiones", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar regiones: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_promociones(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        file_info = next((f for f in downloaded_files if f["filename"] == "promociones.csv"), None)
        if not file_info:
            send_discord_message("No se encontró archivo de promociones.csv", success=False)
            return []
        df = pd.read_csv(file_info["local_path"], na_values=['nan', 'NaN', 'NULL'])
        logging.info(f"Columnas leídas de promociones.csv: {df.columns.tolist()}")
        column_mapping = {col.lower(): expected_col for col, expected_col in zip(df.columns, expected_columns['promociones'])}
        df = df.rename(columns=lambda x: column_mapping.get(x.lower(), x))
        df = df[expected_columns['promociones']]
        missing_columns = [col for col in expected_columns['promociones'] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Faltan columnas en promociones.csv: {missing_columns}")
        df['FECHA_INICIO'] = pd.to_datetime(df['FECHA_INICIO'], errors='coerce').dt.date
        df['FECHA_FIN'] = pd.to_datetime(df['FECHA_FIN'], errors='coerce').dt.date
        df = df.replace([pd.NA, 'nan', 'NaN'], None)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.promociones")
        new_records = []
        for _, row in df.iterrows():
            columns = ', '.join(expected_columns['promociones'])
            placeholders = ', '.join(['%s'] * len(expected_columns['promociones']))
            values = [row[col] for col in expected_columns['promociones']]
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.promociones ({columns}) VALUES ({placeholders})",
                tuple(values)
            )
            new_records.append(f"ID: {row.get('PROMOCION_ID', 'N/A')}")
        conn.commit()
        context['task_instance'].xcom_push(key='promociones_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros en promociones", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar promociones: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_inventarios(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        file_info = next((f for f in downloaded_files if f["filename"] == "inventarios.csv"), None)
        if not file_info:
            send_discord_message("No se encontró archivo de inventarios.csv", success=False)
            return []
        df = pd.read_csv(file_info["local_path"], na_values=['nan', 'NaN', 'NULL'])
        logging.info(f"Columnas leídas de inventarios.csv: {df.columns.tolist()}")
        column_mapping = {col.lower(): expected_col for col, expected_col in zip(df.columns, expected_columns['inventarios'])}
        df = df.rename(columns=lambda x: column_mapping.get(x.lower(), x))
        df = df[expected_columns['inventarios']]
        missing_columns = [col for col in expected_columns['inventarios'] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Faltan columnas en inventarios.csv: {missing_columns}")
        df['FECHA_ACTUALIZACION'] = pd.to_datetime(df['FECHA_ACTUALIZACION'], errors='coerce').dt.date
        df = df.replace([pd.NA, 'nan', 'NaN'], None)
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.inventarios")
        new_records = []
        for _, row in df.iterrows():
            columns = ', '.join(expected_columns['inventarios'])
            placeholders = ', '.join(['%s'] * len(expected_columns['inventarios']))
            values = [row[col] for col in expected_columns['inventarios']]
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.inventarios ({columns}) VALUES ({placeholders})",
                tuple(values)
            )
            new_records.append(f"ID: {row.get('INVENTARIO_ID', 'N/A')}")
        conn.commit()
        context['task_instance'].xcom_push(key='inventarios_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros en inventarios", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar inventarios: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_ventas(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        file_info = next((f for f in downloaded_files if f["filename"] == "ventas.csv"), None)
        if not file_info:
            send_discord_message("No se encontró archivo de ventas.csv", success=False)
            return []
        df = pd.read_csv(file_info["local_path"], na_values=['nan', 'NaN', 'NULL'])
        logging.info(f"Columnas leídas de ventas.csv: {df.columns.tolist()}")
        column_mapping = {col.lower(): expected_col for col, expected_col in zip(df.columns, expected_columns['ventas'])}
        df = df.rename(columns=lambda x: column_mapping.get(x.lower(), x))
        df = df[expected_columns['ventas']]
        missing_columns = [col for col in expected_columns['ventas'] if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Faltan columnas en ventas.csv: {missing_columns}")
        # Reemplazar NaN antes de cualquier conversión
        df = df.replace({pd.NA: None, np.nan: None})
        # Convertir FECHA a timestamp compatible con Snowflake
        df['FECHA'] = pd.to_datetime(df['FECHA'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df['FECHA'] = df['FECHA'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
        # Verificar valores antes de la inserción
        logging.info(f"Valores de FECHA después de conversión: {df['FECHA'].head().tolist()}")
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.ventas")
        new_records = []
        for _, row in df.iterrows():
            columns = ', '.join(expected_columns['ventas'])
            placeholders = ', '.join(['%s'] * len(expected_columns['ventas']))
            values = [row[col] for col in expected_columns['ventas']]
            logging.info(f"Valores para inserción: {values}")
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.ventas ({columns}) VALUES ({placeholders})",
                tuple(values)
            )
            new_records.append(f"ID: {row.get('VENTA_ID', 'N/A')}")
        conn.commit()
        context['task_instance'].xcom_push(key='ventas_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros en ventas", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar ventas: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def cleanup_files(**context):
    s3_client = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        if not downloaded_files:
            return
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        for file_info in downloaded_files:
            if os.path.exists(file_info["local_path"]):
                os.remove(file_info["local_path"])
                logging.info(f"Archivo temporal {file_info['local_path']} eliminado")
            s3_client.delete_object(Bucket=S3_BUCKET, Key=file_info["key"])
            logging.info(f"Archivo {file_info['key']} eliminado de S3")
        send_discord_message("Limpieza de archivos completada", success=True)
    except Exception as e:
        error_msg = f"Error durante la limpieza de archivos: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise

def send_summary(**context):
    try:
        records = {
            'clientes': context['task_instance'].xcom_pull(task_ids='process_clientes', key='clientes_records') or [],
            'productos': context['task_instance'].xcom_pull(task_ids='process_productos', key='productos_records') or [],
            'canales': context['task_instance'].xcom_pull(task_ids='process_canales', key='canales_records') or [],
            'regiones': context['task_instance'].xcom_pull(task_ids='process_regiones', key='regiones_records') or [],
            'promociones': context['task_instance'].xcom_pull(task_ids='process_promociones', key='promociones_records') or [],
            'inventarios': context['task_instance'].xcom_pull(task_ids='process_inventarios', key='inventarios_records') or [],
            'ventas': context['task_instance'].xcom_push(task_ids='process_ventas', key='ventas_records') or []
        }
        total_records = sum(len(records[table]) for table in records)
        if total_records > 0:
            message = "Resumen de la ingesta:\n"
            for table in records:
                message += f"Registros en {table}: {len(records[table])}\n"
            message += f"Total de registros procesados: {total_records}"
            send_discord_message(message, success=True)
        else:
            send_discord_message("No se encontraron nuevos registros para procesar", success=True)
    except Exception as e:
        error_msg = "Error al enviar el resumen"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise

# Definir expected_columns como variable global
expected_columns = {
    'clientes': ['CLIENTE_ID', 'NOMBRE', 'EDAD', 'GENERO', 'CIUDAD', 'FRECUENCIA_COMPRA', 'ULTIMA_COMPRA'],
    'productos': ['PRODUCTO_ID', 'NOMBRE_PRODUCTO', 'CATEGORIA', 'PRECIO_BASE', 'COSTO_VARIABLE', 'MARCA'],
    'canales': ['CANAL_ID', 'NOMBRE_CANAL', 'TIPO_CANAL'],
    'regiones': ['REGION_ID', 'NOMBRE_REGION', 'CIUDAD'],
    'promociones': ['PROMOCION_ID', 'NOMBRE_PROMOCION', 'DESCUENTO_PORCENTAJE', 'FECHA_INICIO', 'FECHA_FIN'],
    'inventarios': ['INVENTARIO_ID', 'PRODUCTO_ID', 'REGION_ID', 'STOCK', 'FECHA_ACTUALIZACION'],
    'ventas': ['VENTA_ID', 'FECHA', 'CLIENTE_ID', 'PRODUCTO_ID', 'CANTIDAD', 'CANAL_ID', 'REGION_ID', 'PROMOCION_ID']
}

default_args = {
    'owner': 'JDRP',
    'start_date': datetime(2025, 5, 20),
    'retries': 1
}

with DAG(
    'ingest_bebidas_data',
    default_args=default_args,
    schedule='0 5 * * *',
    catchup=False
) as dag:
    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_s3_files
    )
    process_clientes_task = PythonOperator(
        task_id='process_clientes',
        python_callable=process_clientes
    )
    process_productos_task = PythonOperator(
        task_id='process_productos',
        python_callable=process_productos
    )
    process_canales_task = PythonOperator(
        task_id='process_canales',
        python_callable=process_canales
    )
    process_regiones_task = PythonOperator(
        task_id='process_regiones',
        python_callable=process_regiones
    )
    process_promociones_task = PythonOperator(
        task_id='process_promociones',
        python_callable=process_promociones
    )
    process_inventarios_task = PythonOperator(
        task_id='process_inventarios',
        python_callable=process_inventarios
    )
    process_ventas_task = PythonOperator(
        task_id='process_ventas',
        python_callable=process_ventas
    )
    cleanup_task = PythonOperator(
        task_id='cleanup_files',
        python_callable=cleanup_files
    )
    summary_task = PythonOperator(
        task_id='send_summary',
        python_callable=send_summary
    )

    download_task >> [
        process_clientes_task,
        process_productos_task,
        process_canales_task,
        process_regiones_task,
        process_promociones_task,
        process_inventarios_task,
        process_ventas_task
    ] >> cleanup_task >> summary_task