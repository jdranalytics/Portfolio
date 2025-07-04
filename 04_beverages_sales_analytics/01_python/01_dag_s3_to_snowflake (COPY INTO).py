import logging
import boto3
import snowflake.connector
import tempfile
import os
import requests
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance
from typing import List, Dict, Optional

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración global
IS_REPLACE = True  # True para reemplazar (TRUNCATE + COPY), False para append (solo COPY)

# Configuración de AWS S3
S3_BUCKET = "XXXXXXXXXX-XXXXX"
S3_PREFIX = ""
AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXX/XXXXXXXXXX"
AWS_REGION = "us-east-2"

# Configuración de Snowflake
SNOWFLAKE_USER = "XXXXXXXXXXXXXXX"
SNOWFLAKE_PASSWORD = "XXXXXXXXXX"
SNOWFLAKE_ACCOUNT = "XXXXXXXXXXXXX"
SNOWFLAKE_WAREHOUSE = "XXXXXXXXXXXXX"
SNOWFLAKE_DATABASE = "XXXXXXXXXXXXX"
SNOWFLAKE_SCHEMA = "XXXXXXXXXXXXXXXXX"
STAGE_NAME = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STAGE_TEMP"

# Configuración de Discord
DISCORD_WEBHOOK_URL = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

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
                "title": f"{emoji} Notificación: Ingesta de datos desde S3 a Snowflake ❄",
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
    snowflake_conn = None
    cursor = None
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        csv_files = []
        if "Contents" in response:
            csv_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].lower().endswith('.csv')]
        if not csv_files:
            expected_files = [f"{table.lower()}.csv" for table in expected_columns.keys()]
            missing_files = [f for f in expected_files if f not in [os.path.basename(key) for key in csv_files]]
            message = f"No se encontraron archivos CSV en el bucket S3: {S3_BUCKET}/{S3_PREFIX}"
            if missing_files:
                message += f"\nArchivos ausentes esperados: {', '.join(missing_files)}"
            send_discord_message(message, success=False)
            raise Exception(message)
        temp_dir = tempfile.gettempdir()
        downloaded_files = []
        snowflake_conn = get_snowflake_connection()
        cursor = snowflake_conn.cursor()
        # Crear stage si no existe
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME}")
        for file_key in csv_files:
            temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=".csv", delete=False)
            local_path = temp_file.name
            temp_file.close()
            s3_client.download_file(S3_BUCKET, file_key, local_path)
            # Subir archivo a la stage sin marca de tiempo
            with open(local_path, 'rb') as f:
                cursor.execute(f"PUT file://{local_path} @{STAGE_NAME}/{os.path.basename(file_key)} AUTO_COMPRESS=TRUE")
            downloaded_files.append({
                "key": file_key,
                "local_path": local_path,
                "filename": os.path.basename(file_key)
            })
            logging.info(f"Archivo subido a stage: @{STAGE_NAME}/{os.path.basename(file_key)}")
        context['task_instance'].xcom_push(key='downloaded_files', value=downloaded_files)
        send_discord_message(f"Se descargaron y subieron {len(downloaded_files)} archivos a la stage", success=True)
        return downloaded_files
    except Exception as e:
        error_msg = f"Error al descargar archivos de S3 o subir a stage: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if snowflake_conn:
            snowflake_conn.close()
        if s3_client:
            pass  # No cerrar cliente S3 explícitamente

def process_table(table_name: str, expected_cols: list, date_cols: list = [], timestamp_cols: list = []):
    def _process(**context):
        conn = None
        cursor = None
        try:
            downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
            if not downloaded_files:  # Si no hay archivos descargados, detenerse
                raise Exception(f"No se encontraron archivos descargados para procesar {table_name}")
            file_info = next((f for f in downloaded_files if f["filename"] == f"{table_name.lower()}.csv"), None)
            if not file_info:
                send_discord_message(f"No se encontró archivo de {table_name.lower()}.csv", success=False)
                return []
            conn = get_snowflake_connection()
            cursor = conn.cursor()
            if IS_REPLACE:
                cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}")
                logging.info(f"Tabla {table_name} truncada para reemplazo")
            # Definir FILE_FORMAT con SKIP_HEADER = 1 y FIELD_OPTIONALLY_ENCLOSED_BY='"'
            file_format = (
                "FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 "
                "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' TRIM_SPACE = TRUE NULL_IF = ('NULL', 'nan', 'NaN'))"
            )
            copy_into = (
                f"COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} "
                f"FROM @{STAGE_NAME}/{file_info['filename']} "
                f"{file_format} "
                "ON_ERROR = 'CONTINUE'"
            )
            logging.info(f"Executing SQL: {copy_into}")  # Depuración
            cursor.execute(copy_into)
            conn.commit()
            # Agregar un retraso para permitir la propagación de datos
            time.sleep(5)
            cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}")
            record_count = cursor.fetchone()[0]
            new_records = [f"ID: {i}" for i in range(record_count)]
            context['task_instance'].xcom_push(key=f"{table_name.lower()}_records", value=new_records)
            return new_records
        except Exception as e:
            if conn:
                conn.rollback()
            error_msg = f"Error al procesar {table_name}: {str(e)}"
            send_discord_message(error_msg, success=False, error_details=str(e))
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    return _process

def cleanup_files(**context):
    s3_client = None
    snowflake_conn = None
    cursor = None
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
        snowflake_conn = get_snowflake_connection()
        cursor = snowflake_conn.cursor()
        for file_info in downloaded_files:
            if os.path.exists(file_info["local_path"]):
                os.remove(file_info["local_path"])
                logging.info(f"Archivo temporal {file_info['local_path']} eliminado")
            s3_client.delete_object(Bucket=S3_BUCKET, Key=file_info["key"])
            logging.info(f"Archivo {file_info['key']} eliminado de S3")
            # Eliminar archivo de la stage
            cursor.execute(f"REMOVE @{STAGE_NAME}/{file_info['filename']}")
        # Limpiar toda la stage después de procesar
        cursor.execute(f"REMOVE @{STAGE_NAME}/*")
        send_discord_message("Limpieza de archivos completada, incluyendo stage", success=True)
    except Exception as e:
        error_msg = f"Error durante la limpieza de archivos: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if snowflake_conn:
            snowflake_conn.close()
        if s3_client:
            pass  # No cerrar cliente S3 explícitamente

def send_summary(**context):
    try:
        records = {
            'clientes': context['task_instance'].xcom_pull(task_ids='process_clientes', key='clientes_records') or [],
            'productos': context['task_instance'].xcom_pull(task_ids='process_productos', key='productos_records') or [],
            'canales': context['task_instance'].xcom_pull(task_ids='process_canales', key='canales_records') or [],
            'regiones': context['task_instance'].xcom_pull(task_ids='process_regiones', key='regiones_records') or [],
            'promociones': context['task_instance'].xcom_pull(task_ids='process_promociones', key='promociones_records') or [],
            'inventarios': context['task_instance'].xcom_pull(task_ids='process_inventarios', key='inventarios_records') or [],
            'ventas': context['task_instance'].xcom_pull(task_ids='process_ventas', key='ventas_records') or []
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

# Definir expected_columns y date/timestamp columns como variable global
expected_columns = {
    'clientes': ['CLIENTE_ID', 'NOMBRE', 'EDAD', 'GENERO', 'CIUDAD', 'FRECUENCIA_COMPRA', 'ULTIMA_COMPRA'],
    'productos': ['PRODUCTO_ID', 'NOMBRE_PRODUCTO', 'CATEGORIA', 'PRECIO_BASE', 'COSTO_VARIABLE', 'MARCA'],
    'canales': ['CANAL_ID', 'NOMBRE_CANAL', 'TIPO_CANAL'],
    'regiones': ['REGION_ID', 'NOMBRE_REGION', 'CIUDAD'],
    'promociones': ['PROMOCION_ID', 'NOMBRE_PROMOCION', 'DESCUENTO_PORCENTAJE', 'FECHA_INICIO', 'FECHA_FIN'],
    'inventarios': ['INVENTARIO_ID', 'PRODUCTO_ID', 'REGION_ID', 'STOCK', 'FECHA_ACTUALIZACION'],
    'ventas': ['VENTA_ID', 'FECHA', 'CLIENTE_ID', 'PRODUCTO_ID', 'CANTIDAD', 'CANAL_ID', 'REGION_ID', 'PROMOCION_ID']
}

date_columns = {
    'clientes': ['ULTIMA_COMPRA'],
    'promociones': ['FECHA_INICIO', 'FECHA_FIN'],
    'inventarios': ['FECHA_ACTUALIZACION'],
    'ventas': [],
    'productos': [],
    'canales': [],
    'regiones': []
}

timestamp_columns = {
    'clientes': [],
    'promociones': [],
    'inventarios': [],
    'ventas': ['FECHA'],
    'productos': [],
    'canales': [],
    'regiones': []
}

default_args = {
    'owner': 'JDRP',
    'start_date': datetime(2025, 5, 20),
    'retries': 1
}

with DAG(
    'ingest_bebidas_data',
    default_args=default_args,
    schedule='0 0 * * *',
    catchup=False
) as dag:
    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_s3_files
    )
    process_clientes_task = PythonOperator(
        task_id='process_clientes',
        python_callable=process_table('clientes', expected_columns['clientes'], date_columns['clientes'], timestamp_cols=timestamp_columns['clientes'])
    )
    process_productos_task = PythonOperator(
        task_id='process_productos',
        python_callable=process_table('productos', expected_columns['productos'], date_columns['productos'], timestamp_cols=timestamp_columns['productos'])
    )
    process_canales_task = PythonOperator(
        task_id='process_canales',
        python_callable=process_table('canales', expected_columns['canales'], date_columns['canales'], timestamp_cols=timestamp_columns['canales'])
    )
    process_regiones_task = PythonOperator(
        task_id='process_regiones',
        python_callable=process_table('regiones', expected_columns['regiones'], date_columns['regiones'], timestamp_cols=timestamp_columns['regiones'])
    )
    process_promociones_task = PythonOperator(
        task_id='process_promociones',
        python_callable=process_table('promociones', expected_columns['promociones'], date_columns['promociones'], timestamp_cols=timestamp_columns['promociones'])
    )
    process_inventarios_task = PythonOperator(
        task_id='process_inventarios',
        python_callable=process_table('inventarios', expected_columns['inventarios'], date_columns['inventarios'], timestamp_cols=timestamp_columns['inventarios'])
    )
    process_ventas_task = PythonOperator(
        task_id='process_ventas',
        python_callable=process_table('ventas', expected_columns['ventas'], date_columns['ventas'], timestamp_cols=timestamp_columns['ventas'])
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