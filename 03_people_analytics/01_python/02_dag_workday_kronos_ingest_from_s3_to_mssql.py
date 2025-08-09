import logging
import pandas as pd
import boto3
import pymssql
import tempfile
import os
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance
from typing import List, Dict, Optional

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de AWS S3
S3_BUCKET = "human-resources-0"
S3_PREFIX = ""
AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXX"
AWS_REGION = "us-east-2"

# Configuración de SQL Server
SQL_SERVER = "XXX.XXX.XXX.XXX"
SQL_PORT = XXXXX
SQL_DB = "HR_Analytics"
SQL_USER = "sa"
SQL_PASSWORD = "123456"
SQL_INSTANCE = "SQLEXPRESS"

# Configuración de Discord
DISCORD_WEBHOOK_URL = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

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
                "title": f"{emoji} Notificación: Ingesta de Workday/Kronos desde S3 a SQL Server",
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

def get_sql_connection():
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            database=SQL_DB,
            user=SQL_USER,
            password=SQL_PASSWORD,
            port=SQL_PORT,
            tds_version="7.0"
        )
        return conn
    except Exception as e:
        error_msg = f"Error al conectar con SQL Server: {str(e)}"
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
        csv_files = [obj["Key"] for obj in response["Contents"] 
                    if obj["Key"].lower().endswith('.csv') and 
                    any(name in obj["Key"].lower() for name in ["workday", "kronos"])]
        if not csv_files:
            message = f"No se encontraron archivos CSV válidos en el bucket S3: {S3_BUCKET}/{S3_PREFIX}"
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

def process_workday_data(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        if not downloaded_files:
            return []
        workday_file = next((f for f in downloaded_files if "workday" in f["filename"].lower()), None)
        if not workday_file:
            send_discord_message("No se encontró archivo de Workday para procesar", success=False)
            return []
        conn = get_sql_connection()
        cursor = conn.cursor()
        new_records = []
        df = pd.read_csv(workday_file["local_path"], na_values=['nan', 'NaN', 'NULL'])
        required_columns = ["employee_id", "first_name", "last_name", "gender", "age", "department", "job_role", 
                           "hire_date", "termination_date", "onleave_date", "salary", "location", "status", 
                           "performance_score"]
        if not all(col in df.columns for col in required_columns):
            message = f"El archivo {workday_file['filename']} no tiene las columnas requeridas: {required_columns}"
            send_discord_message(message, success=False)
            return []
        df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')
        df['termination_date'] = pd.to_datetime(df['termination_date'], errors='coerce')
        df['onleave_date'] = pd.to_datetime(df['onleave_date'], errors='coerce')
        df = df.replace([pd.NA, 'nan', 'NaN'], None)
        for _, row in df.iterrows():
            cursor.execute("SELECT employee_id FROM Workday_Employees WHERE employee_id = %s", 
                         (row["employee_id"],))
            if cursor.fetchone():
                continue
            cursor.execute(
                """
                INSERT INTO Workday_Employees 
                (employee_id, first_name, last_name, gender, age, department, job_role, hire_date, termination_date, 
                 onleave_date, salary, location, status, performance_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row["employee_id"], row["first_name"], row["last_name"], row["gender"], row["age"], row["department"],
                 row["job_role"], row["hire_date"], row["termination_date"], row["onleave_date"], row["salary"], 
                 row["location"], row["status"], row["performance_score"])
            )
            new_records.append(f"Employee ID: {row['employee_id']}, Name: {row['first_name']} {row['last_name']}")
        conn.commit()
        context['task_instance'].xcom_push(key='workday_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros de Workday", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar datos de Workday: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_kronos_data(**context):
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        if not downloaded_files:
            return []
        kronos_file = next((f for f in downloaded_files if "kronos" in f["filename"].lower()), None)
        if not kronos_file:
            send_discord_message("No se encontró archivo de Kronos para procesar", success=False)
            return []
        conn = get_sql_connection()
        cursor = conn.cursor()
        new_records = []
        df = pd.read_csv(kronos_file["local_path"], na_values=['nan', 'NaN', 'NULL'])
        required_columns = ["entry_id", "employee_id", "work_date", "hours_worked", "shift_type", "overtime_hours"]
        if not all(col in df.columns for col in required_columns):
            message = f"El archivo {kronos_file['filename']} no tiene las columnas requeridas: {required_columns}"
            send_discord_message(message, success=False)
            return []
        df['work_date'] = pd.to_datetime(df['work_date'], errors='coerce')
        df = df.replace([pd.NA, 'nan', 'NaN'], None)
        for _, row in df.iterrows():
            cursor.execute("SELECT entry_id FROM Kronos_TimeEntries WHERE entry_id = %s", 
                         (row["entry_id"],))
            if cursor.fetchone():
                continue
            cursor.execute("SELECT employee_id FROM Workday_Employees WHERE employee_id = %s", 
                         (row["employee_id"],))
            if not cursor.fetchone():
                logging.warning(f"Employee ID {row['employee_id']} no existe en Workday_Employees")
                continue
            cursor.execute(
                """
                INSERT INTO Kronos_TimeEntries 
                (entry_id, employee_id, work_date, hours_worked, shift_type, overtime_hours)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (row["entry_id"], row["employee_id"], row["work_date"], row["hours_worked"],
                 row["shift_type"], row["overtime_hours"])
            )
            new_records.append(f"Entry ID: {row['entry_id']}, Employee ID: {row['employee_id']}")
        conn.commit()
        context['task_instance'].xcom_push(key='kronos_records', value=new_records)
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros de Kronos", success=True)
        return new_records
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar datos de Kronos: {str(e)}"
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
        workday_records = context['task_instance'].xcom_pull(task_ids='process_workday', key='workday_records') or []
        kronos_records = context['task_instance'].xcom_pull(task_ids='process_kronos', key='kronos_records') or []
        total_records = len(workday_records) + len(kronos_records)
        if total_records > 0:
            message = f"""Resumen de la ingesta:
Registros de Workday: {len(workday_records)}
Registros de Kronos: {len(kronos_records)}
Total de registros procesados: {total_records}"""
            send_discord_message(message, success=True)
        else:
            send_discord_message("No se encontraron nuevos registros para procesar", success=True)
    except Exception as e:
        error_msg = "Error al enviar el resumen"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise

default_args = {
    'owner': 'JDRP',
    'start_date': datetime(2025, 5, 20),
    'retries': 1
}

with DAG(
    'ingest_workday_kronos',
    default_args=default_args,
    schedule='0 5 * * *',
    catchup=False
) as dag:
    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_s3_files
    )
    process_workday = PythonOperator(
        task_id='process_workday',
        python_callable=process_workday_data
    )
    process_kronos = PythonOperator(
        task_id='process_kronos',
        python_callable=process_kronos_data
    )
    cleanup_task = PythonOperator(
        task_id='cleanup_files',
        python_callable=cleanup_files
    )
    summary_task = PythonOperator(
        task_id='send_summary',
        python_callable=send_summary
    )
    download_task >> process_workday >> process_kronos >> cleanup_task >> summary_task