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
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable, TaskInstance
from typing import List, Dict, Optional

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de AWS S3
S3_BUCKET = "human-resources-XXX"
S3_PREFIX = ""
AWS_ACCESS_KEY = "XXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXX/XXXXXXXXXXXXXXX"
AWS_REGION = "us-east-2"

# Configuración de SQL Server
SQL_SERVER = "XXX.XX.XX.XX:XXXX\\XXXXXXXXXXXX" # Using localhost instead of machine name
SQL_DB = "HR_Analytics"
SQL_USER = "sa"
SQL_PASSWORD = "123456"

# Configuración de Discord
DISCORD_WEBHOOK_URL = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

def send_discord_message(message: str, success: bool = True, error_details: Optional[str] = None) -> None:
    """Envía un mensaje a Discord con el estado de la ejecución."""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = 0x00FF00 if success else 0xFF0000
        emoji = "✅" if success else "❌"
        
        description = message
        if error_details:
            description += f"\n\nDetalles del error:\n```\n{error_details}\n```"
        
        payload = {
            "embeds": [{
                "title": f"{emoji} Notificación: Ingesta de Encuestas de Empleados desde S3 a SQL Server",
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
    """Establece conexión con SQL Server."""
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            database=SQL_DB,
            user=SQL_USER,
            password=SQL_PASSWORD,
            port=1433  # Default SQL Server port
        )
        return conn
    except Exception as e:
        error_msg = f"Error al conectar con SQL Server: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise

def download_s3_files(**context):
    """Descarga archivos de S3."""
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
                    "survey" in obj["Key"].lower()]
        
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

def process_survey_data(**context):
    """Procesa datos de encuestas de empleados."""
    conn = None
    cursor = None
    try:
        downloaded_files = context['task_instance'].xcom_pull(task_ids='download_files', key='downloaded_files')
        if not downloaded_files:
            return []

        survey_file = next((f for f in downloaded_files if "survey" in f["filename"].lower()), None)
        if not survey_file:
            send_discord_message("No se encontró archivo de encuestas para procesar", success=False)
            return []

        conn = get_sql_connection()
        cursor = conn.cursor()
        new_records = []

        df = pd.read_csv(survey_file["local_path"])
        required_columns = ["survey_id", "employee_id", "survey_date", "response", "satisfaction_score"]
        
        if not all(col in df.columns for col in required_columns):
            message = f"El archivo {survey_file['filename']} no tiene las columnas requeridas: {required_columns}"
            send_discord_message(message, success=False)
            return []

        for _, row in df.iterrows():
            cursor.execute("SELECT survey_id FROM Employee_Surveys WHERE survey_id = %s", 
                         (row["survey_id"],))
            if cursor.fetchone():
                continue

            # Mapear la satisfacción general a diferentes aspectos
            satisfaction_value = row["satisfaction_score"]
            
            cursor.execute(
                """
                INSERT INTO Employee_Surveys 
                (survey_id, employee_id, survey_date, response, satisfaction_score)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (row["survey_id"], row["employee_id"], row["survey_date"], 
                 row["response"], satisfaction_value)
            )
            new_records.append(f"Survey ID: {row['survey_id']}, Employee ID: {row['employee_id']}")

        conn.commit()
        context['task_instance'].xcom_push(key='survey_records', value=new_records)
        
        if new_records:
            send_discord_message(f"Se procesaron {len(new_records)} registros de encuestas", success=True)
        return new_records

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error al procesar datos de encuestas: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def cleanup_files(**context):
    """Limpia archivos temporales y de S3."""
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

        # Eliminar archivos temporales
        for file_info in downloaded_files:
            if os.path.exists(file_info["local_path"]):
                os.remove(file_info["local_path"])
                logging.info(f"Archivo temporal {file_info['local_path']} eliminado")

        # Eliminar archivos de S3
        for file_info in downloaded_files:
            s3_client.delete_object(Bucket=S3_BUCKET, Key=file_info["key"])
            logging.info(f"Archivo {file_info['key']} eliminado de S3")

        send_discord_message("Limpieza de archivos completada", success=True)

    except Exception as e:
        error_msg = f"Error durante la limpieza de archivos: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise

def send_summary(**context):
    """Envía resumen de la operación."""
    try:
        survey_records = context['task_instance'].xcom_pull(task_ids='process_surveys', key='survey_records') or []
        
        if survey_records:
            message = f"""Resumen de la ingesta:
            
Registros de Encuestas procesados: {len(survey_records)}"""
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
    'ingest_employee_surveys',
    default_args=default_args,
    schedule='0 6 * * *',  # Daily at 6 AM
    catchup=False
) as dag:
    
    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_s3_files
    )
    
    process_surveys = PythonOperator(
        task_id='process_surveys',
        python_callable=process_survey_data
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_files',
        python_callable=cleanup_files
    )
    
    summary_task = PythonOperator(
        task_id='send_summary',
        python_callable=send_summary
    )
    
    # Define task dependencies
    download_task >> process_surveys >> cleanup_task >> summary_task
