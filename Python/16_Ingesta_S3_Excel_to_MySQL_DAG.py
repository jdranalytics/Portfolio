import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import tempfile
import boto3
import botocore.exceptions

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración
MYSQL_CONN_ID = "airflow_db"  # Configurado en Airflow
AWS_CONN_ID = "aws_default"  # Configurado en Airflow
S3_BUCKET = "ringoquimico"
S3_PREFIX = "EXCELS/"

# Configuración del correo
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "ringoquimico@gmail.com"
SMTP_PASSWORD = "xxxx xxxx xxxx xxxx"
EMAIL_FROM = "ringoquimico@gmail.com"
EMAIL_TO = "ing.jd.rojas@gmail.com"
EMAIL_SUBJECT_NO_FILES = "No se encontraron archivos Excel en S3"
EMAIL_SUBJECT_NO_CHANGES = "No hubo cambios en la ingesta a MySQL"
EMAIL_SUBJECT_SUCCESS = "Nuevos clientes ingresados"
EMAIL_BODY_NO_FILES = """
No se encontraron archivos Excel en el bucket S3: {bucket}/{prefix}.
"""
EMAIL_BODY_NO_CHANGES = """
No se realizaron cambios en la base de datos. No se encontraron nuevos registros para insertar.
"""
EMAIL_BODY_SUCCESS = """
Se han ingresado los siguientes nuevos clientes a la base de datos:
{new_customers}

Total de Registros Agregados: {total_records}
"""

# Función auxiliar para enviar correos
def send_notification_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        logging.info(f"Intentando enviar correo con asunto: {subject}")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        logging.info(f"Correo enviado correctamente: {subject}")
    except Exception as e:
        logging.error(f"Error al enviar correo de notificación: {e}")
        raise

# Tarea 1: Descargar archivos desde S3 usando boto3
def download_from_s3(ti):
    try:
        # Obtener las credenciales de AWS desde la conexión de Airflow
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        credentials = s3_hook.get_credentials()
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key
        )

        # Usar un directorio temporal compatible con Windows y Ubuntu
        temp_dir = tempfile.gettempdir()
        logging.info(f"Usando directorio temporal: {temp_dir}")
        downloaded_files = []

        # Verificar permisos de escritura en el directorio temporal
        if not os.access(temp_dir, os.W_OK):
            logging.error(f"No se tienen permisos de escritura en {temp_dir}")
            raise PermissionError(f"No se tienen permisos de escritura en {temp_dir}")

        # Listar archivos en S3
        logging.info(f"Listando archivos en bucket {S3_BUCKET} con prefijo {S3_PREFIX}")
        s3_files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
        logging.info(f"Archivos encontrados en S3: {s3_files}")

        # Verificar si hay objetos en el prefijo
        if not s3_files:
            logging.info("No se encontraron objetos en el bucket bajo el prefijo especificado.")
            # Enviar notificación por correo
            send_notification_email(
                EMAIL_SUBJECT_NO_FILES,
                EMAIL_BODY_NO_FILES.format(bucket=S3_BUCKET, prefix=S3_PREFIX)
            )
            return []

        # Filtrar solo archivos .xlsx
        excel_files = [key for key in s3_files if key.endswith(".xlsx")]
        logging.info(f"Archivos .xlsx encontrados: {excel_files}")

        # Si no hay archivos .xlsx, enviar notificación y salir
        if not excel_files:
            logging.info("No se encontraron archivos Excel (.xlsx) en el bucket.")
            send_notification_email(
                EMAIL_SUBJECT_NO_FILES,
                EMAIL_BODY_NO_FILES.format(bucket=S3_BUCKET, prefix=S3_PREFIX)
            )
            return []

        for file_key in excel_files:
            original_filename = file_key.split('/')[-1]
            # Generar un nombre de archivo temporal para el destino final
            temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=".xlsx", delete=False)
            local_file = temp_file.name
            temp_file.close()

            logging.info(f"Descargando {file_key} a {local_file}")
            try:
                # Descargar el archivo directamente usando boto3
                s3_client.download_file(
                    Bucket=S3_BUCKET,
                    Key=file_key,
                    Filename=local_file
                )
                logging.info(f"Descargado {file_key} a {local_file}")
            except botocore.exceptions.ClientError as e:
                logging.error(f"Error al descargar {file_key}: {e}")
                if os.path.exists(local_file):
                    os.remove(local_file)
                continue

            if os.path.isfile(local_file):
                downloaded_files.append({"key": file_key, "local_path": local_file, "original_filename": original_filename})
            else:
                logging.error(f"El archivo {local_file} no se descargó correctamente.")
                if os.path.exists(local_file):
                    os.remove(local_file)

        # Pasar la lista de archivos descargados a la siguiente tarea vía XCom
        ti.xcom_push(key="downloaded_files", value=downloaded_files)
        return downloaded_files

    except Exception as e:
        logging.error(f"Error en download_from_s3: {e}")
        raise

# Tarea 2: Ingestar datos a MySQL
def ingest_to_mysql(ti):
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    downloaded_files = ti.xcom_pull(key="downloaded_files", task_ids="download_from_s3")
    if not downloaded_files:
        logging.info("No hay archivos para procesar.")
        return []

    new_customers = []

    try:
        for file_info in downloaded_files:
            local_file = file_info["local_path"]
            original_filename = file_info["original_filename"]

            df = pd.read_excel(local_file)
            required_columns = ["serial", "first_name", "last_name", "email"]
            if not all(col in df.columns for col in required_columns):
                logging.error(f"El archivo {original_filename} no tiene las columnas requeridas.")
                os.remove(local_file)
                continue

            for _, row in df.iterrows():
                serial = row["serial"]
                cursor.execute("SELECT serial FROM customer WHERE serial = %s", (serial,))
                if cursor.fetchone():
                    logging.info(f"Ignorando serial repetido: {serial}")
                    continue

                cursor.execute(
                    "INSERT INTO customer (serial, first_name, last_name, email, file_name) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (serial, row["first_name"], row["last_name"], row["email"], original_filename)
                )
                new_customers.append(f"Serial: {serial}, Nombre: {row['first_name']} {row['last_name']}, Email: {row['email']}, Archivo: {original_filename}")

        if not new_customers:
            logging.info("No se encontraron nuevos registros para insertar.")
            # Enviar notificación por correo
            send_notification_email(
                EMAIL_SUBJECT_NO_CHANGES,
                EMAIL_BODY_NO_CHANGES
            )
            # Eliminar archivos temporales locales
            for file_info in downloaded_files:
                local_file = file_info["local_path"]
                if os.path.exists(local_file):
                    os.remove(local_file)
                    logging.info(f"Archivo temporal {local_file} eliminado (no hubo cambios).")
        else:
            connection.commit()
            logging.info(f"Se insertaron {len(new_customers)} nuevos registros.")

    except Exception as e:
        logging.error(f"Error en la ingesta: {e}")
        connection.rollback()
        # Asegurarnos de eliminar los archivos temporales en caso de error
        for file_info in downloaded_files:
            local_file = file_info["local_path"]
            if os.path.exists(local_file):
                os.remove(local_file)
                logging.info(f"Archivo temporal {local_file} eliminado (error en ingesta).")
        raise
    finally:
        cursor.close()
        connection.close()

    # Pasar la lista de nuevos clientes a la siguiente tarea
    ti.xcom_push(key="new_customers", value=new_customers)
    return new_customers

# Tarea 3: Enviar correo electrónico (solo si hay nuevos clientes)
def send_email(ti):
    new_customers = ti.xcom_pull(key="new_customers", task_ids="ingest_to_mysql")
    if not new_customers:
        logging.info("No hay nuevos clientes para enviar por correo.")
        return

    total_records = len(new_customers)
    send_notification_email(
        EMAIL_SUBJECT_SUCCESS,
        EMAIL_BODY_SUCCESS.format(new_customers="\n".join(new_customers), total_records=total_records)
    )

# Tarea 4: Eliminar archivos de S3 (si hay archivos descargados, independientemente de si hubo cambios)
def delete_from_s3(ti):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    downloaded_files = ti.xcom_pull(key="downloaded_files", task_ids="download_from_s3")

    if not downloaded_files:
        logging.info("No hay archivos para eliminar de S3.")
        return

    files_to_delete = [file_info["key"] for file_info in downloaded_files]
    for file_key in files_to_delete:
        s3_hook.delete_objects(bucket=S3_BUCKET, keys=[file_key])
        logging.info(f"Archivo {file_key} eliminado de S3.")

    # Eliminar archivos temporales locales si no se hizo antes (por ejemplo, si hubo nuevos clientes)
    for file_info in downloaded_files:
        local_file = file_info["local_path"]
        if os.path.exists(local_file):
            os.remove(local_file)
            logging.info(f"Archivo temporal {local_file} eliminado (después de eliminar de S3).")

# Definir el DAG
default_args = {
    "owner": "JDRP",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 3, 22),
}

with DAG(
    dag_id="ingest_excel_from_s3_to_mysql",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Ejecutar diariamente a las 6:00 AM
    catchup=False,
) as dag:
    # Tarea 1: Descargar desde S3
    download_task = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
    )

    # Tarea 2: Ingestar a MySQL
    ingest_task = PythonOperator(
        task_id="ingest_to_mysql",
        python_callable=ingest_to_mysql,
    )

    # Tarea 3: Enviar correo
    email_task = PythonOperator(
        task_id="send_email",
        python_callable=send_email,
    )

    # Tarea 4: Eliminar de S3
    delete_task = PythonOperator(
        task_id="delete_from_s3",
        python_callable=delete_from_s3,
    )

    # Definir el orden de las tareas
    download_task >> ingest_task >> email_task >> delete_task