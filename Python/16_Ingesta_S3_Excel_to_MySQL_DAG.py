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
EMAIL_SUBJECT = "Nuevos clientes ingresados"
EMAIL_BODY_TEMPLATE = """
Se han ingresado los siguientes nuevos clientes a la base de datos:
{new_customers}

Total de Registros Agregados: {total_records}
"""

def ingest_excel_from_s3_to_mysql():
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    # Usar /tmp en Ubuntu
    temp_dir = "/tmp"

    try:
        # Listar archivos en S3
        s3_files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
        if not s3_files:
            logging.error("No se encontraron archivos en el bucket.")
            return

        new_customers = []
        files_to_delete = []

        for file_key in s3_files:
            if file_key.endswith(".xlsx"):
                original_filename = file_key.split('/')[-1]
                local_file = os.path.join(temp_dir, original_filename)

                # Descargar archivo desde S3
                s3_hook.download_file(key=file_key, bucket_name=S3_BUCKET, local_path=local_file)
                logging.info(f"Descargado {file_key} a {local_file}")

                # Verificar si el archivo existe
                if not os.path.exists(local_file):
                    logging.error(f"El archivo {local_file} no se descargó correctamente.")
                    continue

                # Leer archivo Excel
                df = pd.read_excel(local_file)
                required_columns = ["serial", "first_name", "last_name", "email"]
                if not all(col in df.columns for col in required_columns):
                    logging.error(f"El archivo {file_key} no tiene las columnas requeridas.")
                    os.remove(local_file)
                    continue

                # Procesar cada fila
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

                files_to_delete.append(file_key)
                os.remove(local_file)  # Limpiar archivo temporal
                logging.info(f"Archivo temporal {local_file} eliminado.")

        # Confirmar la transacción
        connection.commit()
        logging.info(f"Se insertaron {len(new_customers)} nuevos registros.")

        # Enviar correo si hay nuevos clientes
        if new_customers:
            send_email(new_customers, len(new_customers))

            # Eliminar archivos de S3 tras éxito
            for file_key in files_to_delete:
                s3_hook.delete_objects(bucket=S3_BUCKET, keys=[file_key])
                logging.info(f"Archivo {file_key} eliminado de S3.")

    except Exception as e:
        logging.error(f"Error: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def send_email(new_customers, total_records):
    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = EMAIL_SUBJECT
    msg.attach(MIMEText(EMAIL_BODY_TEMPLATE.format(new_customers="\n".join(new_customers), total_records=total_records), "plain"))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    logging.info("Correo enviado correctamente.")

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
    ingest_task = PythonOperator(
        task_id="ingest_excel_from_s3_to_mysql",
        python_callable=ingest_excel_from_s3_to_mysql,
    )

    ingest_task