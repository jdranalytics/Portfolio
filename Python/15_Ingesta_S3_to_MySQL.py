import logging
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import boto3
import pymysql
import tempfile
import os

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de AWS S3
S3_BUCKET = "ringoquimico"
S3_PREFIX = "EXCELS/"
AWS_ACCESS_KEY = "xxxxxxxxxx"
AWS_SECRET_KEY = "xxxxxxxxxxxxxxxxxxx"

# Configuración de MySQL
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root123456"
MYSQL_DB = "airflow_db"

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
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    connection = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = connection.cursor()

    # Usar directorio temporal portátil para Windows
    temp_dir = tempfile.gettempdir()

    try:
        # Listar todos los archivos en el prefijo de S3
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        if "Contents" not in response:
            logging.error("No se encontraron archivos en el bucket.")
            return

        new_customers = []
        files_to_delete = []

        for obj in response["Contents"]:
            file_key = obj["Key"]
            if file_key.endswith(".xlsx"):
                # Nombre del archivo original
                original_filename = file_key.split('/')[-1]
                # Ruta temporal en Windows
                local_file = os.path.join(temp_dir, original_filename)

                # Descargar el archivo desde S3
                s3_client.download_file(S3_BUCKET, file_key, local_file)
                logging.info(f"Descargado {file_key} a {local_file}")

                # Verificar si el archivo existe
                if not os.path.exists(local_file):
                    logging.error(f"El archivo {local_file} no se descargó correctamente.")
                    continue

                # Leer el archivo Excel
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
                os.remove(local_file)  # Limpiar archivo temporal después de procesarlo
                logging.info(f"Archivo temporal {local_file} eliminado.")

        # Confirmar la transacción
        connection.commit()
        logging.info(f"Se insertaron {len(new_customers)} nuevos registros.")

        # Enviar correo si hay nuevos clientes
        if new_customers:
            send_email(new_customers, len(new_customers))

            # Eliminar archivos de S3 tras éxito
            for file_key in files_to_delete:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=file_key)
                logging.info(f"Archivo {file_key} eliminado de S3.")

    except Exception as e:
        logging.error(f"Error: {e}")
        connection.rollback()
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

if __name__ == "__main__":
    ingest_excel_from_s3_to_mysql()