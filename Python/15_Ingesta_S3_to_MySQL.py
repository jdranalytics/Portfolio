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
AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXX"

# Configuración de MySQL
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root123456"
MYSQL_DB = "airflow_db"

# Configuración del correo
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "ringoquimico@gmail.com"
SMTP_PASSWORD = "XXXX XXXX XXXX XXXX"
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

def ingest_excel_from_s3_to_mysql():
    # Inicializar cliente S3
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # Conectar a MySQL
    connection = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = connection.cursor()

    # Usar directorio temporal portátil para Windows
    temp_dir = tempfile.gettempdir()
    logging.info(f"Usando directorio temporal: {temp_dir}")

    try:
        # Listar todos los archivos en el prefijo de S3
        logging.info(f"Listando archivos en bucket {S3_BUCKET} con prefijo {S3_PREFIX}")
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
        logging.info(f"Respuesta de S3: {response}")

        # Verificar si hay objetos en el prefijo
        if "Contents" not in response:
            logging.info("No se encontraron objetos en el bucket bajo el prefijo especificado.")
            # Enviar notificación por correo
            send_notification_email(
                EMAIL_SUBJECT_NO_FILES,
                EMAIL_BODY_NO_FILES.format(bucket=S3_BUCKET, prefix=S3_PREFIX)
            )
            return

        # Filtrar archivos .xlsx
        excel_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".xlsx")]
        logging.info(f"Archivos .xlsx encontrados: {excel_files}")

        # Si no hay archivos .xlsx, enviar notificación y salir
        if not excel_files:
            logging.info("No se encontraron archivos Excel (.xlsx) en el bucket.")
            send_notification_email(
                EMAIL_SUBJECT_NO_FILES,
                EMAIL_BODY_NO_FILES.format(bucket=S3_BUCKET, prefix=S3_PREFIX)
            )
            return

        new_customers = []
        files_to_delete = []
        downloaded_files = []

        for file_key in excel_files:
            # Nombre del archivo original
            original_filename = file_key.split('/')[-1]
            # Generar un nombre de archivo temporal único
            temp_file = tempfile.NamedTemporaryFile(dir=temp_dir, suffix=".xlsx", delete=False)
            local_file = temp_file.name
            temp_file.close()

            # Descargar el archivo desde S3
            logging.info(f"Descargando {file_key} a {local_file}")
            s3_client.download_file(S3_BUCKET, file_key, local_file)
            logging.info(f"Descargado {file_key} a {local_file}")

            # Verificar si el archivo existe
            if not os.path.exists(local_file):
                logging.error(f"El archivo {local_file} no se descargó correctamente.")
                continue

            downloaded_files.append({"key": file_key, "local_path": local_file, "original_filename": original_filename})

        if not downloaded_files:
            logging.info("No se descargaron archivos Excel para procesar.")
            return

        # Procesar los archivos descargados
        for file_info in downloaded_files:
            local_file = file_info["local_path"]
            file_key = file_info["key"]
            original_filename = file_info["original_filename"]

            # Leer el archivo Excel
            df = pd.read_excel(local_file)
            required_columns = ["serial", "first_name", "last_name", "email"]
            if not all(col in df.columns for col in required_columns):
                logging.error(f"El archivo {original_filename} no tiene las columnas requeridas.")
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
            # Eliminar archivos de S3
            for file_key in files_to_delete:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=file_key)
                logging.info(f"Archivo {file_key} eliminado de S3 (no hubo cambios).")
        else:
            # Confirmar la transacción
            connection.commit()
            logging.info(f"Se insertaron {len(new_customers)} nuevos registros.")

            # Enviar correo si hay nuevos clientes
            send_notification_email(
                EMAIL_SUBJECT_SUCCESS,
                EMAIL_BODY_SUCCESS.format(new_customers="\n".join(new_customers), total_records=len(new_customers))
            )

            # Eliminar archivos de S3 tras éxito
            for file_key in files_to_delete:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=file_key)
                logging.info(f"Archivo {file_key} eliminado de S3 (éxito).")

            # Eliminar archivos temporales locales
            for file_info in downloaded_files:
                local_file = file_info["local_path"]
                if os.path.exists(local_file):
                    os.remove(local_file)
                    logging.info(f"Archivo temporal {local_file} eliminado (éxito).")

    except Exception as e:
        logging.error(f"Error: {e}")
        connection.rollback()
        # Asegurarnos de eliminar los archivos temporales en caso de error
        for file_info in downloaded_files:
            local_file = file_info["local_path"]
            if os.path.exists(local_file):
                os.remove(local_file)
                logging.info(f"Archivo temporal {local_file} eliminado (error).")
        raise
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    ingest_excel_from_s3_to_mysql()