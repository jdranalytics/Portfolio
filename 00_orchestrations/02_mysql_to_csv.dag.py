import logging
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator

# Configuración de la conexión MySQL (debe coincidir con el Conn Id configurado en Airflow)
MYSQL_CONN_ID = "airflow_db"

# Ruta donde se guardará el archivo CSV temporalmente
OUTPUT_FILE_PATH = "/mnt/c/Users/joey_/Desktop/AIRFLOW/CSV/customer.csv"  # Ruta en formato Linux

# Configuración del correo electrónico
EMAIL_FROM = "ringoquimico@gmail.com"  # Correo desde el que se envía
EMAIL_TO = "ing.jd.rojas@gmail.com"    # Correo al que se envía
EMAIL_SUBJECT = "Exportación de datos desde MySQL"
EMAIL_BODY = "Adjunto encontrarás el archivo CSV con los datos exportados desde MySQL."

# Configuración del servidor SMTP (para Gmail)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "ringoquimico@gmail.com"  # Correo desde el que se envía
SMTP_PASSWORD = "XXXX XXXX XXXX XXXX"  # Contraseña de aplicación de Gmail

def mysql_extract():
    """
    Función para exportar datos desde MySQL a un archivo CSV.
    """
    connection = None
    cursor = None
    try:
        # Conectar a MySQL usando el hook
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        logging.info("Conectado a MySQL correctamente.")

        # Definir la consulta SQL
        query = """
            SELECT * 
            FROM CUSTOMER 
            WHERE first_name = 'john'
        """

        # Ejecutar la consulta y obtener los datos
        logging.info("Ejecutando la consulta SQL...")
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        # Crear la carpeta si no existe
        os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)

        # Exportar los datos a un archivo CSV
        logging.info(f"Exportando datos a {OUTPUT_FILE_PATH}")
        with open(OUTPUT_FILE_PATH, "w", encoding="utf-8") as file:
            # Escribir el encabezado (nombres de las columnas)
            column_names = [i[0] for i in cursor.description]
            file.write(",".join(column_names) + "\n")

            # Escribir los datos
            for row in results:
                file.write(",".join(map(str, row)) + "\n")

        logging.info("Exportación completada correctamente.")

    except Exception as e:
        logging.error(f"Error durante la exportación: {e}")
        raise
    finally:
        # Cerrar la conexión y el cursor
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def send_email():
    """
    Función para enviar el archivo CSV por correo electrónico.
    """
    try:
        # Verificar que el archivo CSV exista
        if not os.path.exists(OUTPUT_FILE_PATH):
            raise FileNotFoundError(f"El archivo {OUTPUT_FILE_PATH} no existe.")

        # Crear el mensaje de correo
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg["Subject"] = EMAIL_SUBJECT
        msg.attach(MIMEText(EMAIL_BODY, "plain"))

        # Adjuntar el archivo CSV
        with open(OUTPUT_FILE_PATH, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(OUTPUT_FILE_PATH)}",
            )
            msg.attach(part)

        # Enviar el correo
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        logging.info("Correo enviado correctamente.")

    except Exception as e:
        logging.error(f"Error al enviar el correo: {e}")
        raise

# Definir el DAG
with DAG(
    dag_id="mysql_export_to_csv_and_email",
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(days=1),  # Ejecutar diariamente
    catchup=False,  # No ejecutar tareas pasadas
    default_args={
        "retries": 1,  # Número de reintentos
        "retry_delay": timedelta(minutes=5),  # Tiempo entre reintentos
    },
) as dag:

    # Tarea para exportar datos desde MySQL
    export_task = PythonOperator(
        task_id="export_mysql_to_csv",
        python_callable=mysql_extract,
    )

    # Tarea para enviar el archivo CSV por correo
    email_task = PythonOperator(
        task_id="send_csv_via_email",
        python_callable=send_email,
    )

    # Definir el flujo de tareas
    export_task >> email_task