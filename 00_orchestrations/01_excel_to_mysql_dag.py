import logging
import smtplib
import os
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator

# Configuración de la conexión MySQL (debe coincidir con el Conn Id configurado en Airflow)
MYSQL_CONN_ID = "airflow_db"

# Ruta donde se encuentra el archivo Excel (ajustada para WSL)
EXCEL_FOLDER_PATH = "/mnt/c/Users/joey_/Desktop/AIRFLOW/EXCEL"

# Configuración del correo electrónico
EMAIL_FROM = "ringoquimico@gmail.com"  # Correo desde el que se envía
EMAIL_TO = "ing.jd.rojas@gmail.com"    # Correo al que se envía
EMAIL_SUBJECT = "Nuevos clientes ingresados"
EMAIL_BODY_TEMPLATE = """
Se han ingresado los siguientes nuevos clientes a la base de datos:
{new_customers}
"""

# Configuración del servidor SMTP (para Gmail)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "ringoquimico@gmail.com"  # Correo desde el que se envía
SMTP_PASSWORD = "xxxx xxxx xxxx xxxx" # Contraseña de aplicación de Gmail

def get_latest_excel_file():
    """
    Obtiene el archivo Excel más reciente en la carpeta que sigue el formato "customer yyyy-mm-dd.xlsx".
    """
    try:
        # Verificar si el directorio existe
        if not os.path.exists(EXCEL_FOLDER_PATH):
            raise FileNotFoundError(f"El directorio {EXCEL_FOLDER_PATH} no existe. Asegúrate de que la carpeta esté disponible.")

        # Listar archivos en la carpeta
        files = [f for f in os.listdir(EXCEL_FOLDER_PATH) if f.startswith("customer") and f.endswith(".xlsx")]
        if not files:
            raise FileNotFoundError("No se encontraron archivos Excel en la carpeta.")

        # Obtener el archivo más reciente basado en la fecha en el nombre
        latest_file = None
        latest_date = None
        for file in files:
            try:
                # Extraer la fecha del nombre del archivo
                file_date_str = file.split(" ")[1].split(".")[0]
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")

                # Comparar con la fecha más reciente
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = file
            except (IndexError, ValueError):
                logging.warning(f"El archivo {file} no sigue el formato esperado 'customer yyyy-mm-dd.xlsx'.")
                continue

        if not latest_file:
            raise ValueError("No se encontró ningún archivo válido con el formato 'customer yyyy-mm-dd.xlsx'.")

        return os.path.join(EXCEL_FOLDER_PATH, latest_file)
    except Exception as e:
        logging.error(f"Error al obtener el archivo Excel: {e}")
        raise

def ingest_excel_to_mysql():
    """
    Lee el archivo Excel y lo ingresa en la tabla `customer` de MySQL.
    """
    connection = None
    cursor = None
    try:
        # Obtener el archivo Excel más reciente
        excel_file = get_latest_excel_file()
        logging.info(f"Leyendo archivo Excel: {excel_file}")

        # Leer el archivo Excel
        df = pd.read_excel(excel_file)

        # Validar la estructura del archivo
        required_columns = ["serial", "first_name", "last_name", "email"]
        if not all(column in df.columns for column in required_columns):
            raise ValueError(f"El archivo Excel no tiene las columnas requeridas: {required_columns}")

        # Conectar a MySQL
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()

        # Insertar datos en la tabla `customer`
        new_customers = []
        for _, row in df.iterrows():
            serial = row["serial"]
            first_name = row["first_name"]
            last_name = row["last_name"]
            email = row["email"]

            # Verificar si el serial ya existe
            cursor.execute("SELECT serial FROM customer WHERE serial = %s", (serial,))
            if cursor.fetchone():
                logging.info(f"Ignorando registro con serial repetido: {serial}")
                continue

            # Insertar el nuevo registro
            cursor.execute(
                "INSERT INTO customer (serial, first_name, last_name, email) VALUES (%s, %s, %s, %s)",
                (serial, first_name, last_name, email),
            )
            new_customers.append(f"Serial: {serial}, Nombre: {first_name} {last_name}, Email: {email}")

        # Confirmar la transacción
        connection.commit()
        logging.info(f"Se insertaron {len(new_customers)} nuevos registros.")

        # Enviar correo electrónico con los nuevos clientes
        if new_customers:
            send_email(new_customers)

    except Exception as e:
        logging.error(f"Error durante la ingesta de datos: {e}")
        raise
    finally:
        # Cerrar la conexión y el cursor
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def send_email(new_customers):
    """
    Envía un correo electrónico con los nuevos clientes ingresados.
    """
    try:
        # Crear el mensaje de correo
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg["Subject"] = EMAIL_SUBJECT
        msg.attach(MIMEText(EMAIL_BODY_TEMPLATE.format(new_customers="\n".join(new_customers)), "plain"))

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
    dag_id="ingest_excel_to_mysql",
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(days=1),  # Ejecutar diariamente
    catchup=False,  # No ejecutar tareas pasadas
    default_args={
        "retries": 1,  # Número de reintentos
        "retry_delay": timedelta(minutes=5),  # Tiempo entre reintentos
    },
) as dag:

    # Tarea para ingestar datos desde Excel a MySQL
    ingest_task = PythonOperator(
        task_id="ingest_excel_to_mysql",
        python_callable=ingest_excel_to_mysql,
    )

    ingest_task