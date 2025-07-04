import logging
import smtplib
import os
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy import create_engine, text

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de la conexión MySQL
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root123456"
MYSQL_DB = "airflow_db"

# Ruta donde se encuentra el archivo Excel (ajustada para Windows)
EXCEL_FOLDER_PATH = r"C:\Users\joey_\Desktop\AIRFLOW\EXCEL"

# Configuración del correo electrónico
EMAIL_FROM = "ringoquimico@gmail.com"
EMAIL_TO = "ing.jd.rojas@gmail.com"
EMAIL_SUBJECT = "Nuevos clientes ingresados"
EMAIL_BODY_TEMPLATE = """
Se han ingresado los siguientes nuevos clientes a la base de datos:
{new_customers}
"""

# Configuración del servidor SMTP (para Gmail)
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "ringoquimico@gmail.com"
SMTP_PASSWORD = "xxxx xxxx xxxx xxxx"

def get_latest_excel_file():
    """
    Obtiene el archivo Excel más reciente en la carpeta que sigue el formato "customer yyyy-mm-dd.xlsx".
    """
    try:
        if not os.path.exists(EXCEL_FOLDER_PATH):
            raise FileNotFoundError(f"El directorio {EXCEL_FOLDER_PATH} no existe.")

        files = [f for f in os.listdir(EXCEL_FOLDER_PATH) if f.startswith("customer") and f.endswith(".xlsx")]
        if not files:
            raise FileNotFoundError("No se encontraron archivos Excel en la carpeta.")

        latest_file = None
        latest_date = None
        for file in files:
            try:
                file_date_str = file.split(" ")[1].split(".")[0]
                file_date = pd.to_datetime(file_date_str)
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

        # Conectar a MySQL usando SQLAlchemy
        engine = create_engine(f'mysql+mysqldb://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}')
        # Si usas pymysql, cambia a:
        # engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}')

        # Insertar datos en la tabla `customer`
        new_customers = []
        with engine.connect() as connection:
            for _, row in df.iterrows():
                serial = row["serial"]
                first_name = row["first_name"]
                last_name = row["last_name"]
                email = row["email"]

                # Verificar si el serial ya existe
                query = text("SELECT serial FROM customer WHERE serial = :serial")
                result = connection.execute(query, {"serial": serial})
                if result.fetchone():
                    logging.info(f"Ignorando registro con serial repetido: {serial}")
                    continue

                # Insertar el nuevo registro
                insert_query = text(
                    "INSERT INTO customer (serial, first_name, last_name, email) "
                    "VALUES (:serial, :first_name, :last_name, :email)"
                )
                connection.execute(
                    insert_query,
                    {
                        "serial": serial,
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email
                    }
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

def send_email(new_customers):
    """
    Envía un correo electrónico con los nuevos clientes ingresados.
    """
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg["Subject"] = EMAIL_SUBJECT
        msg.attach(MIMEText(EMAIL_BODY_TEMPLATE.format(new_customers="\n".join(new_customers)), "plain"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        logging.info("Correo enviado correctamente.")

    except Exception as e:
        logging.error(f"Error al enviar el correo: {e}")
        raise

if __name__ == "__main__":
    ingest_excel_to_mysql()