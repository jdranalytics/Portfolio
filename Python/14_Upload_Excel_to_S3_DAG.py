from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Función para subir archivos a S3 y enviar correo
def upload_to_s3_and_notify():
    # Configuración para S3
    local_folder_path = "/mnt/c/Users/joey_/Desktop/AIRFLOW/EXCEL"  # Ruta ajustada para WSL
    s3_bucket = "testeando"  # Nombre de tu bucket
    s3_prefix = "EXCELS/"  # Prefijo (carpeta) dentro del bucket
    aws_conn_id = "aws_default"  # ID de la conexión configurada en Airflow

    # Configuración del servidor SMTP (para Gmail)
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    SMTP_USER = "ringoquimico@gmail.com"  # Correo desde el que se envía
    SMTP_PASSWORD = "xxxx xxxx xxxx xxxx"  # Contraseña de aplicación de Gmail
    EMAIL_FROM = "ringoquimico@gmail.com"  # Remitente
    EMAIL_TO = "ing.jd.rojas@gmail.com"  # Destinatario

    # Verificar que la carpeta existe
    if not os.path.exists(local_folder_path):
        raise FileNotFoundError(f"La carpeta {local_folder_path} no existe.")

    # Crear un hook de S3 para interactuar con S3
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # Lista para almacenar los archivos subidos
    uploaded_files = []

    # Iterar sobre los archivos en la carpeta
    for filename in os.listdir(local_folder_path):
        if filename.endswith(".xlsx"):  # Solo subir archivos Excel
            local_file_path = os.path.join(local_folder_path, filename)
            s3_key = f"{s3_prefix}{filename}"  # Ruta en S3

            # Subir el archivo a S3
            try:
                s3_hook.load_file(
                    filename=local_file_path,
                    key=s3_key,
                    bucket_name=s3_bucket,
                    replace=True  # Sobrescribir si ya existe
                )
                print(f"Archivo {local_file_path} subido exitosamente a s3://{s3_bucket}/{s3_key}")
                uploaded_files.append((filename, s3_key))  # Guardar el nombre y la ruta en S3
            except Exception as e:
                print(f"Error al subir el archivo {filename}: {str(e)}")

    # Enviar correo de notificación si se subieron archivos
    if uploaded_files:
        subject = "Notificación: Archivos subidos a S3"
        body = "Los siguientes archivos han sido subidos exitosamente a S3:\n\n"
        for filename, s3_key in uploaded_files:
            body += f"- {filename}: s3://{s3_bucket}/{s3_key}\n"

        # Crear el mensaje de correo
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # Conectar al servidor SMTP y enviar el correo
        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()  # Habilitar TLS
                server.login(SMTP_USER, SMTP_PASSWORD)  # Iniciar sesión
                server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())  # Enviar correo
                print(f"Correo enviado a {EMAIL_TO} con la notificación.")
        except Exception as e:
            print(f"Error al enviar el correo: {str(e)}")
    else:
        print("No se subieron archivos, no se enviará correo.")

# Definir los argumentos por defecto del DAG
default_args = {
    "owner": "JDRP",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 22),  # Fecha de inicio
    "retries": 1,
}

# Definir el DAG
with DAG(
    dag_id="upload_excel_to_s3_daily",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Ejecutar diariamente a las 6:00 AM
    catchup=False,  # No ejecutar tareas pasadas
) as dag:

    # Tarea para subir archivos y enviar correo
    upload_task = PythonOperator(
        task_id="upload_excel_to_s3_and_notify",
        python_callable=upload_to_s3_and_notify,
    )