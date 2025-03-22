import boto3
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configuración para S3
local_folder_path = r"C:\Users\joey_\Desktop\AIRFLOW\EXCEL"  # Carpeta con los archivos Excel
s3_bucket = "testeando"  # Nombre de tu bucket
s3_prefix = "EXCELS/"  # Prefijo (carpeta) dentro del bucket

# Ajustar la ruta para WSL (si es necesario)
# local_folder_path = "/mnt/c/Users/joey_/Desktop/AIRFLOW/EXCEL"

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

# Crear un cliente de S3
# Opción 1: Usar credenciales configuradas automáticamente (recomendado)
s3_client = boto3.client("s3")

# Opción 2: Introducir credenciales directamente (NO recomendado, solo si no puedes configurarlas de otra forma)
# Descomenta las siguientes líneas y reemplaza con tus credenciales si decides usar esta opción
s3_client = boto3.client(
    "s3",
     aws_access_key_id="XXXXXXXXXXXXXXXXXXXXX",
     aws_secret_access_key="XXXXXXXXXXXXXXXXXXXXXX",
     region_name="us-east-2"  # Cambia a la región de tu bucket
 )

# Lista para almacenar los archivos subidos
uploaded_files = []

# Iterar sobre los archivos en la carpeta
for filename in os.listdir(local_folder_path):
    if filename.endswith(".xlsx"):  # Solo subir archivos Excel
        local_file_path = os.path.join(local_folder_path, filename)
        s3_key = f"{s3_prefix}{filename}"  # Ruta en S3

        # Subir el archivo a S3
        try:
            s3_client.upload_file(
                Filename=local_file_path,
                Bucket=s3_bucket,
                Key=s3_key
            )
            print(f"Archivo {local_file_path} subido exitosamente a s3://{s3_bucket}/{s3_key}")
            uploaded_files.append((filename, s3_key))  # Nombre y ruta en S3
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