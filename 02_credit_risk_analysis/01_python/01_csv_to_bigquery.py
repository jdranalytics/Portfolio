from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
import os
import requests
from datetime import datetime

# Configuración de Discord
DISCORD_WEBHOOK_URL = "XXXXXXXXXXXXXXXXXXXXXXXX"

def send_discord_message(message, success=True):
    """Envía un mensaje a Discord con el estado de la ejecución"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = 0x00FF00 if success else 0xFF0000  # Verde para éxito, rojo para error
        emoji = "✅" if success else "❌"
        
        payload = {
            "embeds": [{
                "title": f"{emoji} Notificación de Carga BigQuery",
                "description": message,
                "color": color,
                "fields": [{"name": "Timestamp", "value": timestamp, "inline": True}],
                "footer": {"text": "Sistema de Monitoreo ETL"}
            }]
        }
        
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code != 204:
            print(f"Error al enviar mensaje a Discord: {response.text}")
    except Exception as e:
        print(f"Error al enviar mensaje a Discord: {e}")

def load_csv_to_bigquery():
    # Configura las credenciales
    credentials_path = r"C:\Users\joey_\Desktop\AIRFLOW\adroit-terminus-450816-r9-1b90cfcf6a76.json"
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"No se encontró el archivo de credenciales: {credentials_path}")
    
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    # Configuración del proyecto y dataset
    PROJECT_ID = "adroit-terminus-450816-r9"
    DATASET_ID = "solicitudes_credito"
    TABLE_ID = "solicitudes"
    
    # Ruta al archivo CSV
    csv_file_path = r"C:\Users\joey_\Documents\Visual Code (Clone)\Portfolio\Data Sources\dataset_credito_sintetico_temporal.csv"
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"No se encontró el archivo CSV: {csv_file_path}")
    if os.path.getsize(csv_file_path) == 0:
        raise ValueError(f"El archivo CSV está vacío: {csv_file_path}")

    # Configuración del job para reemplazar datos
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Si tu CSV tiene encabezados
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Reemplaza los datos existentes
        autodetect=True,  # Detecta automáticamente el esquema
        field_delimiter=","  # Ajusta si tu CSV usa otro delimitador
    )

    # Crea el cliente de BigQuery
    client = bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )

    # Notificación de inicio
    file_size = os.path.getsize(csv_file_path) / 1024 / 1024
    start_message = f"Iniciando carga de datos a BigQuery\n" \
                    f"- Dataset: {DATASET_ID}\n" \
                    f"- Tabla: {TABLE_ID}\n" \
                    f"- Tamaño archivo: {file_size:.2f} MB"
    print(f"Cargando {csv_file_path} a {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    send_discord_message(start_message, success=True)

    # Carga el archivo CSV
    try:
        with open(csv_file_path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
                job_config=job_config
            )

        # Espera a que el job termine
        job.result()

        # Notificación de éxito (sin total_bytes_processed)
        success_message = f"✅ Carga completada con éxito!\n" \
                         f"- Tabla: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}\n" \
                         f"- Filas cargadas: {job.output_rows:,}"
        print(success_message)
        send_discord_message(success_message, success=True)
        return True

    except GoogleAPICallError as e:
        # Notificación de error específico de la API
        error_message = f"⚠️ Error en la API de Google:\n" \
                        f"- Error: {str(e)}\n" \
                        f"- Código: {getattr(e, 'code', 'N/A')}\n" \
                        f"- Dataset: {DATASET_ID}\n" \
                        f"- Tabla: {TABLE_ID}"
        print(error_message)
        send_discord_message(error_message, success=False)
        return False

    except Exception as e:
        # Notificación de error genérico
        error_message = f"❌ Error inesperado:\n" \
                        f"- Tipo: {type(e).__name__}\n" \
                        f"- Detalles: {str(e)}\n" \
                        f"- Dataset: {DATASET_ID}\n" \
                        f"- Tabla: {TABLE_ID}"
        print(error_message)
        send_discord_message(error_message, success=False)
        return False

if __name__ == "__main__":
    success = load_csv_to_bigquery()
    if not success:
        solution_message = "Posibles soluciones:\n" \
                          "1. Verifica tu conexión a internet\n" \
                          "2. Revisa el estado del servicio en https://status.cloud.google.com/\n" \
                          "3. Intenta con un archivo más pequeño para probar\n" \
                          "4. Verifica los permisos de la cuenta de servicio\n" \
                          "5. Considera cargar primero a Google Cloud Storage y luego a BigQuery"
        print(solution_message)
        send_discord_message(solution_message, success=False)