from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
import os
import requests
from datetime import datetime

# Configuración de Discord
DISCORD_WEBHOOK_URL = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

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
                "footer": {"text": "Sistema de Monitoreo ETL: Proyecto Solicitudes de Crédito"}
            }]
        }
        
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code != 204:
            print(f"Error al enviar mensaje a Discord: {response.text}")
    except Exception as e:
        print(f"Error al enviar mensaje a Discord: {e}")

# Configuración de BigQuery
PROJECT_ID = "adroit-terminus-450816-r9"
DATASET_ID = "solicitudes_credito"
TABLE_ID = "solicitudes"
CREDENTIALS_PATH = r"C:\Users\joey_\Desktop\AIRFLOW\adroit-terminus-450816-r9-1b90cfcf6a76.json"

def get_bigquery_client():
    """Crea y retorna un cliente de BigQuery"""
    if not os.path.exists(CREDENTIALS_PATH):
        raise FileNotFoundError(f"No se encontró el archivo de credenciales: {CREDENTIALS_PATH}")
    
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_csv_to_bigquery():
    """Carga el CSV a la tabla solicitudes"""
    csv_file_path = r"C:\Users\joey_\Documents\Visual Code (Clone)\Portfolio\Data Sources\dataset_credito_sintetico_temporal.csv"
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"No se encontró el archivo CSV: {csv_file_path}")
    if os.path.getsize(csv_file_path) == 0:
        raise ValueError(f"El archivo CSV está vacío: {csv_file_path}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        field_delimiter=","
    )

    client = get_bigquery_client()
    file_size = os.path.getsize(csv_file_path) / 1024 / 1024
    start_message = f"Iniciando carga de datos a BigQuery\n" \
                    f"- Dataset: {DATASET_ID}\n" \
                    f"- Tabla: {TABLE_ID}\n" \
                    f"- Tamaño archivo: {file_size:.2f} MB"
    print(start_message)
    send_discord_message(start_message, success=True)

    try:
        with open(csv_file_path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
                job_config=job_config
            )
        job.result()
        success_message = f"✅ Carga completada con éxito!\n" \
                         f"- Tabla: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}\n" \
                         f"- Filas cargadas: {job.output_rows:,}"
        print(success_message)
        send_discord_message(success_message, success=True)
        return True
    except GoogleAPICallError as e:
        error_message = f"⚠️ Error en la API de Google:\n" \
                        f"- Error: {str(e)}\n" \
                        f"- Código: {getattr(e, 'code', 'N/A')}\n" \
                        f"- Dataset: {DATASET_ID}\n" \
                        f"- Tabla: {TABLE_ID}"
        print(error_message)
        send_discord_message(error_message, success=False)
        return False
    except Exception as e:
        error_message = f"❌ Error inesperado:\n" \
                        f"- Tipo: {type(e).__name__}\n" \
                        f"- Detalles: {str(e)}\n" \
                        f"- Dataset: {DATASET_ID}\n" \
                        f"- Tabla: {TABLE_ID}"
        print(error_message)
        send_discord_message(error_message, success=False)
        return False

def run_query(query, task_name):
    """Ejecuta una consulta en BigQuery y maneja errores"""
    client = get_bigquery_client()
    start_message = f"Iniciando tarea: {task_name}"
    print(start_message)
    send_discord_message(start_message, success=True)

    try:
        query_job = client.query(query)
        query_job.result()
        success_message = f"✅ Tarea completada: {task_name}\n" \
                         f"- Filas afectadas: {query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else 'N/A'}"
        print(success_message)
        send_discord_message(success_message, success=True)
        return True
    except GoogleAPICallError as e:
        error_message = f"⚠️ Error en la API de Google (tarea: {task_name}):\n" \
                        f"- Error: {str(e)}\n" \
                        f"- Código: {getattr(e, 'code', 'N/A')}"
        print(error_message)
        send_discord_message(error_message, success=False)
        return False
    except Exception as e:
        error_message = f"❌ Error inesperado (tarea: {task_name}):\n" \
                        f"- Tipo: {type(e).__name__}\n" \
                        f"- Detalles: {str(e)}"
        print(error_message)
        send_discord_message(error_message, success=False)
        return False

def update_aggregated_table():
    """Actualiza la tabla solicitudes_agregadas"""
    query = """
    CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.solicitudes_agregadas` AS
    SELECT
      DATE_TRUNC(fecha_solicitud, MONTH) AS fecha_mes,
      COUNT(*) AS total_solicitudes,
      COUNTIF(solicitud_credito IS NOT NULL) AS solicitudes_revisadas,
      SUM(CASE WHEN solicitud_credito = 1 THEN 1 ELSE 0 END) AS solicitudes_aprobadas,
      SAFE_DIVIDE(
        SUM(CASE WHEN solicitud_credito = 1 THEN 1 ELSE 0 END),
        COUNTIF(solicitud_credito IS NOT NULL)
      ) * 100 AS tasa_aprobacion
    FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`
    GROUP BY fecha_mes
    ORDER BY fecha_mes;
    """
    return run_query(query, "Actualizar tabla solicitudes_agregadas")

def retrain_forecast_models():
    """Reentrena los modelos de pronóstico"""
    queries = [
        (
            """
            CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_solicitudes`
            OPTIONS(
                model_type='ARIMA_PLUS',
                time_series_timestamp_col='fecha_mes',
                time_series_data_col='total_solicitudes',
                data_frequency='MONTHLY',
                horizon=6
            ) AS
            SELECT
                fecha_mes,
                total_solicitudes
            FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes_agregadas`;
            """,
            "Reentrenar modelo_solicitudes"
        ),
        (
            """
            CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_aprobadas`
            OPTIONS(
              model_type='ARIMA_PLUS',
              time_series_timestamp_col='fecha_mes',
              time_series_data_col='solicitudes_aprobadas',
              data_frequency='MONTHLY',
              horizon=6
            ) AS
            SELECT
              fecha_mes,
              solicitudes_aprobadas
            FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes_agregadas`;
            """,
            "Reentrenar modelo_aprobadas"
        ),
        (
            """
            CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_tasa_aprobacion`
            OPTIONS(
              model_type='ARIMA_PLUS',
              time_series_timestamp_col='fecha_mes',
              time_series_data_col='tasa_aprobacion',
              data_frequency='MONTHLY',
              horizon=6
            ) AS
            SELECT
              fecha_mes,
              tasa_aprobacion
            FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes_agregadas`;
            """,
            "Reentrenar modelo_tasa_aprobacion"
        )
    ]

    for query, task_name in queries:
        if not run_query(query, task_name):
            return False
    return True

def update_forecasts():
    """Actualiza los pronósticos"""
    queries = [
        (
            """
            CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.forecast_solicitudes` AS
            SELECT
              forecast_timestamp AS fecha_mes,
              forecast_value AS total_solicitudes_pred,
              prediction_interval_lower_bound AS total_solicitudes_lower,
              prediction_interval_upper_bound AS total_solicitudes_upper
            FROM ML.FORECAST(
              MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_solicitudes`,
              STRUCT(6 AS horizon, 0.95 AS confidence_level)
            );
            """,
            "Actualizar forecast_solicitudes"
        ),
        (
            """
            CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.forecast_aprobadas` AS
            SELECT
              forecast_timestamp AS fecha_mes,
              forecast_value AS solicitudes_aprobadas_pred,
              prediction_interval_lower_bound AS solicitudes_aprobadas_lower,
              prediction_interval_upper_bound AS solicitudes_aprobadas_upper
            FROM ML.FORECAST(
              MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_aprobadas`,
              STRUCT(6 AS horizon, 0.95 AS confidence_level)
            );
            """,
            "Actualizar forecast_aprobadas"
        ),
        (
            """
            CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.forecast_tasa_aprobacion` AS
            SELECT
              forecast_timestamp AS fecha_mes,
              forecast_value AS tasa_aprobacion_pred,
              prediction_interval_lower_bound AS tasa_aprobacion_lower,
              prediction_interval_upper_bound AS tasa_aprobacion_upper
            FROM ML.FORECAST(
              MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_tasa_aprobacion`,
              STRUCT(6 AS horizon, 0.95 AS confidence_level)
            );
            """,
            "Actualizar forecast_tasa_aprobacion"
        ),
        (
            """
            CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.forecast_final` AS
            SELECT
              s.fecha_mes,
              s.total_solicitudes_pred,
              s.total_solicitudes_lower,
              s.total_solicitudes_upper,
              a.solicitudes_aprobadas_pred,
              a.solicitudes_aprobadas_lower,
              a.solicitudes_aprobadas_upper,
              t.tasa_aprobacion_pred,
              t.tasa_aprobacion_lower,
              t.tasa_aprobacion_upper
            FROM `adroit-terminus-450816-r9.solicitudes_credito.forecast_solicitudes` s
            JOIN `adroit-terminus-450816-r9.solicitudes_credito.forecast_aprobadas` a
              ON s.fecha_mes = a.fecha_mes
            JOIN `adroit-terminus-450816-r9.solicitudes_credito.forecast_tasa_aprobacion` t
              ON s.fecha_mes = t.fecha_mes
            ORDER BY s.fecha_mes;
            """,
            "Actualizar forecast_final"
        )
    ]

    for query, task_name in queries:
        if not run_query(query, task_name):
            return False
    return True

def retrain_clustering_model():
    """Reentrena el modelo de clustering"""
    query = """
    CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering`
    OPTIONS(
      model_type='kmeans',
      num_clusters=3
    ) AS
    SELECT
      edad,
      ingresos_anuales,
      puntaje_crediticio,
      deuda_actual,
      antiguedad_laboral,
      numero_dependientes
    FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`;
    """
    return run_query(query, "Reentrenar modelo_clustering")

def update_clusters():
    """Actualiza las tablas de clusters"""
    queries = [
        (
            """
            CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.predicciones_clustering` AS
            SELECT
              id_cliente,
              CENTROID_ID AS cluster
            FROM ML.PREDICT(MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering`,
              (SELECT
                id_cliente,
                edad,
                ingresos_anuales,
                puntaje_crediticio,
                deuda_actual,
                antiguedad_laboral,
                numero_dependientes
              FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`));
            """,
            "Actualizar predicciones_clustering"
        ),
        (
            """
            CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.clusters_con_datos` AS
            SELECT
              c.id_cliente,
              c.cluster,
              s.edad,
              s.ingresos_anuales,
              s.puntaje_crediticio,
              s.deuda_actual,
              s.antiguedad_laboral,
              s.numero_dependientes,
              s.estado_civil,
              s.tipo_empleo,
              s.solicitud_credito
            FROM `adroit-terminus-450816-r9.solicitudes_credito.predicciones_clustering` c
            JOIN `adroit-terminus-450816-r9.solicitudes_credito.solicitudes` s
              ON c.id_cliente = s.id_cliente;
            """,
            "Actualizar clusters_con_datos"
        )
    ]

    for query, task_name in queries:
        if not run_query(query, task_name):
            return False
    return True

def retrain_logistic_model():
    """Reentrena el modelo de regresión logística"""
    query = """
    CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_aprobacion`
    OPTIONS(
      model_type='logistic_reg',
      input_label_cols=['solicitud_credito']
    ) AS
    SELECT
      edad,
      ingresos_anuales,
      puntaje_crediticio,
      historial_pagos,
      deuda_actual,
      antiguedad_laboral,
      estado_civil,
      numero_dependientes,
      tipo_empleo,
      solicitud_credito
    FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`
    WHERE solicitud_credito IS NOT NULL;
    """
    return run_query(query, "Reentrenar modelo_aprobacion")

def update_logistic_predictions():
    """Actualiza las predicciones del modelo de regresión logística"""
    query = """
    CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.predicciones_aprobaciones_reglog` AS
    SELECT
      id_cliente,
      predicted_solicitud_credito,
      predicted_solicitud_credito_probs
    FROM ML.PREDICT(MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_aprobacion`,
      (SELECT
        id_cliente,
        edad,
        ingresos_anuales,
        puntaje_crediticio,
        historial_pagos,
        deuda_actual,
        antiguedad_laboral,
        estado_civil,
        numero_dependientes,
        tipo_empleo
      FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes` 
      WHERE solicitud_credito IS NULL));
    """
    return run_query(query, "Actualizar predicciones_aprobaciones_reglog")

def update_combined_table():
    """Actualiza la tabla combinada de históricos y pronósticos"""
    query = """
    CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.forecast_combinado` AS
    SELECT
      fecha_mes,
      total_solicitudes AS total_solicitudes,
      NULL AS total_solicitudes_lower,
      NULL AS total_solicitudes_upper,
      solicitudes_aprobadas AS solicitudes_aprobadas,
      NULL AS solicitudes_aprobadas_lower,
      NULL AS solicitudes_aprobadas_upper,
      tasa_aprobacion AS tasa_aprobacion,
      NULL AS tasa_aprobacion_lower,
      NULL AS tasa_aprobacion_upper,
      'Histórico' AS tipo_dato
    FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes_agregadas`
    UNION ALL
    SELECT
      CAST(fecha_mes AS DATE) AS fecha_mes,
      total_solicitudes_pred AS total_solicitudes,
      total_solicitudes_lower,
      total_solicitudes_upper,
      solicitudes_aprobadas_pred AS solicitudes_aprobadas,
      solicitudes_aprobadas_lower,
      solicitudes_aprobadas_upper,
      tasa_aprobacion_pred AS tasa_aprobacion,
      tasa_aprobacion_lower,
      tasa_aprobacion_upper,
      'Pronóstico' AS tipo_dato
    FROM `adroit-terminus-450816-r9.solicitudes_credito.forecast_final`
    ORDER BY fecha_mes, tipo_dato;
    """
    return run_query(query, "Actualizar forecast_combinado")

def main():
    """Ejecuta todas las tareas de actualización"""
    # Paso 1: Cargar datos a la tabla solicitudes
    if not load_csv_to_bigquery():
        solution_message = "Posibles soluciones:\n" \
                          "1. Verifica tu conexión a internet\n" \
                          "2. Revisa el estado del servicio en https://status.cloud.google.com/\n" \
                          "3. Intenta con un archivo más pequeño para probar\n" \
                          "4. Verifica los permisos de la cuenta de servicio\n" \
                          "5. Considera cargar primero a Google Cloud Storage y luego a BigQuery"
        print(solution_message)
        send_discord_message(solution_message, success=False)
        return

    # Paso 2: Actualizar tabla agregada
    if not update_aggregated_table():
        return

    # Paso 3: Reentrenar modelos de pronóstico
    if not retrain_forecast_models():
        return

    # Paso 4: Actualizar pronósticos
    if not update_forecasts():
        return

    # Paso 5: Reentrenar modelo de clustering
    if not retrain_clustering_model():
        return

    # Paso 6: Actualizar tablas de clusters
    if not update_clusters():
        return

    # Paso 7: Reentrenar modelo de regresión logística
    if not retrain_logistic_model():
        return

    # Paso 8: Actualizar predicciones del modelo de regresión logística
    if not update_logistic_predictions():
        return

    # Paso 9: Actualizar tabla combinada
    if not update_combined_table():
        return

    # Notificación final
    final_message = "🎉 Proceso de actualización completado con éxito!"
    print(final_message)
    send_discord_message(final_message, success=True)

if __name__ == "__main__":
    main()