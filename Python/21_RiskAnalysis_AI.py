import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import requests
import json
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from datetime import datetime
import time
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
import logging
import traceback
import numpy as np
import os
import re
import atexit
import tempfile

# Configurar el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configuración de la aplicación Streamlit
st.set_page_config(page_title="Risk Analysis", layout="wide", page_icon="💰")

# Configuración inicial
PROJECT_ID = "adroit-terminus-450816-r9"
DATASET_ID = "solicitudes_credito"
CREDENTIALS_PATH = "C:/Users/joey_/Desktop/AIRFLOW/adroit-terminus-450816-r9-1b90cfcf6a76.json"
DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1354192765130375248/MF7bEPPlHnrzgYnJJ4iev7xTr0TrxVpqKw_MOVVIRseppELwK0hBM7VMZf8DQnVPpvh6"
MODEL_LIST = ["gemini-2.0-flash", "deepseek-chat", "deepseek-reasoner"]
GEMINI_API_KEY_PATH = "C:/Users/joey_/Desktop/AIRFLOW/API KEYS/gemini.txt"
DEEPSEEK_API_KEY_PATH = "C:/Users/joey_/Desktop/AIRFLOW/API KEYS/deepseek.txt"
DEEPSEEK_API_BASE = "https://api.deepseek.com/v1"

# Límite configurable de filas para pasar al prompt
MAX_ROWS = 1000  # Puedes ajustar este valor según tus necesidades

# Ruta del archivo CSV temporal
TEMP_CSV_PATH = os.path.join(tempfile.gettempdir(), "solicitudes_maestra_temp.csv")

# Autenticación con BigQuery
logger.info("Autenticando con BigQuery...")
try:
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    logger.info("Autenticación con BigQuery completada.")
except Exception as e:
    st.error(f"Error al autenticar con BigQuery: {e}")
    logger.error("Error al autenticar con BigQuery: %s", str(e))
    client = None

# Función para enviar mensajes a Discord
def send_discord_message(message, success=True):
    timestamp = datetime.now().isoformat()
    color = 65280 if success else 16711680
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
    
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload)
        logger.info("Mensaje enviado a Discord: %s", message)
    except Exception as e:
        st.error(f"Error al enviar mensaje a Discord: {e}")
        logger.error("Error al enviar mensaje a Discord: %s", str(e))

# Función para cargar datos de solicitudes_maestra directamente desde BigQuery
def load_solicitudes_maestra():
    if client is None:
        logger.error("No se puede cargar datos porque la autenticación con BigQuery falló.")
        return pd.DataFrame()
    
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.solicitudes_maestra`"
    logger.info("Ejecutando consulta BigQuery: %s", query)
    try:
        df = client.query(query).to_dataframe()
        logger.info("Consulta ejecutada. Filas: %d, Columnas: %s", len(df), df.columns.tolist())
        
        if df.empty:
            logger.warning("No se encontraron datos en solicitudes_maestra")
            return pd.DataFrame()
        
        df.columns = df.columns.str.lower()
        logger.info("Columnas normalizadas: %s", df.columns.tolist())
        
        if 'id_cliente' not in df.columns:
            logger.warning("Columna 'id_cliente' no encontrada. Columnas disponibles: %s", df.columns.tolist())
            send_discord_message("Advertencia: Columna 'id_cliente' no encontrada", success=False)
        else:
            logger.info("Columna 'id_cliente' encontrada.")
        
        send_discord_message("Datos de solicitudes_maestra cargados con éxito desde BigQuery", success=True)
        return df
    except Exception as e:
        logger.error("Error al cargar datos de solicitudes_maestra: %s\n%s", str(e), traceback.format_exc())
        send_discord_message(f"Error al cargar datos: {e}", success=False)
        return pd.DataFrame()

# Función para cargar datos desde el CSV temporal
def load_from_temp_csv():
    try:
        if os.path.exists(TEMP_CSV_PATH):
            df = pd.read_csv(TEMP_CSV_PATH)
            logger.info("Datos cargados desde el CSV temporal: %s", TEMP_CSV_PATH)
            return df
        else:
            logger.warning("El archivo CSV temporal no existe: %s", TEMP_CSV_PATH)
            return pd.DataFrame()
    except Exception as e:
        logger.error("Error al cargar datos desde el CSV temporal: %s", str(e))
        return pd.DataFrame()

# Función para eliminar el CSV temporal al cerrar
def cleanup_temp_csv():
    try:
        if os.path.exists(TEMP_CSV_PATH):
            os.remove(TEMP_CSV_PATH)
            logger.info("Archivo CSV temporal eliminado: %s", TEMP_CSV_PATH)
        else:
            logger.info("No se encontró el archivo CSV temporal para eliminar: %s", TEMP_CSV_PATH)
    except Exception as e:
        logger.error("Error al eliminar el archivo CSV temporal: %s", str(e))

# Registrar la función de limpieza para que se ejecute al cerrar el script
atexit.register(cleanup_temp_csv)

# Cargar datos al iniciar el asistente y generar el CSV temporal
if "datos_maestra" not in st.session_state:
    with st.spinner("Cargando datos desde BigQuery y generando CSV temporal..."):
        logger.info("Cargando datos desde BigQuery...")
        df = load_solicitudes_maestra()
        if not df.empty:
            # Guardar los datos en un CSV temporal
            try:
                df.to_csv(TEMP_CSV_PATH, index=False)
                logger.info("Archivo CSV temporal generado: %s", TEMP_CSV_PATH)
            except Exception as e:
                logger.error("Error al generar el archivo CSV temporal: %s", str(e))
                st.error(f"Error al generar el archivo CSV temporal: {e}")
        else:
            logger.warning("No se encontraron datos para generar el CSV temporal.")
            st.warning("No se encontraron datos en BigQuery para generar el CSV temporal.")
        st.session_state.datos_maestra = df
        logger.info("Datos cargados. Estado: %s", "Vacío" if st.session_state.datos_maestra.empty else "Con datos")

# Verificar si el CSV temporal está disponible
csv_available = os.path.exists(TEMP_CSV_PATH)

# No detener el script si los datos están vacíos, solo mostrar una advertencia
if st.session_state.datos_maestra.empty:
    st.warning("No hay datos disponibles para analizar. Continúa con una consulta si deseas intentar de nuevo.")
    logger.warning("No hay datos disponibles para analizar.")
else:
    # Convertir fecha_solicitud, inicio_mes e inicio_semana a datetime
    for col in ['fecha_solicitud', 'inicio_mes', 'inicio_semana']:
        if col in st.session_state.datos_maestra.columns:
            try:
                st.session_state.datos_maestra[col] = pd.to_datetime(st.session_state.datos_maestra[col])
                if col == 'fecha_solicitud':
                    min_date = st.session_state.datos_maestra['fecha_solicitud'].min()
                    max_date = st.session_state.datos_maestra['fecha_solicitud'].max()
                    logger.info("Rango de fechas disponible: desde %s hasta %s", min_date, max_date)
                    st.info(f"Rango de fechas disponible: desde {min_date} hasta {max_date}")
            except Exception as e:
                st.error(f"Error al convertir la columna '{col}' a datetime: {e}")
                logger.error("Error al convertir %s: %s", col, str(e))

    # Mostrar columnas disponibles
    logger.info("Columnas disponibles en los datos: %s", st.session_state.datos_maestra.columns.tolist())

    # Mostrar información de la tabla
    table_columns = st.session_state.datos_maestra.columns.tolist()
    st.info(f"Analizando datos desde BigQuery con las columnas: {', '.join(table_columns)}")

    # Mostrar una muestra de los datos
    st.write("Muestra de los datos cargados (primeras 5 filas):")
    st.dataframe(st.session_state.datos_maestra.head())

    # Resumen de datos
    data_summary_dict = {
        "columns": table_columns,
        "num_records": len(st.session_state.datos_maestra),
        "sample_data": st.session_state.datos_maestra.head(5).to_dict(orient="records"),
        "date_range": f"Desde {min_date.strftime('%Y-%m-%d')} hasta {max_date.strftime('%Y-%m-%d')}"
    }
    data_summary_str = (
        f"Columnas: {data_summary_dict['columns']}\n"
        f"Número de registros: {data_summary_dict['num_records']}\n"
        f"Rango de fechas: {data_summary_dict['date_range']}\n"
        f"Muestra de datos: {data_summary_dict['sample_data']}"
    )
    logger.info("data_summary (string): %s", data_summary_str)

# Definir SYSTEM_ROLE con reglas claras
SYSTEM_ROLE = """
Eres un científico de datos y analista de riesgo crediticio experto. Tu objetivo es analizar los datos de solicitudes de crédito proporcionados en un DataFrame y responder a consultas del usuario con propuestas claras, código Python ejecutable, tablas, gráficas, respuestas textuales, conclusiones relevantes y recomendaciones accionables. Usa razonamiento lógico y sigue estas reglas estrictas:

- CRÍTICO: NO SIMULES DATOS, NO LOS INVENTES Y TAMPOCO ASUMAS. DEBES USAR EXCLUSIVAMENTE LOS DATOS PROPORCIONADOS EN EL DATAFRAME (df), y 'df' está definido como "df = pd.read_csv(TEMP_CSV_PATH)". NO SE HACEN IMPUTACIONES A COLUMNAS CON NULL.
- Responde siempre en español, independientemente del idioma de la consulta del usuario.
- Antes de responder cualquier consulta, verifica las columnas disponibles en el DataFrame usando df.columns y asegúrate de que las columnas necesarias estén presentes.
- Columnas disponibles en el DataFrame: id_cliente, edad, ingresos_anuales, puntaje_crediticio, historial_pagos, deuda_actual, antiguedad_laboral, estado_civil, numero_dependientes, tipo_empleo, fecha_solicitud, solicitud_credito, inicio_mes, inicio_semana, predicted_solicitud_credito, probabilidad_aprobacion, cluster, estado.
- Reglas estrictas para identificar métricas de solicitudes:
  - Para determinar si una solicitud fue aprobada o rechazada:
    - Usa la columna 'solicitud_credito' libres de NaN.
    - Si solicitud_credito == 1, la solicitud fue aprobada.
    - Si solicitud_credito == 0, la solicitud fue rechazada.
    - Si solicitud_credito es NaN, la solicitud está pendiente y NO debe usarse para calcular métricas de aprobación/rechazo.
  - Para determinar si una solicitud fue evaluada:
    - Usa la columna 'estado'.
    - Si estado == 'Evaluada', la solicitud tiene una decisión real (solicitud_credito no es NaN).
    - Si estado == 'Pendiente (Predicha)', la solicitud no tiene decisión real (solicitud_credito es NaN o NULL, predicted_solicitud_credito no es NaN).
  - Métricas específicas:
    - **CRÍTICO:** Antes de realizar cualquier operación con columnas numéricas (como calcular mínimos, máximos, segmentaciones, tasas, etc.), elimina los valores NaN usando .dropna() o filtrando con .notna(). Ejemplo: df['columna'].dropna().min() para calcular el mínimo, o df[df['columna'].notna()] para filtrar filas.
    - Solicitudes aprobadas: Filtra donde solicitud_credito == 1, elimina NaN con df['solicitud_credito'].notna() y cuenta las filas.
    - Solicitudes rechazadas: Filtra donde solicitud_credito == 0, elimina NaN con df['solicitud_credito'].notna() y cuenta las filas.
    - Solicitudes predichas aprobadas: Filtra donde predicted_solicitud_credito == 1, elimina NaN con df['predicted_solicitud_credito'].notna() y cuenta las filas.
    - Solicitudes predichas rechazada: Filtra donde predicted_solicitud_credito == 0, elimina NaN con df['predicted_solicitud_credito'].notna() y cuenta las filas.
    - Total solicitudes evaluadas: Filtra donde estado == 'Evaluada' y cuenta las filas.
    - Total solicitudes predichas: Filtra donde estado == 'Pendiente (Predicha)' y cuenta las filas.
    - Número de clientes únicos: Conta los valores únicos en la columna id_cliente.
    - TASA DE APROBACIÓN: (Número de solicitudes aprobadas / Total de solicitudes evaluadas) * 100. Se deben eliminar los NaN en solicitud_credito antes de calcular la tasa, usando df[df['solicitud_credito'].notna()].
    - fecha_solicitud: Fecha de la solicitud (formato YYYY-MM-DD).
    - inicio_mes: Primer día del mes de la fecha_solicitud (formato YYYY-MM-DD).
    - inicio_semana: Primer día de la semana de la fecha_solicitud (formato YYYY-MM-DD).
  - Reglas adicionales:
    - Los NaN o NULL en solicitud_credito y predicted_solicitud_credito no se grafican.
    - Para columnas numéricas derivadas como tasa_aprobacion, redondea siempre a 1 decimal para consistencia.
- Reglas estrictas sobre valores nulos:
  - Las únicas columnas que pueden tener valores nulos (NaN) son: 'solicitud_credito', 'predicted_solicitud_credito' y 'probabilidad_aprobacion'.
  - Todas las demás columnas (como 'deuda_actual', 'historial_pagos', 'estado', etc.) NO deben tener valores nulos.
  - Antes de usar cualquier columna (excepto las tres mencionadas), verifica que no tenga valores nulos usando df['columna'].isna().any(). Si se encuentran valores nulos en una columna que no debería tenerlos, genera un DataFrame 'df_main' con un mensaje de error (por ejemplo, df_main = pd.DataFrame({'Error': ["Columna 'deuda_actual' contiene valores nulos, lo cual no está permitido"]})).
- Reglas estrictas para la generación de código:
  1. **Carga explícita del DataFrame desde el CSV temporal:**
     - Al inicio del código, carga los datos desde el archivo CSV temporal usando `df = pd.read_csv(TEMP_CSV_PATH)` (definida en el script principal) y asigna el resultado a `df`. 'df = pd.read_csv(TEMP_CSV_PATH)' debe estar definida en el script principal, que contiene la ruta al archivo CSV temporal (`solicitudes_maestra_temp.csv`) en el directorio temporal del sistema.
     - **CRÍTICO:** NO redefinas la variable `TEMP_CSV_PATH` en el código generado. `TEMP_CSV_PATH` ya está definida en el script principal y apunta a la ruta correcta: `solicitudes_maestra_temp.csv` en el directorio temporal del sistema.
     - **CRÍTICO:** NO redefinas la función `df = pd.read_csv(TEMP_CSV_PATH)` en el código generado. Usa la función tal como está definida en el script principal, que no acepta argumentos y usa `TEMP_CSV_PATH` internamente.
     - Define una lista `required_columns` con las columnas necesarias para la consulta.
     - Después de cargar `df`, aplica un `dropna()` al subconjunto de columnas requeridas usando `df = df.dropna(subset=required_columns)`. Esto elimina filas donde cualquiera de las columnas requeridas tenga valores NaN.
     - Si el DataFrame está vacío o no se puede cargar, genera un df_main con un mensaje de error.
  2. Antes de realizar cualquier operación con una columna (como calcular mínimos, máximos, o segmentaciones), verifica que:
     - La columna existe en el DataFrame usando 'columna in df.columns'.
     - Si la columna NO es 'solicitud_credito', 'predicted_solicitud_credito' ni 'probabilidad_aprobacion', verifica que no tenga valores nulos usando df['columna'].isna().any(). Si hay valores nulos, genera un DataFrame 'df_main' con un mensaje de error.
     - Si la columna es numérica, asegúrate de que los valores sean numéricos usando pd.to_numeric(df['columna'], errors='coerce') y verifica que no haya valores no numéricos que causen problemas.
  3. Si una columna no existe, tiene valores nulos (cuando no debería), o no cumple con los requisitos (por ejemplo, no es numérica para operaciones que lo requieren), genera un DataFrame 'df_main' con una columna 'Error' que describa el problema (por ejemplo, df_main = pd.DataFrame({'Error': ['Columna "deuda_actual" no encontrada']})).
  4. Si no puedes proceder con el análisis debido a datos insuficientes, genera una gráfica vacía con un título que indique el problema (por ejemplo, fig = px.bar(title='No hay datos suficientes para generar el gráfico')).
  5. CRÍTICO: Siempre genera un DataFrame principal llamado 'df_main'. Si el análisis falla por cualquier motivo (falta de datos, errores en los cálculos, etc.), asigna a 'df_main' un DataFrame con un mensaje de error.
  6. No uses funciones de Streamlit (como st.write(), st.plotly_chart(), etc.) dentro del código generado. Las gráficas deben asignarse a una variable 'fig' y los DataFrames a 'df_main' para que Streamlit los renderice fuera del código generado.
  7. No llames a funciones externas (como calculate_approve_rate_heatmap) a menos que estén definidas explícitamente dentro del código generado, excepto la función `df = pd.read_csv(TEMP_CSV_PATH)`, que está definida en el script principal.
- Flujo de trabajo para consultas de generación de código (Etapa 1):
  1. Razona la solicitud paso a paso, identificando lo que se solicita, las métricas requeridas, las columnas necesarias y las condiciones de filtrado.
  2. Verifica la disponibilidad de las columnas necesarias y los datos en el DataFrame usando df.columns y las reglas de validación mencionadas.
  3. Si no puedes ejecutar la consulta (por falta de columnas, datos insuficientes, valores nulos no permitidos, o condiciones no cumplidas), indícalo claramente, explica por qué, y genera un DataFrame 'df_main' con un mensaje de error.
  4. Si puedes ejecutar la consulta, propone una solución clara para resolverla, explicando los pasos a seguir.
  5. Genera código Python que implemente la solución usando pandas para data wrangling y plotly.express para visualización. Asegúrate de que las gráficas sean compatibles con Streamlit (usa variables como 'fig' y no llames a .show() ni uses print() para mostrar resultados; los resultados deben asignarse a variables para que Streamlit los renderice con st.write() o st.dataframe()).
  6. Genera SOLO UN DataFrame principal (el más importante para el análisis) que contenga los resultados clave de las operaciones (como agregaciones, transformaciones, o datos usados para gráficas). If the query asks for a DataFrame or values, generate that DataFrame as the main one. If the query asks for a plot (like a heatmap), generate the DataFrame that feeds the plot as the main one. Nombra este DataFrame 'df_main'.
  7. Si no se puede calcular la métrica solicitada, sugiere una métrica alternativa que sí se pueda calcular con los datos disponibles y genera el código correspondiente, incluyendo un DataFrame principal.
  8. Proporciona el código ejecutable bajo una sección titulada "Implementación (código Python):" en tu respuesta, asegurándote de que esté correctamente indentado y sea sintácticamente correcto.
  9. CRÍTICO: Asegúrate de que 'df_main' siempre esté definido al final del código, incluso si hay errores en el flujo. Si el análisis falla, asigna a 'df_main' un DataFrame con un mensaje de error.
"""
logger.info("SYSTEM_ROLE actualizado para que el dataframe sea df = pd.read_csv(TEMP_CSV_PATH)")

# Definir USER_ROLE_TEMPLATE para la primera etapa (generación de código)
USER_ROLE_TEMPLATE_CODE = """
Analiza los datos de solicitudes de crédito y responde a la siguiente consulta: {prompt}. 

Los datos están disponibles en un archivo CSV temporal cargando los datos desde este archivo usando 'df = pd.read_csv(TEMP_CSV_PATH)', (sin pasar argumentos, ya que la función usa `TEMP_CSV_PATH` internamente) y asigna el resultado a una variable `df`. **CRÍTICO:** NO redefinas `TEMP_CSV_PATH` en el código generado; ya está definida en el script principal. Después de cargar `df`, aplica un `dropna()` al subconjunto de columnas requeridas (`required_columns`) usando `df = df.dropna(subset=required_columns)` para eliminar filas donde cualquiera de las columnas requeridas tenga valores NaN. Realiza el análisis siguiendo el flujo de trabajo definido en el SYSTEM_ROLE para consultas de generación de código (Etapa 1), utilizando pandas para data wrangling y plotly.express para visualización.

Datos preprocesados (si aplica):
{preprocessed_data}

Devuelve un análisis que incluya:
CRÍTICO: USA RAZONAMIENTO LÓGICO PASO A PASO, NO INVENTES DATOS, NO SIMULES, NO ASUMAS. SOLO DEBES USAR LOS DATOS DISPONIBLES EN EL DATAFRAME (df).
1. Razona la solicitud paso a paso, identificando lo que se solicita, las métricas requeridas, las columnas necesarias y las condiciones de filtrado. Sigue estrictamente las reglas del SYSTEM_ROLE para identificar métricas como aprobaciones, rechazos y solicitudes evaluadas.
2. Define una lista `required_columns` con las columnas necesarias para la consulta y aplica el `dropna()` al subconjunto de esas columnas después de cargar `df`.
3. Verifica la disponibilidad de las columnas necesarias en el DataFrame 'df' usando df.columns y asegúrate de que las columnas estén presentes antes de proceder. Sigue las reglas de validación de datos del SYSTEM_ROLE, incluyendo la verificación de valores nulos en columnas que no deberían tenerlos.
4. Si no puedes ejecutar la consulta, indícalo claramente, explica por qué y sugiere una métrica alternativa que sí se pueda calcular con los datos disponibles.
5. If puedes ejecutar la consulta (o una alternativa), propone una solución clara, genera y muestra el código Python que implemente la solución usando el DataFrame 'df'. Asegúrate de que el código sea compatible with Streamlit: no uses fig.show(), print(), ni funciones de Streamlit como st.plotly_chart() para mostrar resultados; asigna las gráficas a variables como 'fig' y los DataFrames a 'df_main' para que Streamlit los renderice con st.plotly_chart() o st.dataframe().
6. CRÍTICO: Genera SOLO UN DataFrame principal (el más importante para el análisis) que contenga los resultados clave de las operaciones. Si la consulta pide un DataFrame o valores, genera ese DataFrame como el principal (nómbralo 'df_main'). Si la consulta pide una gráfica (como un heatmap), genera el DataFrame que alimenta la gráfica como el principal (nómbralo 'df_main'). Este DataFrame será analizado en la Etapa 2.
7. TODOS LOS DATAFRAMES PRINCIPALES SE LLAMAN 'df_main' y los gráficos con 'plot_' o 'fig'. Si generas una gráfica, asegúrate de que 'df_main' contenga los datos exactos usados para esa gráfica y que la gráfica se asigne a 'fig'.
8. CRÍTICO: Asegúrate de que 'df_main' siempre esté definido al final del código, incluso si hay errores en el flujo. Si el análisis falla, asigna a 'df_main' un DataFrame con un mensaje de error.
9. Proporciona el código ejecutable bajo una sección titulada "Implementación (código Python):" en tu respuesta, asegurándote de que esté correctamente indentado y sea sintácticamente correcto.
"""
logger.info("USER_ROLE_TEMPLATE_CODE actualizado para evitar redefinición de TEMP_CSV_PATH en el código generado.")

# Definir USER_ROLE_TEMPLATE para la segunda etapa (análisis de DataFrames)
USER_ROLE_TEMPLATE_ANALYSIS = """
Analiza el DataFrame principal resultante generado a partir de la siguiente consulta inicial: {initial_prompt}.

A continuación, se proporciona un resumen estadístico del DataFrame principal 'df_main', junto con una muestra de las primeras filas (si aplica):

**Resumen estadístico del DataFrame 'df_main':**
{df_summary}

**Muestra de las primeras filas del DataFrame 'df_main' (hasta {max_rows} filas, solo si el DataFrame tiene 1000 filas o menos):**
{df_sample}

Realiza el análisis siguiendo el flujo de trabajo definido en el SYSTEM_ROLE para consultas de análisis de DataFrames (Etapa 2). Proporciona un análisis detallado, conclusiones y recomendaciones basadas exclusivamente en los datos proporcionados en el prompt, en el contexto de la consulta inicial.

IMPORTANTE: NO EJECUTES CÓDIGO PARA CARGAR DATOS. Usa los valores exactamente como se proporcionan en el resumen estadístico y la muestra del prompt. NO SIMULES DATOS, NO LOS INVENTES Y TAMPOCO ASUMAS. DEBES USAR EXCLUSIVAMENTE LOS DATOS PROPORCIONADOS EN EL PROMPT.
CRÍTICO: DEBES MANTENER EL MISMO PERÍODO DE EVALUACIÓN DE LA CONSULTA INICIAL.

- Si el DataFrame tiene más de 1000 filas, realiza un análisis basado únicamente en el resumen estadístico (estadísticas descriptivas, conteos de valores categóricos y correlaciones), sin considerar la muestra de filas.
- Si el DataFrame tiene 1000 filas o menos, realiza un análisis completo que incluya tanto el resumen estadístico como la muestra de filas, explorando tendencias, patrones y relaciones.

Devuelve: 'Periodo de Evaluación = Fecha inicial y fecha final de la consulta' tomando en cuenta hasta donde hay datos disponibles.
0. Usa los datos del resumen estadístico y la muestra proporcionada (si aplica), sin recalcular métricas. Genera una tabla de estadísticas descriptivas de los datos sin tomar en cuenta 'id_cliente' y usando el periodo de evaluación.
1. Un análisis textual detallado de los datos con análisis numérico, explorando columnas, valores, tendencias, máximos, mínimos, promedios, correlaciones, distribuciones, y patrones relevantes.
2. Conclusiones relevantes basadas en el análisis haciendo mención a valores numéricos clave.
3. Recomendaciones accionables basadas en los datos y el contexto de la consulta, mencionando umbrales de valores numéricos clave y recomendaciones relevantes.
"""
logger.info("USER_ROLE_TEMPLATE_ANALYSIS actualizado para manejar DataFrames grandes y pequeños.")

# Funciones auxiliares
def check_model_availability(model_name, api_key):
    try:
        if "gemini" in model_name:
            llm = ChatGoogleGenerativeAI(model=model_name, google_api_key=api_key, temperature=0.3)
            llm.invoke("Ping")
        elif "deepseek" in model_name:
            llm = ChatOpenAI(model=model_name, api_key=api_key, base_url=DEEPSEEK_API_BASE, temperature=0.3)
        logger.info("Modelo %s disponible.", model_name)
        return "🟢"
    except Exception as e:
        logger.error("Modelo %s no disponible: %s", model_name, str(e))
        return "🔴"

# Función para generar un resumen estadístico del DataFrame
def generate_dataframe_summary(df):
    if df.empty:
        return "El DataFrame está vacío."
    
    summary = []
    summary.append(f"Número total de filas: {len(df)}")
    summary.append(f"Columnas: {', '.join(df.columns.tolist())}")
    
    # Estadísticas descriptivas para columnas numéricas
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        desc = df[numeric_cols].describe().round(2).to_markdown(index=True)
        summary.append("\n**Estadísticas descriptivas (columnas numéricas):**\n")
        summary.append(desc)
    
    # Conteos para columnas categóricas
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    for col in categorical_cols:
        value_counts = df[col].value_counts().head(5).to_markdown(index=True)
        summary.append(f"\n**Conteo de valores para la columna '{col}' (top 5):**\n")
        summary.append(value_counts)
    
    # Correlaciones entre columnas numéricas (si aplica)
    if len(numeric_cols) > 1:
        corr = df[numeric_cols].corr().round(2).to_markdown(index=True)
        summary.append("\n**Matriz de correlaciones (columnas numéricas):**\n")
        summary.append(corr)
    
    return "\n".join(summary)

# Función para convertir un DataFrame a formato Markdown con límite de filas
def dataframe_to_markdown(df, max_rows=MAX_ROWS):
    if df.empty:
        return "El DataFrame está vacío."
    
    # Limitar el número de filas según max_rows
    if len(df) > max_rows:
        df = df.head(max_rows)
        note = f"\n**Nota:** Se muestran solo las primeras {max_rows} filas de un total de {len(df)}."
    else:
        note = ""
    
    try:
        # Intentar convertir el DataFrame a Markdown usando tabulate
        markdown = df.to_markdown(index=False)
        return f"{markdown}{note}"
    except ImportError as e:
        # Si tabulate no está instalado, usar una representación alternativa
        logger.warning("No se encontró 'tabulate'. Usando representación alternativa para el DataFrame.")
        lines = ["| " + " | ".join(df.columns) + " |"]
        lines.append("| " + " | ".join(["---"] * len(df.columns)) + " |")
        for _, row in df.iterrows():
            lines.append("| " + " | ".join(str(val) for val in row) + " |")
        alternative = "\n".join(lines)
        return f"{alternative}{note}\n**Advertencia:** No se encontró 'tabulate'. Se recomienda instalarlo con 'pip install tabulate' para una mejor representación."

# Función para limpiar el código extraído (corrección básica de indentación y líneas vacías)
def clean_code(code_str):
    # Dividir el código en líneas
    lines = code_str.splitlines()
    # Eliminar líneas vacías y espacios innecesarios
    lines = [line.rstrip() for line in lines if line.strip()]
    
    # Detectar el nivel de indentación base (asumiendo que la primera línea no está indentada)
    if not lines:
        return ""
    
    # Corregir indentación básica
    cleaned_lines = []
    for line in lines:
        # Contar los espacios al inicio de la línea
        leading_spaces = len(line) - len(line.lstrip())
        # Asegurar que la indentación sea múltiplo de 4 (estándar en Python)
        corrected_spaces = (leading_spaces // 4) * 4
        # Reemplazar la indentación original con la corregida
        cleaned_line = " " * corrected_spaces + line.lstrip()
        cleaned_lines.append(cleaned_line)
    
    return "\n".join(cleaned_lines)

# Función para eliminar bloques de código de una respuesta para evitar duplicados en la visualización
def remove_code_blocks(response):
    # Usar una expresión regular para encontrar y eliminar bloques de código (```python ... ```)
    return re.sub(r'```python\n.*?\n```', '[Código Python removido para evitar duplicación]', response, flags=re.DOTALL)

# Función para ejecutar código Python generado y capturar resultados
def execute_generated_code(code_str):
    try:
        local_vars = {'pd': pd, 'px': px, 'go': go, 'np': np, 'load_from_temp_csv': load_from_temp_csv, 'TEMP_CSV_PATH': TEMP_CSV_PATH}
        exec(code_str, globals(), local_vars)
        dataframes = {k: v for k, v in local_vars.items() if isinstance(v, pd.DataFrame) and k != 'df'}
        plots = {k: v for k, v in local_vars.items() if isinstance(v, (go.Figure, dict)) and (k.startswith('plot_') or k == 'fig')}
        variables = {k: v for k, v in local_vars.items() if not isinstance(v, (pd.DataFrame, go.Figure, dict)) and k not in ['pd', 'px', 'go', 'df', 'np', 'load_from_temp_csv', 'TEMP_CSV_PATH']}
        
        # Asegurarse de que df_main siempre esté presente
        if 'df_main' not in dataframes:
            if dataframes:
                # Si no hay df_main pero hay otros DataFrames, usar el último creado
                last_df_name = list(dataframes.keys())[-1]
                dataframes['df_main'] = dataframes[last_df_name]
                logger.info(f"No se encontró 'df_main'. Usando el último DataFrame creado: {last_df_name}")
            else:
                # Si no hay ningún DataFrame, crear uno con un mensaje de error
                dataframes['df_main'] = pd.DataFrame({'Error': ['No se generó ningún DataFrame en el código']})
                if 'fig' not in plots:
                    plots['fig'] = px.bar(title="No se generó ningún DataFrame ni gráfico")
                logger.warning("No se generó ningún DataFrame en el código. Creando df_main con mensaje de error.")
        
        return {
            'dataframes': dataframes,
            'plots': plots,
            'variables': variables
        }
    except NameError as ne:
        logger.error(f"Error de definición (NameError) al ejecutar el código generado: {str(ne)}")
        error_df = pd.DataFrame({'Error': [f"Error de definición (NameError): {str(ne)}"]})
        error_fig = px.bar(title="Error de definición en el código")
        return {
            'dataframes': {'df_main': error_df},
            'plots': {'fig': error_fig},
            'variables': {},
            'error': str(ne)
        }
    except Exception as e:
        logger.error(f"Error al ejecutar el código generado: {str(e)}")
        error_df = pd.DataFrame({'Error': [f"Error en la ejecución del código: {str(e)}"]})
        error_fig = px.bar(title="Error en la ejecución del código")
        return {
            'dataframes': {'df_main': error_df},
            'plots': {'fig': error_fig},
            'variables': {},
            'error': str(e)
        }

# Función para invocar el modelo de IA y obtener una respuesta
def invoke_model(prompt_template, variables):
    if llm is None:
        return "Error: No se ha configurado un modelo de IA válido. Por favor, verifica la disponibilidad del modelo y las claves API."
    prompt = prompt_template.format(**variables)
    try:
        response = llm.invoke(prompt)
        return response.content
    except Exception as e:
        logger.error(f"Error al invocar el modelo: {str(e)}")
        return f"Error al invocar el modelo: {str(e)}"

# Inicializar claves API y disponibilidad de modelos
if "api_keys" not in st.session_state:
    st.session_state.api_keys = {}
    for model, path in [("gemini-2.0-flash", GEMINI_API_KEY_PATH), ("deepseek-chat", DEEPSEEK_API_KEY_PATH), 
                        ("deepseek-reasoner", DEEPSEEK_API_KEY_PATH)]:
        try:
            with open(path, "r") as file:
                st.session_state.api_keys[model] = file.read().strip()
        except Exception as e:
            st.session_state.api_keys[model] = None
            st.sidebar.error(f"Error al leer la clave API para {model}: {e}")

if "model_status" not in st.session_state:
    st.session_state.model_status = {model: check_model_availability(model, st.session_state.api_keys.get(model, "")) 
                                     if st.session_state.api_keys.get(model) else "🔴" for model in MODEL_LIST}

# Barra lateral
st.sidebar.title("👨‍💻📊 Análisis de Riesgo Crediticio")
st.sidebar.markdown("Consulta los datos de solicitudes de crédito con un enfoque en riesgo crediticio.")

# Indicador de datos cargados
if "datos_maestra" in st.session_state and not st.session_state.datos_maestra.empty:
    st.sidebar.success("Datos cargados directamente desde BigQuery.")
else:
    st.sidebar.error("Datos no disponibles.")

# Indicador de disponibilidad del CSV temporal
if csv_available:
    st.sidebar.success("Archivo CSV temporal disponible: solicitudes_maestra_temp.csv")
else:
    st.sidebar.error("Archivo CSV temporal no disponible: solicitudes_maestra_temp.csv")

# Botón para limpiar caché
if "clear_cache_counter" not in st.session_state:
    st.session_state.clear_cache_counter = 0

clear_cache_key = f"clear_cache_button_{st.session_state.clear_cache_counter}"
if st.sidebar.button("Limpiar caché", key=clear_cache_key):
    st.session_state.clear_cache_counter += 1
    st.cache_data.clear()
    for key in list(st.session_state.keys()):
        if key not in ['api_keys', 'model_status', 'clear_cache_counter']:
            del st.session_state[key]
    st.session_state.datos_maestra = load_solicitudes_maestra()
    if not st.session_state.datos_maestra.empty:
        try:
            st.session_state.datos_maestra.to_csv(TEMP_CSV_PATH, index=False)
            logger.info("Archivo CSV temporal actualizado después de limpiar caché: %s", TEMP_CSV_PATH)
        except Exception as e:
            logger.error("Error al actualizar el archivo CSV temporal después de limpiar caché: %s", str(e))
    st.rerun()

# Mostrar disponibilidad de modelos
st.sidebar.subheader("Disponibilidad de Modelos")
for model, status in st.session_state.model_status.items():
    st.sidebar.write(f"{model}: {status}")

# Selección del modelo
if "model_select_counter" not in st.session_state:
    st.session_state.model_select_counter = 0

model_select_key = f"model_select_{st.session_state.model_select_counter}"
model_option = st.sidebar.selectbox("Elige el modelo", MODEL_LIST, index=0, key=model_select_key)

# Configurar el modelo seleccionado
llm = None
if st.session_state.model_status[model_option] == "🟢":
    try:
        if "gemini" in model_option:
            llm = ChatGoogleGenerativeAI(model=model_option, google_api_key=st.session_state.api_keys[model_option], 
                                         temperature=0.3)
        elif "deepseek" in model_option:
            llm = ChatOpenAI(model=model_option, api_key=st.session_state.api_keys[model_option], 
                             base_url=DEEPSEEK_API_BASE, temperature=0.3)
        st.sidebar.success(f"¡Conexión con {model_option} establecida!")
    except Exception as e:
        st.sidebar.error(f"Error al conectar con {model_option}: {e}")
else:
    st.sidebar.error(f"El modelo {model_option} no está disponible. Continúa con una consulta si deseas intentar de nuevo.")

# Mostrar ejemplos en el sidebar justo debajo del aviso del modelo seleccionado
st.sidebar.markdown("### 📝 Ejemplos de consultas")
example_queries = {
    "📈 Tendencia de aprobaciones": "Necesito una gráfica de línea con la tendencia agrupados mes a mes para los últimos 12 meses, en donde pueda ver la tasa de aprobaciones de los casos evaluados (Porcentaje con 1 decimal).",
    "📊 Análisis por segmento": "Muestra un análisis de la tasa de aprobación por segmento de ingresos_anuales, agrupando en rangos de 10000 y ordenados de mayor a menor tasa.",
    "🔄 Correlación de variables": "Genera una matriz de correlación entre las variables numéricas: ingresos_anuales, puntaje_crediticio, deuda_actual y antiguedad_laboral.",
    "⚖️ Perfil de riesgo": "Analiza el perfil de riesgo de los clientes mostrando la distribución del puntaje_crediticio y la tasa de aprobación por cada rango de 50 puntos.",
    "🧮 Heatmap": "Crea un heatmap que visualice la tasa de aprobación (porcentaje o ratio) en función de dos variables: Eje X: Deuda actual en dólares (USD), dividida en 6 segmentos equidistantes entre el valor mínimo y máximo de la deuda. Categoriza los segmentos con rangos de valores (min-max). Eje Y: Ingresos anuales en dólares (USD), divididos en 6 segmentos equidistantes entre el valor mínimo y máximo de los ingresos. Categoriza los segmentos con rangos de valores (min-max). El heatmap debe usar la paleta RdBu con etiquetas de datos visibles en porcentaje de un decimal. Ordena los segmentos del menor al mayor."
}

with st.sidebar.expander("🔍 Ver ejemplos de consultas", expanded=False):
    st.info("Haga clic en cualquier ejemplo para usarlo como consulta:")
    for label, query in example_queries.items():
        if st.button(label, key=f"sidebar_example_{label}"):
            if "chat_input_counter" not in st.session_state:
                st.session_state.chat_input_counter = 0
            st.session_state.chat_input_counter += 1
            st.session_state.selected_query = query
            st.rerun()

# Inicializar historial de mensajes
msgs = StreamlitChatMessageHistory(key="langchain_messages")
if len(msgs.messages) == 0:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    initial_message = f"[{timestamp}] ¿Cómo puedo ayudarte con el análisis de riesgo crediticio? / How can I assist you with credit risk analysis?"
    msgs.add_ai_message(initial_message)

if "plots" not in st.session_state:
    st.session_state.plots = []
if "dataframes" not in st.session_state:
    st.session_state.dataframes = []

# Función para mostrar el historial de chat
def display_chat_history():
    for msg in msgs.messages:
        avatar = "👨‍💻" if msg.type == "human" else "🤖"
        with st.chat_message(msg.type, avatar=avatar):
            message_content = msg.content
            if message_content.startswith("["):
                timestamp_end = message_content.find("]")
                if timestamp_end != -1:
                    timestamp = message_content[1:timestamp_end]
                    content = message_content[timestamp_end + 2:]
                    st.markdown(f"**{timestamp}** {content}")
            else:
                st.write(message_content)
            if "PLOT_INDEX:" in message_content:
                plot_index = int(message_content.split("PLOT_INDEX:")[1])
                st.plotly_chart(st.session_state.plots[plot_index], key=f"history_plot_{plot_index}")
            elif "DATAFRAME_INDEX:" in message_content:
                df_index = int(message_content.split("DATAFRAME_INDEX:")[1])
                st.dataframe(st.session_state.dataframes[df_index], key=f"history_dataframe_{df_index}")

# Interfaz principal
st.title("Análisis de Riesgo Crediticio")
st.markdown("Consulta los datos de solicitudes de crédito con un enfoque en riesgo crediticio.")

display_chat_history()

# Entrada de consulta
st.write("### Ingrese su consulta:")
if "chat_input_counter" not in st.session_state:
    st.session_state.chat_input_counter = 0

chat_input_key = f"chat_input_{st.session_state.chat_input_counter}"

# Si hay una consulta seleccionada, mostrarla como texto antes del chat input
if "selected_query" in st.session_state:
    st.info("📝 Ejemplo seleccionado (puede copiarlo y modificarlo):")
    st.code(st.session_state.selected_query)
    # Limpiar la consulta seleccionada después de mostrarla
    st.session_state.pop("selected_query", None)

prompt = st.chat_input(
    placeholder="Ej: Genera un heatmap de la tasa de aprobación evaluada...", 
    key=chat_input_key
)

# Procesar la consulta
if prompt:
    st.session_state.chat_input_counter += 1  # Incrementar el contador para forzar un nuevo key en el próximo renderizado
    logger.info(f"Prompt recibido: {prompt}")
    
    with st.spinner(f"Procesando tu consulta..."):
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            user_message = f"[{timestamp}] {prompt}"
            st.chat_message("human", avatar="👨‍💻").write(f"**{timestamp}** - {prompt}")
            msgs.add_user_message(user_message)
            logger.info("Consulta del usuario recibida: %s", prompt)

            # Etapa 1: Razonar y generar código
            st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - Razonamiento de la solicitud (Etapa 1):")
            variables = {
                "prompt": prompt,
                "preprocessed_data": data_summary_str
            }
            reasoning_response = invoke_model(USER_ROLE_TEMPLATE_CODE, variables)
            
            # Eliminar el bloque de código del reasoning_response para evitar duplicación
            cleaned_reasoning_response = remove_code_blocks(reasoning_response)
            st.write(cleaned_reasoning_response)

            # Extraer el código de la sección "Implementación (código Python):"
            implementation_section = "Implementación (código Python):"
            code_start_marker = "```python"
            code_end_marker = "```"
            
            # Buscar la sección de implementación
            implementation_start = reasoning_response.find(implementation_section)
            if implementation_start == -1:
                st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - No se encontró la sección 'Implementación (código Python):' en la respuesta del modelo.")
                msgs.add_ai_message(f"[{timestamp}] No se encontró la sección 'Implementación (código Python):' en la respuesta del modelo.")
                st.stop()

            # Buscar el bloque de código dentro de la sección de implementación
            code_start = reasoning_response.find(code_start_marker, implementation_start)
            code_end = reasoning_response.find(code_end_marker, code_start + len(code_start_marker))
            if code_start == -1 or code_end == -1:
                st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - No se pudo encontrar un bloque de código Python válido bajo la sección 'Implementación (código Python):'.")
                msgs.add_ai_message(f"[{timestamp}] No se pudo encontrar un bloque de código Python válido bajo la sección 'Implementación (código Python):'.")
                st.stop()

            # Extraer y limpiar el código
            code_str = reasoning_response[code_start + len(code_start_marker):code_end].strip()
            code_str = clean_code(code_str)
            
            st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - Código Python de implementación:")
            st.code(code_str, language="python")

            # Ejecutar código
            st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - Ejecutando el código de implementación...")
            result = execute_generated_code(code_str)

            # Almacenar el DataFrame principal
            df_main = result['dataframes']['df_main']  # Ahora siempre estará presente gracias a execute_generated_code
            
            # Asegurar que columnas numéricas como tasa_aprobacion estén redondeadas a 1 decimal
            if 'tasa_aprobacion' in df_main.columns:
                df_main['tasa_aprobacion'] = df_main['tasa_aprobacion'].round(1)
            
            st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - Tabla principal generada:")
            st.write("Tabla: df_main")
            st.dataframe(df_main)
            # Agregar opción para descargar el DataFrame como CSV
            csv = df_main.to_csv(index=False)
            st.download_button(
                label="Descargar df_main como CSV",
                data=csv,
                file_name="df_main.csv",
                mime="text/csv"
            )
            df_index = len(st.session_state.dataframes)
            st.session_state.dataframes.append(df_main)
            logger.info(f"DataFrame principal generado: df_main, filas: {len(df_main)}")
            logger.info(f"DataFrame generado en Etapa 1:\n{df_main.to_string()}")
            msgs.add_ai_message(f"[{timestamp}] DATAFRAME_INDEX:{df_index}")

            if result['plots']:
                st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - Gráficos generados:")
                for name, plot in result['plots'].items():
                    if isinstance(plot, dict):
                        plot = pio.from_json(json.dumps(plot))
                    st.plotly_chart(plot)
                    plot_index = len(st.session_state.plots)
                    st.session_state.plots.append(plot)
                    logger.info(f"Gráfico generado: {name}")
                    msgs.add_ai_message(f"[{timestamp}] PLOT_INDEX:{plot_index}")

            # Generar un resumen estadístico del DataFrame
            df_summary = generate_dataframe_summary(df_main)
            
            # Determinar si incluir la muestra de filas (solo si el DataFrame tiene 1000 filas o menos)
            if len(df_main) <= 1000:
                df_sample = dataframe_to_markdown(df_main, max_rows=MAX_ROWS)
            else:
                df_sample = "No se incluye muestra de filas porque el DataFrame tiene más de 1000 filas."

            # Etapa 2: Análisis del DataFrame principal pasado como texto en el prompt
            st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - Análisis del DataFrame principal (Etapa 2):")
            analysis_prompt = {
                "initial_prompt": prompt,
                "df_summary": df_summary,
                "df_sample": df_sample,
                "max_rows": MAX_ROWS
            }
            analysis_response = invoke_model(USER_ROLE_TEMPLATE_ANALYSIS, analysis_prompt)
            st.write(analysis_response)
            msgs.add_ai_message(f"[{timestamp}] {analysis_response}")
            logger.info("Etapa 2 analysis completed:\n%s", analysis_response)

            # Agregar mensaje de finalización y disponibilidad del chat
            st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - ✅ **Consulta finalizada.** El chat está nuevamente disponible. Por favor, ingrese su próxima consulta.")
            msgs.add_ai_message(f"[{timestamp}] ✅ **Consulta finalizada.** El chat está nuevamente disponible. Por favor, ingrese su próxima consulta.")
            
            # Forzar un rerun de la página para reiniciar el chat input
            st.rerun()

        except Exception as e:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_message = f"[{timestamp}] Error al procesar la consulta: {str(e)}"
            st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - Error: {str(e)}")
            msgs.add_ai_message(error_message)
            logger.error(f"Error al procesar la consulta: {str(e)}")

            # Agregar mensaje de finalización incluso en caso de error
            st.chat_message("ai", avatar="🤖").write(f"**{timestamp}** - ✅ **Consulta finalizada (con error).** El chat está nuevamente disponible. Por favor, ingrese su próxima consulta.")
            msgs.add_ai_message(f"[{timestamp}] ✅ **Consulta finalizada (con error).** El chat está nuevamente disponible. Por favor, ingrese su próxima consulta.")

df = pd.read_csv(TEMP_CSV_PATH)
logger.info(f"CSV TEMPORAL: {TEMP_CSV_PATH}, Registros Cargados:  {len(df)}")