-- CREACIÓN DE REGLA DE REDES

CREATE OR REPLACE NETWORK RULE discord_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('discord.com:443')
  COMMENT = 'Allow access to Discord webhook';

-- PERMISOS PARA ACCESOS DE INTEGRACIÓN EXTERNA
  
  CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION discord_access_integration
  ALLOWED_NETWORK_RULES = (discord_network_rule)
  ENABLED = TRUE
  COMMENT = 'Integration for Discord webhook access';

-- PERMISOS DE INTEGRACION A DISCORD
  
GRANT USAGE ON INTEGRATION discord_access_integration TO ROLE <'MI_ROL'>;


-- FUNCIÓN PARA MENSAJE A DISCORD.

USE DATABASE <your_database>;
USE SCHEMA <your_schema>;

CREATE OR REPLACE FUNCTION send_discord_webhook(message STRING, success BOOLEAN)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('requests')
HANDLER = 'send_webhook'
EXTERNAL_ACCESS_INTEGRATIONS = (discord_access_integration)
AS $$
import requests
import json

def send_webhook(message, success):
    try:
        webhook_url = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"  # Cambia esto por tu webhook real
        timestamp = __import__('datetime').datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = 0x00FF00 if success else 0xFF0000
        emoji = "✅" if success else "❌"
        payload = {
            "embeds": [{
                "title": f"{emoji} Exportación Prophet a Snowflake",
                "description": message,
                "color": color,
                "fields": [
                    {"name": "Timestamp", "value": timestamp, "inline": True}
                ],
                "footer": {"text": "Sistema de Monitoreo ML"}
            }]
        }
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code == 204:
            return "Mensaje enviado a Discord."
        else:
            return f"Error al enviar mensaje a Discord: {response.status_code} - {response.text}"
    except Exception as e:
        return f"Error al enviar mensaje a Discord: {str(e)}"
$$;

-- MENSAJE A DISCORD DE OPERACIÓN DE CLUSTERING

USE DATABASE XXXXXXXXXXXXXX;
USE SCHEMA XXXXXXXX;

-- Esta función recibe todos los datos necesarios como argumentos para armar el mensaje a Discord
CREATE OR REPLACE FUNCTION send_discord_webhook_clustering(
    clusters_seleccionados INTEGER,
    silhouette_score FLOAT,
    davies_bouldin_score FLOAT,
    calidad_clusterizacion STRING,
    pc1_vars STRING,
    pc2_vars STRING,
    enviados_elbows INTEGER,
    enviados_clusters INTEGER,
    count_elbows_total INTEGER,
    count_clusters_total INTEGER,
    success BOOLEAN
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('requests')
HANDLER = 'send_webhook'
EXTERNAL_ACCESS_INTEGRATIONS = (discord_access_integration)
AS $$
import requests
import datetime

def send_webhook(
    clusters_seleccionados,
    silhouette_score,
    davies_bouldin_score,
    calidad_clusterizacion,
    pc1_vars,
    pc2_vars,
    enviados_elbows,
    enviados_clusters,
    count_elbows_total,
    count_clusters_total,
    success
):
    DISCORD_WEBHOOK_URL = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    color = 0x00FF00 if success else 0xFF0000
    emoji = "✅" if success else "❌"
    mensaje = (
        f"**ML Clustering a Snowflake**\n"
        f"- Clusters seleccionados: {clusters_seleccionados}\n"
        f"- Silhouette Score: {silhouette_score:.3f}\n"
        f"- Davies-Bouldin Score: {davies_bouldin_score:.3f}\n"
        f"- Calidad de clusterización: {calidad_clusterizacion}\n\n"
        f"Variables más influyentes en PC1: {pc1_vars}\n"
        f"Variables más influyentes en PC2: {pc2_vars}\n\n"
        f"Registros cargados en esta corrida:\n"
        f"- CLUSTERING_ELBOWS_METRICS: {enviados_elbows}\n"
        f"- CLIENTES_CLUSTERS: {enviados_clusters}\n\n"
        f"Conteo total en tablas Snowflake:\n"
        f"- CLUSTERING_ELBOWS_METRICS: {count_elbows_total}\n"
        f"- CLIENTES_CLUSTERS: {count_clusters_total}"
    )
    payload = {
        "embeds": [{
            "title": f"{emoji} ML Clustering a Snowflake",
            "description": mensaje,
            "color": color,
            "fields": [
                {"name": "Timestamp", "value": timestamp, "inline": True}
            ],
            "footer": {"text": "Sistema de Monitoreo ML"}
        }]
    }
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            return "Mensaje enviado a Discord."
        else:
            return f"Error al enviar mensaje a Discord: {response.text}"
    except Exception as e:
        return f"Error al enviar mensaje a Discord: {e}"
$$;