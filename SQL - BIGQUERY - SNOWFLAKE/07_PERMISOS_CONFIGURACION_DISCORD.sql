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