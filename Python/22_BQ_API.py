from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from google.oauth2 import service_account
from werkzeug.exceptions import BadRequest
import os

# Inicializar Flask
app = Flask(__name__)

# Configurar cliente de BigQuery
project_id = "adroit-terminus-450816-r9"
if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
    credentials = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    )
else:
    credentials = None  # Usará ADC en Cloud Run
client = bigquery.Client(credentials=credentials, project=project_id)

# Endpoint de bienvenida
@app.route('/')
def home():
    return jsonify({"message": "Bienvenido a la API de BigQuery"})

# Endpoint para obtener datos
@app.route('/get_data', methods=['GET'])
def get_data():
    try:
        query = "SELECT * FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`"
        results = client.query(query).to_dataframe()
        return jsonify(results.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para insertar datos
@app.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        data = request.json
        if not isinstance(data, dict):
            raise BadRequest("El cuerpo de la solicitud debe ser un objeto JSON válido.")

        table_id = "adroit-terminus-450816-r9.solicitudes_credito.solicitudes"
        errors = client.insert_rows_json(table_id, [data])

        if errors:
            return jsonify({"error": errors}), 400
        return jsonify({"message": "Datos insertados correctamente"}), 200
    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para actualizar datos
@app.route('/update_data', methods=['POST'])
def update_data():
    try:
        data = request.json
        if not data or 'condicion' not in data or 'columna' not in data or 'nuevo_valor' not in data:
            raise BadRequest("Se requieren los campos 'condicion', 'columna' y 'nuevo_valor'.")

        # Validar inputs para evitar inyecciones SQL
        allowed_columns = ['estado', 'monto', 'fecha']  # Ajusta según tu esquema
        if data['columna'] not in allowed_columns:
            raise BadRequest("Columna no permitida.")

        # Consulta parametrizada
        query = f"""
        UPDATE `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`
        SET `{data['columna']}` = @nuevo_valor
        WHERE id = @condicion
        """
        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("nuevo_valor", "STRING", data['nuevo_valor']),
                ScalarQueryParameter("condicion", "STRING", data['condicion'])
            ]
        )
        client.query(query, job_config=job_config)
        return jsonify({"message": "Datos actualizados correctamente"}), 200
    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para eliminar datos
@app.route('/delete_data', methods=['DELETE'])
def delete_data():
    try:
        data = request.json
        if not data or 'condicion' not in data:
            raise BadRequest("Se requiere el campo 'condicion'.")

        # Consulta parametrizada
        query = """
        DELETE FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`
        WHERE id = @condicion
        """
        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("condicion", "STRING", data['condicion'])
            ]
        )
        client.query(query, job_config=job_config)
        return jsonify({"message": "Datos eliminados correctamente"}), 200
    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Usar el puerto proporcionado por Cloud Run o 8080 por defecto
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)