import logging
import pandas as pd
import numpy as np
import pymssql
import xgboost as xgb
import joblib
import os
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import RFE
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import GridSearchCV

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de SQL Server
SQL_SERVER = "XXX.XX.XXX.XX\\XXXXXXX"
SQL_DB = "HR_Analytics"
SQL_USER = "sa"
SQL_PASSWORD = "123456"

# Configuración de Discord
DISCORD_WEBHOOK_URL = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

def send_discord_message(message: str, success: bool = True, error_details: str = None, metrics: dict = None) -> None:
    """Envía un mensaje a Discord con el estado de la ejecución y métricas si están disponibles."""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = 0x00FF00 if success else 0xFF0000
        emoji = "✅" if success else "❌"
        
        description = message
        if error_details:
            description += f"\n\nDetalles del error:\n```\n{error_details}\n```"
        
        fields = [{"name": "Timestamp", "value": timestamp, "inline": True}]
        
        # Agregar métricas si están disponibles
        if metrics:
            metrics_text = "\n".join([f"{k}: {v:.4f}" for k, v in metrics.items()])
            fields.append({"name": "Métricas del Modelo", "value": f"```\n{metrics_text}\n```", "inline": False})
        
        payload = {
            "embeds": [{
                "title": f"{emoji} Predicción de Rotación de Personal",
                "description": description,
                "color": color,
                "fields": fields,
                "footer": {"text": "Sistema de Monitoreo ML"}
            }]
        }
        
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            logging.info(f"Mensaje enviado a Discord: {message}")
        else:
            logging.error(f"Error al enviar mensaje a Discord: {response.text}")
    except Exception as e:
        logging.error(f"Error al enviar mensaje a Discord: {e}")

def get_sql_connection():
    """Establece conexión con SQL Server."""
    try:
        server_name = SQL_SERVER.split('\\')[0]
        conn = pymssql.connect(
            server=server_name,
            user=SQL_USER,
            password=SQL_PASSWORD,
            database=SQL_DB
        )
        return conn
    except Exception as e:
        error_msg = f"Error al conectar con SQL Server: {str(e)}"
        send_discord_message(error_msg, success=False, error_details=str(e))
        raise

def load_data(**context):
    """Carga los datos desde SQL Server."""
    try:
        conn = get_sql_connection()
        
        query = """
        SELECT w.employee_id, w.department, w.salary, w.hire_date, 
               AVG(k.hours_worked) as avg_hours_worked, 
               AVG(k.overtime_hours) as avg_overtime_hours, 
               AVG(s.satisfaction_score) as avg_satisfaction_score,
               CASE WHEN w.status = 'Terminated' THEN 1 ELSE 0 END AS turnover
        FROM Workday_Employees w
        LEFT JOIN Kronos_TimeEntries k ON w.employee_id = k.employee_id
        LEFT JOIN Employee_Surveys s ON w.employee_id = s.employee_id
        GROUP BY w.employee_id, w.department, w.salary, w.hire_date, w.status
        """
        
        df = pd.read_sql(query, conn)
        df = df.reset_index(drop=True)  # Resetear índices
        conn.close()
        
        # Convertir a diccionario orientado a registros para mejor serialización
        data_dict = df.to_dict('records')
        context['task_instance'].xcom_push(key='employee_data', value=data_dict)
        context['task_instance'].xcom_push(key='data_loaded_message', 
            value=f"✅ Carga de datos completada: {len(df)} registros")
        
        return data_dict
    except Exception as e:
        send_discord_message("Error al cargar datos", success=False, error_details=str(e))
        raise

def preprocess_data(**context):
    """Preprocesa los datos para el entrenamiento."""
    try:
        # Cargar datos desde XCom y convertir a DataFrame
        data_dict = context['task_instance'].xcom_pull(task_ids='load_data', key='employee_data')
        df = pd.DataFrame(data_dict)
        
        # Calcular antigüedad
        df['tenure'] = (pd.to_datetime('today') - pd.to_datetime(df['hire_date'])).dt.days / 365
        
        # Manejo de valores faltantes
        df['avg_overtime_hours'] = df['avg_overtime_hours'].fillna(0)
        df.loc[(df['avg_hours_worked'].isna()) & (df['tenure'] > 1/365), 'avg_hours_worked'] = 8
        df.loc[(df['avg_hours_worked'].isna()) & (df['tenure'] <= 1), 'avg_hours_worked'] = 0
        
        # Imputar avg_satisfaction_score usando XGBoost
        def impute_satisfaction_scores(df):
            train_mask = ~df['avg_satisfaction_score'].isna()
            features = ['tenure', 'avg_hours_worked', 'avg_overtime_hours', 'salary']
            
            if train_mask.sum() > 0:  # Si hay datos para entrenar
                X_train = df[train_mask][features]
                y_train = df[train_mask]['avg_satisfaction_score']
                X_predict = df[~train_mask][features]
                
                imputer = xgb.XGBRegressor(
                    n_estimators=100,
                    learning_rate=0.1,
                    random_state=42
                )
                
                imputer.fit(X_train, y_train)
                predictions = imputer.predict(X_predict)
                df.loc[~train_mask, 'avg_satisfaction_score'] = np.round(predictions, decimals=0)
            else:  # Si no hay datos para entrenar, usar la media global
                df['avg_satisfaction_score'] = df['avg_satisfaction_score'].fillna(3)  # Valor medio en escala 1-5
            
            return df
        
        df = impute_satisfaction_scores(df)
        
        # Codificar variables categóricas y asegurar las mismas columnas
        department_dummies = pd.get_dummies(df['department'], prefix='department')
        # Guardar las columnas de departamento para usar en predicción
        department_columns = department_dummies.columns.tolist()
        context['task_instance'].xcom_push(key='department_columns', value=department_columns)
        
        # Unir los dummies al dataframe
        df = pd.concat([df.drop('department', axis=1), department_dummies], axis=1)
        
        # Resetear índices y convertir a formato serializable
        df = df.reset_index(drop=True)
        data_dict = df.to_dict('records')
        context['task_instance'].xcom_push(key='preprocessed_data', value=data_dict)
        context['task_instance'].xcom_push(key='preprocess_message', 
            value="✅ Preprocesamiento de datos completado")
        
        return data_dict
    except Exception as e:
        send_discord_message("Error en el preprocesamiento", success=False, error_details=str(e))
        raise

def train_model(**context):
    """Entrena el modelo de predicción."""
    try:
        # Cargar datos preprocesados desde XCom
        data_dict = context['task_instance'].xcom_pull(task_ids='preprocess_data', key='preprocessed_data')
        df = pd.DataFrame(data_dict)
        df = df.reset_index(drop=True)  # Asegurar índices limpios
        
        # Preparar features
        numeric_features = ['salary', 'avg_hours_worked', 'avg_overtime_hours', 'avg_satisfaction_score']
        department_features = [col for col in df.columns if col.startswith('department_')]
        all_features = numeric_features + department_features
        
        # Aplicar RFE para selección de features
        X_rfe = df[all_features]
        y_rfe = df['turnover']
        
        # Escalar features numéricos
        scaler = StandardScaler()
        X_rfe[numeric_features] = scaler.fit_transform(X_rfe[numeric_features])
        
        # Configurar RFE con XGBoost usando los mismos parámetros base
        xgb_model = xgb.XGBClassifier(
            n_estimators=100,  # Valor medio del grid search
            learning_rate=0.1,
            random_state=42,
            objective='binary:logistic',
            eval_metric='auc'
        )
        rfe_selector = RFE(estimator=xgb_model, n_features_to_select=5, step=1, verbose=1)
        rfe_selector.fit(X_rfe, y_rfe)
        
        # Obtener features seleccionados
        selected_features = [feature for feature, selected in zip(all_features, rfe_selector.support_) if selected]
        
        # Preparar datos para entrenamiento
        X = df[selected_features]
        y = df['turnover']
        
        # División de datos
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Escalado de features
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Balanceo de clases con SMOTE
        smote = SMOTE(random_state=42)
        X_train_balanced, y_train_balanced = smote.fit_resample(X_train_scaled, y_train)
        
        # Grid Search para optimización de hiperparámetros
        param_grid = {
            'n_estimators': [50, 100, 200],
            'max_depth': [3, 5, 7],
            'learning_rate': [0.01, 0.1],
            'subsample': [0.8, 1.0],
            'colsample_bytree': [0.8, 1.0],
            'min_child_weight': [1, 3],
            'gamma': [0, 0.1]
        }
        
        # Crear modelo base XGBoost
        xgb_base = xgb.XGBClassifier(
            objective='binary:logistic',
            eval_metric='auc',
            random_state=42
        )
        
        grid_search = GridSearchCV(
            estimator=xgb_base,
            param_grid=param_grid,
            cv=5,
            scoring='f1',
            n_jobs=-1,
            verbose=1
        )
        
        grid_search.fit(X_train_balanced, y_train_balanced)
        
        # Entrenar modelo final con mejores parámetros
        best_model = grid_search.best_estimator_
        best_model.fit(X_train_balanced, y_train_balanced)
        
        # Calcular probabilidades y predicciones
        y_pred_proba = best_model.predict_proba(X_test_scaled)[:, 1]

        # Encontrar el mejor umbral de decisión
        thresholds = np.arange(0.1, 0.9, 0.1)
        best_threshold = 0.5
        best_f1 = 0

        for threshold in thresholds:
            y_pred_threshold = (y_pred_proba >= threshold).astype(int)
            f1 = f1_score(y_test, y_pred_threshold)
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = threshold

        # Usar el umbral optimizado para las predicciones finales
        final_predictions = (y_pred_proba >= best_threshold).astype(int)
        
        # Calcular métricas con el umbral optimizado
        metrics = {
            'accuracy': accuracy_score(y_test, final_predictions),
            'precision': precision_score(y_test, final_predictions, zero_division=0),
            'recall': recall_score(y_test, final_predictions, zero_division=0),
            'f1_score': f1_score(y_test, final_predictions, zero_division=0)
        }
        
        # Guardar el umbral optimizado en XCom para usarlo en save_predictions
        context['task_instance'].xcom_push(key='best_threshold', value=best_threshold)
        
        # Guardar modelo, scaler y features seleccionados
        os.makedirs('Modelos Entrenados', exist_ok=True)
        joblib.dump(best_model, 'Modelos Entrenados/turnover_model.pkl')
        joblib.dump(scaler, 'Modelos Entrenados/scaler.pkl')
        joblib.dump(selected_features, 'Modelos Entrenados/selected_features.pkl')
        
        # Guardar métricas en XCom
        context['task_instance'].xcom_push(key='model_metrics', value=metrics)
        context['task_instance'].xcom_push(key='selected_features', value=selected_features)
          # Guardar mensaje con métricas y features seleccionados
        train_message = f"""✅ Modelo entrenado exitosamente
Features seleccionados: {', '.join(selected_features)}

Métricas del modelo:
- Accuracy: {metrics['accuracy']:.4f}
- Precision: {metrics['precision']:.4f}
- Recall: {metrics['recall']:.4f}
- F1-Score: {metrics['f1_score']:.4f}"""
        context['task_instance'].xcom_push(key='train_message', value=train_message)
        context['task_instance'].xcom_push(key='train_metrics', value=metrics)
        
        # Guardar métricas en la base de datos
        conn = get_sql_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO ML_Model_Accuracy 
                (model_name, run_date, accuracy, precision, recall, f1_score)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                ('Turnover_Prediction', datetime.now(), metrics['accuracy'],
                 metrics['precision'], metrics['recall'], metrics['f1_score'])
            )
            conn.commit()
            logging.info("Métricas guardadas exitosamente en ML_Model_Accuracy")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error al guardar métricas en ML_Model_Accuracy: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
        
        return metrics
    except Exception as e:
        send_discord_message("Error en el entrenamiento del modelo", success=False, error_details=str(e))
        raise

def save_predictions(**context):
    """Guarda las predicciones en la base de datos."""
    try:
        # Cargar datos preprocesados desde XCom
        data_dict = context['task_instance'].xcom_pull(task_ids='preprocess_data', key='preprocessed_data')
        df = pd.DataFrame(data_dict)
        df = df.reset_index(drop=True)  # Asegurar índices limpios
        selected_features = context['task_instance'].xcom_pull(task_ids='train_model', key='selected_features')
        department_columns = context['task_instance'].xcom_pull(task_ids='preprocess_data', key='department_columns')
        
        # Cargar modelo y scaler
        model = joblib.load('Modelos Entrenados/turnover_model.pkl')
        scaler = joblib.load('Modelos Entrenados/scaler.pkl')
        
        # Asegurar que tenemos todas las columnas necesarias
        for col in department_columns:
            if col not in df.columns:
                df[col] = 0
        
        # Preparar datos para predicción
        X = df[selected_features]
        X_scaled = scaler.transform(X)
        
        # Obtener el umbral optimizado del entrenamiento
        best_threshold = context['task_instance'].xcom_pull(task_ids='train_model', key='best_threshold')
        
        # Calcular probabilidades para todos los datos
        probabilities_np = model.predict_proba(X_scaled)[:, 1]
        predictions_np = (probabilities_np >= best_threshold).astype(int)
        
        # Usar las métricas del entrenamiento para mantener consistencia
        train_metrics = context['task_instance'].xcom_pull(task_ids='train_model', key='train_metrics')
        
        # Filtrar solo empleados activos (turnover = 0)
        active_mask = df['turnover'] == 0
        active_employees = df[active_mask]
        active_probabilities = probabilities_np[active_mask]
        active_predictions = predictions_np[active_mask]
        
        # Identificar empleados de alto riesgo entre los activos (≥85%) y en riesgo (≥70%)
        high_risk_count = sum(active_probabilities >= 0.85)
        at_risk_count = sum(active_probabilities >= 0.70)
        
        # Convertir numpy arrays a listas Python y redondear probabilidades
        probabilities = [float(round(p, 4)) for p in probabilities_np]
        predictions = [int(p) for p in predictions_np]
        
        # Guardar predicciones en la base de datos
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Primero eliminar predicciones anteriores
        cursor.execute("DELETE FROM Turnover_Predictions")
        
        # Insertar nuevas predicciones
        for i, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO Turnover_Predictions 
                (employee_id, prediction_date, turnover_probability, predicted_turnover)
                VALUES (%s, %s, %s, %s)
                """,
                (int(row['employee_id']), datetime.now(), probabilities[i], predictions[i])
            )
        conn.commit()
        conn.close()
        
        # Calcular porcentajes sobre empleados activos
        total_active = len(active_employees)
        risk_percentage = (at_risk_count / total_active * 100) if total_active > 0 else 0
        high_risk_percentage = (high_risk_count / total_active * 100) if total_active > 0 else 0
        
        predictions_message = f"""✅ Predicciones completadas:
        - Total empleados activos: {total_active}
        - Empleados en riesgo de rotación (≥70%): {at_risk_count} ({risk_percentage:.1f}% de activos)
        - Empleados de alto riesgo (≥85%): {high_risk_count} ({high_risk_percentage:.1f}% de activos)"""
        
        # Recopilar todos los mensajes
        data_loaded_message = context['task_instance'].xcom_pull(task_ids='load_data', key='data_loaded_message')
        preprocess_message = context['task_instance'].xcom_pull(task_ids='preprocess_data', key='preprocess_message')
        
        # Construir mensaje final
        final_message = f"""🤖 Pipeline de Predicción de Rotación Completado

{data_loaded_message}
{preprocess_message}

{predictions_message}

Métricas del modelo:
- Accuracy: {train_metrics['accuracy']:.4f}
- Precision: {train_metrics['precision']:.4f}
- Recall: {train_metrics['recall']:.4f}
- F1-Score: {train_metrics['f1_score']:.4f}"""
        
        send_discord_message(final_message, success=True)
        
    except Exception as e:
        error_message = "❌ Error en el pipeline de predicción de rotación"
        send_discord_message(error_message, success=False, error_details=str(e))
        raise

default_args = {
    'owner': 'JDRP',
    'start_date': datetime(2025, 5, 20),
    'retries': 1
}

with DAG(
    'hr_turnover_prediction',
    default_args=default_args,
    schedule='0 6 * * *',  # Diariamente a las 6 AM
    catchup=False
) as dag:
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    
    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data
    )
    
    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )
    
    predict_task = PythonOperator(
        task_id='save_predictions',
        python_callable=save_predictions
    )
    
    # Definir dependencias
    load_task >> preprocess_task >> train_task >> predict_task