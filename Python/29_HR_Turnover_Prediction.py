import pandas as pd
import pyodbc
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import joblib
import datetime
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Configuración de SQL Server
SQL_SERVER = "172.28.192.1:50121\\SQLEXPRESS"
SQL_DB = "HR_Analytics"
SQL_USER = "sa"
SQL_PASSWORD = "123456"

def train_turnover_model():
    try:
        # Conectar a SQL Server
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};'
            f'UID={SQL_USER};PWD={SQL_PASSWORD}'
        )
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

        # Preprocesamiento
        df['tenure'] = (pd.to_datetime('today') - pd.to_datetime(df['hire_date'])).dt.days / 365
        df = pd.get_dummies(df, columns=['department'], drop_first=True)
        df.fillna({'avg_hours_worked': 0, 'avg_overtime_hours': 0, 'avg_satisfaction_score': 3}, inplace=True)

        # Features y target
        feature_cols = ['salary', 'tenure', 'avg_hours_worked', 'avg_overtime_hours', 'avg_satisfaction_score'] + \
                       [col for col in df if col.startswith('department_')]
        X = df[feature_cols]
        y = df['turnover']

        # Dividir datos
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Entrenar modelo
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        # Evaluar
        y_pred = model.predict(X_test)
        metrics = {
            'accuracy': accuracy_score(y_test, y_pred),
            'precision': precision_score(y_test, y_pred, zero_division=0),
            'recall': recall_score(y_test, y_pred, zero_division=0),
            'f1_score': f1_score(y_test, y_pred, zero_division=0)
        }

        # Guardar modelo
        joblib.dump(model, 'turnover_model.pkl')

        # Guardar métricas en SQL Server
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ML_Model_Accuracy (model_name, run_date, accuracy, precision, recall, f1_score)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ('Turnover_Prediction', datetime.datetime.now(), metrics['accuracy'], 
             metrics['precision'], metrics['recall'], metrics['f1_score'])
        )

        # Guardar predicciones
        df['turnover_probability'] = model.predict_proba(X)[:, 1]
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO Turnover_Predictions (employee_id, prediction_date, turnover_probability)
                VALUES (?, ?, ?)
                """,
                (row['employee_id'], datetime.datetime.now(), row['turnover_probability'])
            )
        conn.commit()
        conn.close()

        logging.info(f"Turnover Prediction - Metrics: {metrics}")
        return metrics

    except Exception as e:
        logging.error(f"Error in turnover prediction: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise

if __name__ == "__main__":
    train_turnover_model()