-- DDL PARA TABLAS Y VISTAS DEL MODELO DE DATOS DE HR GONDERY (PROYECTO FICTICIO CON DATA SINTÉTICA)

-- TABLA QUE SIMULA DATOS DEL HEADCOUNT (SIMULACIÓN DE WORKDAY)

CREATE TABLE Workday_Employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
	gender VARCHAR(10),
	age INT,
    department VARCHAR(50),
    job_role VARCHAR(50),
	shift_type VARCHAR(10),
    hire_date DATE,
	termination_date DATE,
	onleave_date DATE,
    salary DECIMAL(10, 2),
    location VARCHAR(50),
    status VARCHAR(20),
	performance_score INT
);

-- TABLA QUE SIMULA DATOS DE TIEMPOS(SIMULACIÓN DE KRONOS RESUMIDO, NO SON LOGS DE ENTRADAS Y SALIDAS)

CREATE TABLE Kronos_TimeEntries (
    entry_id INT PRIMARY KEY,
    employee_id INT,
    work_date DATE,
    hours_worked DECIMAL(5, 2),
    shift_type VARCHAR(20),
    overtime_hours DECIMAL(5, 2)
);

-- TABLA ENCUESTAS A LOS EMPLEADOS

CREATE TABLE Employee_Surveys (
    survey_id INT PRIMARY KEY,
    employee_id INT,
    survey_date DATE,
    response TEXT,
    satisfaction_score INT

);

-- TABLA QUE ALMACENA DATOS DE PRECISIÓN Y CALIDAD DEL MODELO XGBOOST

CREATE TABLE ML_Model_Accuracy (
    run_id INT IDENTITY(1,1) PRIMARY KEY,
    model_name VARCHAR(50),
    run_date DATETIME,
    accuracy DECIMAL(5, 4),
    precision DECIMAL(5, 4),
    recall DECIMAL(5, 4),
    f1_score DECIMAL(5, 4),
	AUC DECIMAL(5, 4),
	quality VARCHAR (20)
);

-- TABLA QUE ALMACENA LOS RESULTADOS DE LA PREDICCIÓN BATCH DEL TURNOVER

CREATE TABLE Turnover_Predictions (
    prediction_id INT IDENTITY(1,1) PRIMARY KEY,
    employee_id INT,
    prediction_date DATETIME,
    turnover_probability DECIMAL(5, 4),
	predicted_turnover INT

);

-- TABLA QUE ALMACENA LOS RESULTADOS DE LA PREDICCIÓN DE SENTIMIENTO Y TÓPICO PRINCIPAL

CREATE TABLE Sentiment_Predictions (
    prediction_id INT IDENTITY(1,1) PRIMARY KEY,
    survey_id INT,
    prediction_date DATETIME,
    predicted_sentiment VARCHAR(20),
	ISSUE TEXT,
);

-- TABLA QUE ALMACENA LAS MÉTRICAS DE LA PREDICCIÓN DEL OVERTIME (FORECASTING)

CREATE TABLE ML_Model_Metrics_Overtime_Predictions (
	[id] [int] IDENTITY(1,1) NOT NULL,
	[timestamp] [datetime] NULL,
	[department] [varchar](100) NULL,
	[rmse] [float] NULL,
	[mae] [float] NULL,
	[smape] [float] NULL,
	[mase] [float] NULL,
	[model_quality] [varchar](50) NULL
);

-- TABLA QUE ALMACENA LOS RESULTADOS DE LA PREDICCIÓN DEL OVERTIME (FORECASTING)

CREATE TABLE Overtime_Predictions (
    prediction_id INT IDENTITY(1,1) PRIMARY KEY,
    department VARCHAR(50),
    prediction_date DATETIME,
    predicted_overtime DECIMAL(10, 2)
);

-- TABLA QUE SIMULA LOS DATOS DE PLAN DE ENTRENAMIENTO (RESUMIDO, NO SON LOGS)

CREATE TABLE Training (
    training_id INT PRIMARY KEY,
    employee_id INT NOT NULL,
    course_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('Completed', 'Scheduled')),
    training_date DATE NOT NULL,
    plan_year INT NOT NULL CHECK (plan_year IN (2024, 2025)),
    duration_hours DECIMAL(4,1) NOT NULL
);


-- TABLA PARA ALMACENAR LOS CLÚSTERES DE EMPLEADOS

CREATE TABLE Employee_Clusters (
    employee_id INT PRIMARY KEY,
    cluster INT NOT NULL
);

-- TABLA PARA ALMACENAR LAS MÉTRICAS DE CLUSTERING (K-MEANS)

CREATE TABLE Clustering_Metrics (
    run_id INT IDENTITY(1,1) PRIMARY KEY,
    run_datetime DATETIME NOT NULL,
    num_clusters INT NOT NULL,
    silhouette_score FLOAT NOT NULL,
    davies_bouldin_score FLOAT NOT NULL,
    mean_squared_distance FLOAT NOT NULL,
    quality VARCHAR(20) NOT NULL
);

-- TABLA PARA ALMACENAR LOS RESULTADOS DEL MÉTODO DEL CODO

CREATE TABLE Elbow_Method_Results (
    num_clusters INT PRIMARY KEY,
    inertia FLOAT NOT NULL,
    silhouette_score FLOAT NOT NULL,
    davies_bouldin_score FLOAT NOT NULL,
    selected BIT NOT NULL
);


---------------------------------------------------------------------------------------------------


-- VISTA DE MÉTRICAS DE PRECISIÓN DE MODELOS DE FORECAST DE OVERTIME POR DEPARTAMENTO

CREATE VIEW vw_ml_overtime_forecast_metrics 
AS
SELECT 
    FORMAT(timestamp,  'yyyy-MM-dd HH:mm' ) AS run_datetime ,
    department,
    ROUND(rmse,2) AS rmse,
    ROUND(mae,2) AS mae,
    ROUND(smape,2) AS smape,
    ROUND(mase,2) AS mase,
    model_quality,
    CASE 
        WHEN model_quality = 'Bueno' THEN 3
        WHEN model_quality = 'Aceptable' THEN 2
        ELSE 1
    END AS model_score
FROM ML_Model_Metrics_Overtime_Predictions;

-- VISTA DE MÉTRICAS DE PRECISIÓN DE MODELO DE CLASIFICACIÓN DE TURNOVER

CREATE VIEW vw_ml_turnover_classifier_metrics 
AS
SELECT 
	run_id,
	FORMAT(run_date,  'yyyy-MM-dd HH:mm' ) AS run_date,
	ROUND(accuracy,2) AS accuracy,
	ROUND(precision,2) AS precision,
	ROUND(recall,2) AS recall,
	ROUND(f1_score,2) AS f1_score,
	AUC,quality
FROM ML_Model_Accuracy;

-- CREAR VISTA DEL HEADCOUNT FULL

CREATE VIEW vw_headcount_full
AS
SELECT w.employee_id, w.department, w.first_name, w.last_name, w.gender, w.age, w.job_role, w.location , w.salary, w.hire_date, w.termination_date, w.onleave_date, w.performance_score, w.shift_type,
       ROUND(AVG(k.hours_worked),2) as avg_hours_worked, 
       ROUND(AVG(k.overtime_hours),2) as avg_overtime_hours, 
       ROUND(AVG(s.satisfaction_score),2) as avg_satisfaction_score,
	   w.status as status,
       CASE WHEN w.status = 'Terminated' THEN 1 ELSE 0 END AS turnover,
	   tp.predicted_turnover as predicted_turnover,
	   ROUND(tp.turnover_probability,2) as turnover_probability,
	   CASE WHEN tp.turnover_probability >=0.70 AND tp.turnover_probability < 0.90 THEN 'Moderate'
			WHEN tp.turnover_probability >=0.90 THEN 'High'
			ELSE 'No Risk' END AS turnover_risk_level,
		cl.cluster

FROM Workday_Employees w

LEFT JOIN Kronos_TimeEntries k ON w.employee_id = k.employee_id
LEFT JOIN Employee_Surveys s ON w.employee_id = s.employee_id
LEFT JOIN Turnover_Predictions tp ON w.employee_id = tp.employee_id
LEFT JOIN Employee_Clusters cl ON w.employee_id = cl.employee_id

GROUP BY w.employee_id, w.department, w.salary, w.hire_date, w.status, tp.predicted_turnover,tp.turnover_probability,  w.first_name, w.last_name, w.job_role, w.location, w.gender, w.age, w.termination_date, w.onleave_date, w.performance_score, w.shift_type, cl.cluster ;


-- CREAR VISTA DE FORECAST DEL OVERTIME POR DEPARTAMENTO

CREATE VIEW vw_historical_data
AS
WITH historical_data AS
    (SELECT 
            DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, k.work_date), 0)) AS work_date, -- Start of week (Sunday)
            w.department, 
            CAST(SUM(k.overtime_hours) AS FLOAT) AS total_overtime
     FROM Kronos_TimeEntries k
     JOIN Workday_Employees w ON k.employee_id = w.employee_id
     GROUP BY DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, k.work_date), 0)), w.department)

SELECT work_date, 
       department, 
       total_overtime, 
       'historical' AS data_type, 
       CAST(NULL AS FLOAT) AS confidence_lower, 
       CAST(NULL AS FLOAT) AS confidence_upper
FROM historical_data
;

-- CREAR VISTA DEL PLAN DE ENTRENAMIENTO

CREATE VIEW vw_training_completion
AS
SELECT 
    employee_id,
    plan_year,
    COUNT(*) AS total_courses,
    SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed_courses,
    ROUND(
        CASE 
            WHEN COUNT(*) > 0 
            THEN (SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) 
            ELSE 0 
        END, 
        2
    ) AS completion_percentage
FROM Training
GROUP BY employee_id, plan_year;




--------------------------------------------------------------------------------------------------------------------------

-- CREAR PROCEDIMIENTO ALMACENADO PARA SABER EL UMBRAL DE RIESGO CUANDO EL TURNOVER Y EL PREDICHO SON IGUAL A 1

CREATE PROCEDURE avg_turnover_threshold
AS
SELECT AVG(turnover_probability) AS probability FROM vw_headcount_full WHERE turnover = 1 AND  predicted_turnover =1;
