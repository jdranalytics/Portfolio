-- COLUMNAS Y TIPO DE DATOS: id_cliente, ST, edad, IN, ingresos_anuales, FL, puntaje_crediticio, IN, historial_pagos, ST, deuda_actual, FL, antiguedad_laboral, IN, estado_civil, ST, numero_dependientes, IN, tipo_empleo, ST, fecha_solicitud, DA, solicitud_credito, FL, inicio_mes, DA, inicio_semana, DA

-- TABLA PRINCIPAL (SOLICITUDES) ESTA TABLA VIENE DE UNA INGESTA CON PROCESO AIRFLOW DE LOCAL A GOOGLE CLOUD

SELECT id_cliente, edad, ingresos_anuales, puntaje_crediticio, historial_pagos, deuda_actual, antiguedad_laboral, estado_civil, numero_dependientes, tipo_empleo, fecha_solicitud, solicitud_credito, inicio_mes, inicio_semana 
FROM solicitudes;

-- TABLA MAESTRA



-- MODELO DE CLUSTERING (K-MEANS) (modelo_clustering_creditos)

CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering`
OPTIONS(
  model_type='kmeans',
  num_clusters=5,  
  standardize_features = TRUE
) AS
SELECT
  edad,
  ingresos_anuales,
  puntaje_crediticio,
  deuda_actual,
  antiguedad_laboral,
  numero_dependientes

FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`;


-- CREAR/MODIFICAR MODELO K-MEANS K2 A K6 PARA ELBOW ANALYSIS (clustering_k2_k3_k4_k5_k6)

CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k2`
OPTIONS(
  model_type='kmeans',
  num_clusters=2
) AS
SELECT
  edad,
  ingresos_anuales,
  puntaje_crediticio,
  deuda_actual,
  antiguedad_laboral,
  numero_dependientes
FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`;

CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k3`
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

CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k4`
OPTIONS(
  model_type='kmeans',
  num_clusters=4
) AS
SELECT
  edad,
  ingresos_anuales,
  puntaje_crediticio,
  deuda_actual,
  antiguedad_laboral,
  numero_dependientes
FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`;

CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k5`
OPTIONS(
  model_type='kmeans',
  num_clusters=5
) AS
SELECT
  edad,
  ingresos_anuales,
  puntaje_crediticio,
  deuda_actual,
  antiguedad_laboral,
  numero_dependientes
FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`;

CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k6`
OPTIONS(
  model_type='kmeans',
  num_clusters=6
) AS
SELECT
  edad,
  ingresos_anuales,
  puntaje_crediticio,
  deuda_actual,
  antiguedad_laboral,
  numero_dependientes
FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`;


-- TABLA PARA ELBOW ANALYSIS DE K2 - K6 ()

SELECT
  2 AS num_clusters,
  davies_bouldin_index,
  mean_squared_distance
FROM
  ML.EVALUATE(MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k2`)
UNION ALL
SELECT
  3 AS num_clusters,
  davies_bouldin_index,
  mean_squared_distance
FROM
  ML.EVALUATE(MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k3`)
UNION ALL
SELECT
  4 AS num_clusters,
  davies_bouldin_index,
  mean_squared_distance
FROM
  ML.EVALUATE(MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k4`)
  UNION ALL
SELECT
  5 AS num_clusters,
  davies_bouldin_index,
  mean_squared_distance
FROM
  ML.EVALUATE(MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k5`)
UNION ALL
SELECT
  6 AS num_clusters,
  davies_bouldin_index,
  mean_squared_distance
FROM
  ML.EVALUATE(MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_clustering_k6`)
ORDER BY num_clusters;



-- TABLA DE PREDICCIÓN MASIVA DE CLUSTERING (modelo_clustering_creditos)

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

-- MODELO DE REGRESIÓN LOGARÍTMICA (modelo_reglog_creditos)

CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_aprobacion`
TRANSFORM(
  ML.STANDARD_SCALER(edad) OVER () AS edad,
  ML.STANDARD_SCALER(ingresos_anuales) OVER () AS ingresos_anuales,
  ML.STANDARD_SCALER(puntaje_crediticio) OVER () AS puntaje_crediticio,
  ML.STANDARD_SCALER(deuda_actual) OVER () AS deuda_actual,
  ML.STANDARD_SCALER(antiguedad_laboral) OVER () AS antiguedad_laboral,
  historial_pagos,
  estado_civil,
  numero_dependientes,
  tipo_empleo,
  solicitud_credito
)
OPTIONS(
  model_type='logistic_reg',
  input_label_cols=['solicitud_credito'],
  l2_reg=1.0,
  auto_class_weights=TRUE,
  data_split_method='AUTO_SPLIT'
) AS
SELECT *
FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`
WHERE solicitud_credito IS NOT NULL;

-- PREDICCIÓN MASIVA DE CLASIFICACIÓN REGLOG (prediccion_reglog_creditos)

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
  
-- EVALUACIÓN MODELO REGLOG (evaluacion_modelo_reglog)

CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.evaluacion_modelo_reglog` AS

SELECT
*
FROM
  ML.EVALUATE(MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_aprobacion`);

SELECT * FROM  `adroit-terminus-450816-r9.solicitudes_credito.evaluacion_modelo_reglog`;

-- SOLICITUDES AGRUPADAS PARA FORECASTING (solicitudes_historico)

CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.solicitudes_agregadas` AS
SELECT
  DATE_TRUNC(fecha_solicitud, MONTH) AS fecha_mes,
  COUNT(*) AS total_solicitudes, -- Total de solicitudes (revisadas + no revisadas)
  COUNTIF(solicitud_credito IS NOT NULL) AS solicitudes_revisadas, -- Total de revisadas
  SUM(CASE WHEN solicitud_credito = 1 THEN 1 ELSE 0 END) AS solicitudes_aprobadas, -- Total de aprobadas
  SAFE_DIVIDE(
    SUM(CASE WHEN solicitud_credito = 1 THEN 1 ELSE 0 END),
    COUNTIF(solicitud_credito IS NOT NULL)
  ) * 100 AS tasa_aprobacion -- Tasa de aprobación (%)
FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes`
GROUP BY fecha_mes
ORDER BY fecha_mes;

-- MODELO ARIMA (SOLICITUDES RECIBIDAS) (modelo_arima_creditos_total_solicitudes)

CREATE OR REPLACE MODEL `adroit-terminus-450816-r9.solicitudes_credito.modelo_solicitudes`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='fecha_mes',
  time_series_data_col='total_solicitudes',
  data_frequency='MONTHLY',
  horizon=6 -- Pronosticar los próximos 6 meses
) AS
SELECT
  fecha_mes,
  total_solicitudes
FROM `adroit-terminus-450816-r9.solicitudes_credito.solicitudes_agregadas`;

-- PREDICCIÓN DE SOLICITUDES RECIBIDAS (pronostico_arima_creditos_solicitudes)

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
  
-- MODELO ARIMA SOLICITUDES APROBADAS (modelo_arima_credito_aprobaciones)

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

-- PREDICCIÓN ARIMA SOLICITUDES APROBADAS (pronostico_arima_creditos_aprobacion)

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

-- MODELO ARIMA TASA DE APROBACION (modelo_arima_creditos_tasa_aprobacion)

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

-- PREDICCIÓN ARIMA TASA DE APROBACIÓN (pronostico_arima_creditos_tasa_aprobacion)

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

-- FORECAST FINAL AGRUPADO (forecast_final)

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

-- FORECAST FINAL COMBINADO CON HISTÓRICO 12 MESES (forecast_creditos_combinado)

CREATE OR REPLACE TABLE `adroit-terminus-450816-r9.solicitudes_credito.forecast_combinado` AS
-- Datos históricos
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

-- Datos pronosticados
SELECT
  CAST(fecha_mes AS DATE) AS fecha_mes, -- Convertimos TIMESTAMP a DATE para que coincida
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