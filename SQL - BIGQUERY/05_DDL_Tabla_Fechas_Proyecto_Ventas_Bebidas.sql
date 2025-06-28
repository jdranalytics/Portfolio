----------------------------------------------------------------------------------------------
-- DDL PARA CREACIÓN DE VISTA DE FECHAS PARA EL PROYECTO DE VENTAS DE BEBIDAS
-- Esta tabla de fechas facilita el análisis temporal en Tableau y otras herramientas de BI.
-- La vista genera un rango de fechas basado en las fechas de ventas e inventarios (Tablas de Hecho).
----------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.vw_dates AS
WITH AllDates AS (
    SELECT FECHA::DATE AS DateValue FROM BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.VENTAS
    UNION ALL
    SELECT FECHA_ACTUALIZACION AS DateValue FROM BEBIDAS_PROJECT.BEBIDAS_ANALYTICS.INVENTARIOS
),
MinMaxDates AS (
    SELECT
        MIN(DateValue) AS MinGlobalDate,
        MAX(DateValue) AS MaxGlobalDate
    FROM AllDates
),
DateSeries AS (
    SELECT
        DATEADD(day, SEQ4(), (SELECT MinGlobalDate FROM MinMaxDates)) AS generated_date
    FROM
        TABLE(GENERATOR(ROWCOUNT => 365 * 50)) 
)
SELECT
    generated_date AS full_date
FROM
    DateSeries
WHERE
    generated_date >= (SELECT MinGlobalDate FROM MinMaxDates) AND
    generated_date <= (SELECT MaxGlobalDate FROM MinMaxDates)
ORDER BY
    full_date;