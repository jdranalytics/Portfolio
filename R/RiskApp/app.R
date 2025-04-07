library(shiny)
library(shinydashboard)
library(shinythemes)
library(bigrquery)
library(DBI)
library(dplyr)
library(ggplot2)
library(plotly)
library(forecast)
library(DT)
library(httr)
library(jsonlite)
library(lubridate)
library(scales)
library(shinyjs)
library(shinyWidgets)
library(shinyAce)
library(viridis)

# Configurar un mirror de CRAN predeterminado
if (is.null(getOption("repos")) || is.null(getOption("repos")["CRAN"]) || getOption("repos")["CRAN"] == "@CRAN@") {
  options(repos = c(CRAN = "https://cloud.r-project.org"))
}

# Manejo de paquetes requeridos
required_packages <- c('dplyr', 'ggplot2', 'caret', 'forecast', 'tidyverse', 'tidyplots', 'plotly', 'tidyr', 'GGally', 'ggforce', 'ggridges', 'ggalt', 'ggExtra', 'ggalluvial', 'waterfalls')
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

# CONFIGURACIÓN INICIAL ----
project_id <- "adroit-terminus-450816-r9"
dataset_id <- "solicitudes_credito"
credentials_path <- "C:/Users/joey_/Desktop/AIRFLOW/adroit-terminus-450816-r9-1b90cfcf6a76.json"
DISCORD_WEBHOOK_URL <- "https://discordapp.com/api/webhooks/1354192765130375248/MF7bEPPlHnrzgYnJJ4iev7xTr0TrxVpqKw_MOVVIRseppELwK0hBM7VMZf8DQnVPpvh6"
OPENAI_API_KEY_PATH <- "C:/Users/joey_/Desktop/AIRFLOW/API KEYS/openai.txt"
GEMINI_API_KEY_PATH <- "C:/Users/joey_/Desktop/AIRFLOW/API KEYS/gemini.txt"

# MODELOS DISPONIBLES ----
OPENAI_MODELS <- c("gpt-4o-mini")
GEMINI_MODELS <- c("gemini-1.5-flash", "gemini-2.0-flash" , "gemini-2.0-flash-lite")
DEEPSEEK_MODELS <- c("deepseek-chat", "deepseek-reasoner")

# AUTENTICACIÓN ----
bq_auth(path = credentials_path)

# CARGAR LLAVES API ----
load_api_key <- function(path) {
  if (!file.exists(path)) {
    message("El archivo no existe: ", path)
    return(NULL)
  }
  tryCatch({
    key <- trimws(readLines(path, n = 1, warn = FALSE))
    if (nchar(key) == 0) stop("La llave API está vacía")
    message("Llave API cargada desde ", path, ". Longitud: ", nchar(key))
    return(key)
  }, error = function(e) {
    message("Error al cargar la llave API desde ", path, ": ", e$message)
    return(NULL)
  })
}

OPENAI_API_KEY <- load_api_key(OPENAI_API_KEY_PATH)
GEMINI_API_KEY <- load_api_key(GEMINI_API_KEY_PATH)
DEEPSEEK_API_KEY <- load_api_key("C:/Users/joey_/Desktop/AIRFLOW/API KEYS/deepseek.txt")

# FUNCIONES AUXILIARES ----
send_discord_message <- function(message, success = TRUE) {
  timestamp <- Sys.time()
  color <- ifelse(success, 65280, 16711680)
  emoji <- ifelse(success, "✅", "❌")
  
  payload <- list(
    embeds = list(
      list(
        title = sprintf("%s Notificación de Carga BigQuery", emoji),
        description = message,
        color = color,
        fields = list(list(name = "Timestamp", value = as.character(timestamp), inline = TRUE)),
        footer = list(text = "Sistema de Monitoreo ETL: Proyecto Solicitudes de Crédito")
      )
    )
  )
  
  tryCatch({
    POST(DISCORD_WEBHOOK_URL, body = toJSON(payload, auto_unbox = TRUE), encode = "json")
  }, error = function(e) {
    message("Error al enviar mensaje a Discord: ", e$message)
  })
}

# VERIFICAR ESTADO DE LAS APIs ----
check_openai_status <- function() {
  if (is.null(OPENAI_API_KEY)) return(FALSE)
  response <- tryCatch({
    POST(
      url = "https://api.openai.com/v1/chat/completions",
      body = list(
        model = "gpt-4o-mini",
        messages = list(list(role = "user", content = "Ping")),
        max_tokens = 10
      ),
      add_headers(
        "Content-Type" = "application/json",
        "Authorization" = paste("Bearer", OPENAI_API_KEY)
      ),
      encode = "json"
    )
  }, error = function(e) {
    message("Error al conectar con OpenAI: ", e$message)
    return(NULL)
  })
  status <- !is.null(response) && status_code(response) == 200
  message("Estado de OpenAI: ", ifelse(status, "En línea", "Fuera de línea"))
  return(status)
}

check_gemini_status <- function() {
  if (is.null(GEMINI_API_KEY)) return(FALSE)
  response <- tryCatch({
    POST(
      url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent",
      body = list(
        contents = list(list(parts = list(list(text = "Ping"))))
      ),
      add_headers(
        "Content-Type" = "application/json",
        "x-goog-api-key" = GEMINI_API_KEY
      ),
      encode = "json"
    )
  }, error = function(e) {
    message("Error al conectar con Gemini: ", e$message)
    return(NULL)
  })
  status <- !is.null(response) && status_code(response) == 200
  message("Estado de Gemini: ", ifelse(status, "En línea", "Fuera de línea"))
  return(status)
}

check_deepseek_status <- function() {
  if (is.null(DEEPSEEK_API_KEY)) {
    message("DeepSeek API key is not available")
    return(FALSE)
  }
  
  response <- tryCatch({
    POST(
      url = "https://api.deepseek.com/v1/chat/completions",
      body = list(
        model = "deepseek-chat",
        messages = list(list(role = "user", content = "Ping")),
        max_tokens = 10
      ),
      add_headers(
        "Content-Type" = "application/json",
        "Authorization" = paste("Bearer", DEEPSEEK_API_KEY)
      ),
      encode = "json"
    )
  }, error = function(e) {
    message("Error checking DeepSeek status: ", e$message)
    return(FALSE)
  })
  
  if (!is.null(response)) {
    resp_content <- content(response, "parsed")
    if (!is.null(resp_content$error)) {
      message("DeepSeek API error: ", resp_content$error$message)
      return(FALSE)
    }
    return(TRUE)
  }
  
  return(FALSE)
}

####################### ETL ##########################

run_query <- function(query, task_name) {
  send_discord_message(sprintf("Iniciando tarea: %s", task_name), success = TRUE)
  tryCatch({
    job <- bq_project_query(project_id, query)
    bq_table_download(job)
    send_discord_message(sprintf("✅ %s completada con éxito", task_name), success = TRUE)
    return(TRUE)
  }, error = function(e) {
    send_discord_message(sprintf("❌ Error en %s: %s", task_name, e$message), success = FALSE)
    return(FALSE)
  })
}

update_aggregated_table <- function() {
  query <- sprintf("
    CREATE OR REPLACE TABLE `%s.%s.solicitudes_agregadas` AS
    SELECT
      DATE_TRUNC(fecha_solicitud, MONTH) AS fecha_mes,
      COUNT(*) AS total_solicitudes,
      SUM(CASE WHEN solicitud_credito IS NULL THEN 0 ELSE 1 END) AS solicitudes_revisadas,
      SUM(CASE WHEN solicitud_credito = 1 THEN 1 ELSE 0 END) AS aprobadas,
      SUM(CASE WHEN solicitud_credito = 0 THEN 1 ELSE 0 END) AS rechazadas,
      SAFE_DIVIDE(
        SUM(CASE WHEN solicitud_credito = 1 THEN 1 ELSE 0 END),
        COUNT(CASE WHEN solicitud_credito IS NOT NULL THEN 1 END)
      ) AS tasa_aprobacion
    FROM `%s.%s.solicitudes`
    GROUP BY fecha_mes
    ORDER BY fecha_mes;
  ", project_id, dataset_id, project_id, dataset_id)
  run_query(query, "Actualizar tabla agregada")
}

update_master_table <- function() {
  query <- sprintf("
    CREATE OR REPLACE TABLE `%s.%s.solicitudes_maestra` AS
    SELECT
      s.*,
      r.predicted_solicitud_credito,
      (SELECT prob
        FROM UNNEST(r.predicted_solicitud_credito_probs)
        WHERE label = r.predicted_solicitud_credito) AS probabilidad_aprobacion,
      c.cluster,
      CASE WHEN s.solicitud_credito IS NULL THEN 'Pendiente (Predicha)'
        ELSE 'Evaluada' END AS estado
    FROM `%s.%s.solicitudes` s
    LEFT JOIN `%s.%s.predicciones_aprobaciones_reglog` r
      ON s.id_cliente = r.id_cliente
    LEFT JOIN `%s.%s.predicciones_clustering` c
      ON s.id_cliente = c.id_cliente;
  ", project_id, dataset_id, project_id, dataset_id, project_id, dataset_id, project_id, dataset_id)
  run_query(query, "Actualizar tabla maestra")
}

retrain_clustering_model <- function() {
  query <- sprintf("
    CREATE OR REPLACE MODEL `%s.%s.modelo_clustering`
    OPTIONS(model_type='kmeans', num_clusters=3) AS
    SELECT
      edad,
      ingresos_anuales,
      puntaje_crediticio,
      deuda_actual,
      antiguedad_laboral,
      numero_dependientes
    FROM `%s.%s.solicitudes`;
  ", project_id, dataset_id, project_id, dataset_id)
  run_query(query, "Reentrenar modelo de clustering")
}

update_clustering_predictions <- function() {
  query <- sprintf("
    CREATE OR REPLACE TABLE `%s.%s.predicciones_clustering` AS
    SELECT
      id_cliente,
      CENTROID_ID AS cluster
    FROM ML.PREDICT(MODEL `%s.%s.modelo_clustering`,
      (SELECT
        id_cliente,
        edad,
        ingresos_anuales,
        puntaje_crediticio,
        deuda_actual,
        antiguedad_laboral,
        numero_dependientes
      FROM `%s.%s.solicitudes`));
  ", project_id, dataset_id, project_id, dataset_id, project_id, dataset_id)
  run_query(query, "Actualizar predicciones de clustering")
}

retrain_logistic_model <- function() {
  query <- sprintf("
    CREATE OR REPLACE MODEL `%s.%s.modelo_aprobacion`
    OPTIONS(model_type='logistic_reg', input_label_cols=['solicitud_credito']) AS
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
    FROM `%s.%s.solicitudes`
    WHERE solicitud_credito IS NOT NULL;
  ", project_id, dataset_id, project_id, dataset_id)
  run_query(query, "Reentrenar modelo de regresión logística")
}

update_logistic_predictions <- function() {
  query <- sprintf("
    CREATE OR REPLACE TABLE `%s.%s.predicciones_aprobaciones_reglog` AS
    SELECT
      id_cliente,
      predicted_solicitud_credito,
      predicted_solicitud_credito_probs
    FROM ML.PREDICT(MODEL `%s.%s.modelo_aprobacion`,
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
      FROM `%s.%s.solicitudes`
      WHERE solicitud_credito IS NULL));
  ", project_id, dataset_id, project_id, dataset_id, project_id, dataset_id)
  run_query(query, "Actualizar predicciones de regresión logística")
}

list_tables <- function() {
  tables <- bq_dataset_tables(bq_dataset(project_id, dataset_id))
  if (length(tables) > 0) sapply(tables, function(t) t$table) else character(0)
}

load_table_data <- function(table_name) {
  query <- sprintf("SELECT * FROM `%s.%s.%s`", project_id, dataset_id, table_name)
  message("Ejecutando consulta para ", table_name, ": ", query)
  tryCatch({
    job <- bq_project_query(project_id, query)
    datos <- bq_table_download(job)
    if (is.null(datos) || nrow(datos) == 0) {
      warning("No se encontraron datos en ", table_name, ". Verifica que la tabla exista y tenga datos.")
      return(data.frame())
    }
    message("Datos cargados exitosamente de ", table_name, ". Filas: ", nrow(datos))
    return(datos)
  }, error = function(e) {
    message("Error al cargar datos de ", table_name, ": ", e$message)
    return(data.frame())
  })
}

############ FUNCIÓN CONSULTA AI ################################################################

diccionario_contexto <- paste(
  "DICCIONARIO DE CONTEXTO Y MÉTRICAS:",
  "0. NO SIMULAR DATOS",
  "1. Métricas de Solicitudes:",
  " - Solicitudes aprobadas = sum(solicitud_credito == 1, na.rm = TRUE)",
  " - Solicitudes rechazadas = sum(solicitud_credito == 0, na.rm = TRUE)",
  " - Solicitudes predichas aprobadas = sum(predicted_solicitud_credito == 1, na.rm = TRUE)",
  " - Solicitudes predichas rechazadas = sum(predicted_solicitud_credito == 0, na.rm = TRUE)",
  " - Total solicitudes evaluadas = sum(estado == 'Evaluada', na.rm = TRUE)",
  " - Total solicitudes predichas = sum(estado == 'Pendiente (Predicha)', na.rm = TRUE)",
  "2. Relaciones importantes:",
  " - Si solicitud_credito IS NULL: Solicitud pendiente por evaluación (participa en predicciones)",
  " - Si predicted_solicitud_credito IS NULL: Solicitud ya evaluada (estado = 'Evaluada')",
  " - Estado 'Evaluada': Solicitudes con decisión real (solicitud_credito no es NULL)",
  " - Estado 'Pendiente (Predicha)': Solicitudes sin decisión real (solicitud_credito IS NULL)",
  " - Los NULL en solicitud_credito y predicted_solicitud_credito no se grafican",
  sep = "\n"
)

diccionario_visuales <- paste(
  "DICCIONARIO DE VISUALES RECOMENDADAS:",
  "1. Para análisis de distribución:",
  " - Histogramas con línea de densidad roja punteada",
  " - Histogramas marginales (ggExtra::ggMarginal)",
  " - Diagramas de caja (boxplots) con puntos de datos superpuestos",
  " - Gráficos de violín (violin plots) para mostrar densidad y dispersión",
  " - Gráficos de densidad para comparar distribuciones",
  "2. Para comparación de categorías:",
  " - Gráficos de barras con etiquetas de valores",
  " - Gráficos de barras divergentes para mostrar desviaciones",
  " - Gráficos de donut (variación de pie charts con espacio central)",
  " - Gráficos de barras apiladas o agrupadas para múltiples variables",
  " - Waterfall charts para mostrar contribuciones acumulativas",
  " - Bump charts para mostrar cambios en rankings a lo largo del tiempo",
  "3. Para relaciones entre variables:",
  " - Scatterplots con líneas de tendencia y elipses de confianza",
  " - Bubble charts para 3 dimensiones cuantitativas",
  " - Heatmaps para matrices de correlación",
  " - Gráficos de pares (ggpairs) para análisis multivariado",
  " - Dumbell charts para mostrar cambios entre dos puntos en el tiempo",
  "4. Para flujos y composiciones:",
  " - Gráficos Sankey o Alluvial para flujos y procesos",
  " - Diagramas de Venn para superposición de conjuntos",
  "5. Para análisis avanzados:",
  " - Radar charts para comparación de múltiples métricas",
  " - Waterfall charts para análisis financieros",
  " - Series temporales con bandas de confianza",
  "6. Visualizaciones por grupos:",
  " - Boxplots por grupos con puntos de datos",
  " - Histogramas por grupos (usando facet_wrap o facet_grid)",
  "7. Mejores prácticas visuales:",
  " - DEBES usar paletas de colores accesibles como viridis, sequential y/o ColorBrewer",
  " - Asegurar contraste adecuado para daltonismo",
  " - Limitar a 6-8 categorías por gráfico para claridad",
  " - Incluir títulos descriptivos y etiquetas claras",
  " - Usar escalas apropiadas (logarítmicas cuando sea necesario)",
  " - Los heatmaps y correlogramas deben usar escalas de colores como Red-Blue-White Divergente",
  " - Añadir anotaciones para puntos clave cuando sea relevante",
  sep = "\n"
)

construct_prompt <- function(prompt, datos_tabla, table_name) {
  columnas <- paste(colnames(datos_tabla), collapse = ", ")
  sprintf(
    "Actúa como un analista y científico de datos experto en R para análisis de riesgo crediticio, eres un pro que usará 'datos_tabla' para analizar. Tienes acceso total a una tabla llamada 'datos_tabla' con las siguientes columnas: %s y %d filas.\n\n%s\n\n%s\n\nConsulta del usuario: %s\n\nInstrucciones adicionales:\n1. NO SIMULES DATOS; usa exclusivamente 'datos_tabla' y siempre verifica el tipo de datos de las columnas, NO SIMULES DATOS\n2. SIEMPRE crea y asigna un DataFrame llamado 'resultado_df' con los datos relevantes para todos los análisis que deriven de él\n3. SIEMPRE genera una gráfica principal asignándola a 'grafico'\n4. SIEMPRE genera tantas visualizaciones adicionales que creas necesarias para sustentar el análisis y asígnalas a 'graficoX' donde X es el número a partir de 2\n5. Usa las definiciones exactas del diccionario de contexto para los cálculos\n6. No apliques filtros a menos que se soliciten explícitamente\n7. Maneja datos faltantes con na.rm = TRUE\n8. NO comentes ningún código de gráficas\n9. Usa theme_gray() para todas las gráficas\n10. Todas las gráficas deben ser objetos ggplotly y puedes combinar con tidyplots y/o ggplot2 según se adapte mejor al requerimiento analítico (Puedes usar las visualizaciones recomendadas del diccionario de visuales) y deben renderizarse en el carrusel\n11. Asegúrate de que cada gráfica tenga un título descriptivo\n12. Asegúrate de que los gráficos sean lo más estéticos posibles siguiendo las mejores prácticas de visualización del diccionario\n13. Si las gráficas son barplots o similares, asegúrate de que tengan etiquetas de datos arriba de las barras a menos que se indique lo contrario\n14. Si te piden o propones histogramas, deben incluir una línea de densidad en rojo con punteado fino\n15. Al inicio del código, incluye este bloque para manejar paquetes:\n required_packages <- c('dplyr', 'ggplot2', 'caret', 'forecast', 'tidyverse', 'tidyplots', 'plotly', 'tidyr', 'GGally', 'ggforce', 'ggridges', 'ggalt', 'ggExtra', 'ggalluvial', 'waterfalls')\n for(pkg in required_packages) {\n if (!require(pkg, character.only = TRUE)) {\n install.packages(pkg)\n library(pkg, character.only = TRUE)\n }\n }\n16. Añade otros paquetes a required_packages según los necesites para el análisis específico\n17. SIEMPRE genera un análisis estadístico detallado que incluya:\n - PRIMERO crea un dataframe llamado 'estadisticas_numericas' que contenga todas las métricas relevantes\n - LUEGO calcula y almacena todas las correlaciones importantes en 'estadisticas_numericas'\n - DESPUÉS identifica y almacena patrones y anomalías con valores numéricos\n18. SIEMPRE genera conclusiones basadas en datos DESPUÉS de calcular las estadísticas:\n - SOLO DESPUÉS de tener 'estadisticas_numericas' completo, crea un vector de texto llamado 'conclusiones_texto'\n - Cada conclusión DEBE usar valores específicos de 'estadisticas_numericas'\n - Incluye interpretación de visualizaciones con valores numéricos exactos\n19. SIEMPRE genera recomendaciones accionables basadas en las conclusiones:\n - SOLO DESPUÉS de tener 'conclusiones_texto', crea un vector de texto llamado 'recomendaciones_texto'\n - Cada recomendación DEBE basarse en valores específicos de 'estadisticas_numericas'\n - Incluye objetivos numéricos exactos basados en los cálculos realizados\n20. Asegúrate de que todos los dataframes y variables ('resultado_df', 'estadisticas_numericas', 'conclusiones_texto', 'recomendaciones_texto') estén disponibles en el ambiente y contengan valores reales\n21. NUNCA uses print() o message() para mostrar conclusiones, usa las variables definidas\n22. Asegúrate de que todas las variables contengan valores calculados, no referencias o placeholders\n23. NO SIMULES DATOS Y NO INVENTES LAS CONCLUSIONES Y RECOMENDACIONES, CALCULA TODO PRIMERO\n24. IMPORTANTE: Las conclusiones y recomendaciones DEBEN ser vectores de texto que contengan los valores numéricos específicos calculados",
    columnas, nrow(datos_tabla), diccionario_contexto, diccionario_visuales, prompt
  )
}

consultar_ia <- function(prompt, datos_tabla, table_name, provider, model) {
  if (is.null(datos_tabla) || nrow(datos_tabla) == 0) {
    message("No hay datos en la tabla seleccionada para enviar a la IA.")
    return(list(
      texto = sprintf("No hay datos disponibles en '%s' para analizar. Verifica la carga de datos desde BigQuery.", table_name),
      codigo_r = NULL,
      graficas = NULL,
      dataframe = NULL
    ))
  }
  
  columnas <- paste(colnames(datos_tabla), collapse = ", ")
  message("Columnas enviadas a la IA: ", columnas)
  message("Número de filas en ", table_name, ": ", nrow(datos_tabla))
  
  prompt_completo <- construct_prompt(prompt, datos_tabla, table_name)
  
  if (provider == "OpenAI") {
    if (is.null(OPENAI_API_KEY)) {
      return(list(texto = "No se pudo conectar con OpenAI: Llave API no disponible.", codigo_r = NULL, graficas = NULL, dataframe = NULL))
    }
    response <- tryCatch({
      POST(
        url = "https://api.openai.com/v1/chat/completions",
        body = list(
          model = model,
          messages = list(
            list(role = "system", content = "Eres un experto en análisis de datos y visualización en R."),
            list(role = "user", content = prompt_completo)
          ),
          max_tokens = 800,
          temperature = 0.5
        ),
        add_headers(
          "Content-Type" = "application/json",
          "Authorization" = paste("Bearer", OPENAI_API_KEY)
        ),
        encode = "json"
      )
    }, error = function(e) {
      message("Error en la solicitud a OpenAI: ", e$message)
      return(NULL)
    })
    
    if (!is.null(response) && status_code(response) == 429) {
      return(list(
        texto = "Error 429: Demasiadas solicitudes a OpenAI. Por favor, espera unos minutos y vuelve a intentar.",
        codigo_r = NULL,
        graficas = NULL,
        dataframe = NULL
      ))
    }
  } else if (provider == "Gemini") {
    if (is.null(GEMINI_API_KEY)) {
      return(list(texto = "No se pudo conectar con Gemini: Llave API no disponible.", codigo_r = NULL, graficas = NULL, dataframe = NULL))
    }
    response <- tryCatch({
      POST(
        url = sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", model),
        body = list(
          contents = list(list(parts = list(list(text = prompt_completo))))
        ),
        add_headers(
          "Content-Type" = "application/json",
          "x-goog-api-key" = GEMINI_API_KEY
        ),
        encode = "json"
      )
    }, error = function(e) {
      message("Error en la solicitud a Gemini: ", e$message)
      return(NULL)
    })
  } else if (provider == "DeepSeek") {
    if (is.null(DEEPSEEK_API_KEY)) {
      return(list(
        texto = "No se pudo conectar con DeepSeek: Llave API no disponible.",
        codigo_r = NULL,
        graficas = NULL,
        dataframe = NULL
      ))
    }
    response <- tryCatch({
      POST(
        url = "https://api.deepseek.com/v1/chat/completions",
        body = list(
          model = model,
          messages = list(
            list(role = "system", content = "Eres un experto en análisis y ciencia de datos y visualización en R. Utiliza ÚNICAMENTE los datos proporcionados en 'datos_tabla'. NO simules ni generes datos aleatorios bajo ninguna circunstancia., SOLO DEBES ANALIZAR:",datos_tabla),
            list(role = "user", content = prompt_completo)
          ),
          max_tokens = 8000,
          temperature = 0.2,
          presence_penalty = 0.2,
          frequency_penalty = 0.2
        ),
        add_headers(
          "Content-Type" = "application/json",
          "Authorization" = paste("Bearer", DEEPSEEK_API_KEY)
        ),
        encode = "json"
      )
    }, error = function(e) {
      message("Error en la solicitud a DeepSeek: ", e$message)
      return(NULL)
    })
  }
  
  default_response <- list(
    texto = sprintf("Error al conectar con %s. Respuesta por defecto: No se pudo procesar la solicitud.", provider),
    codigo_r = NULL,
    graficas = NULL,
    dataframe = NULL
  )
  
  if (is.null(response) || status_code(response) != 200) {
    message("Respuesta inválida de ", provider, ". Código de estado: ", if (!is.null(response)) status_code(response) else "NULL")
    return(default_response)
  }
  
  if (provider == "OpenAI") {
    respuesta <- content(response, "parsed")$choices[[1]]$message$content
  } else if (provider == "Gemini") {
    respuesta <- content(response, "parsed")$candidates[[1]]$content$parts[[1]]$text
  } else if (provider == "DeepSeek") {
    respuesta <- content(response, "parsed")$choices[[1]]$message$content
  }
  
  message("Respuesta recibida de ", provider, ": ", substr(respuesta, 1, 100), "...")
  
  lineas <- strsplit(respuesta, "\n")[[1]]
  codigo_idx <- grep("```r", lineas, fixed = TRUE)
  fin_codigo_idx <- if (length(codigo_idx) > 0) grep("```", lineas[-(1:codigo_idx[1])], fixed = TRUE)[1] + codigo_idx[1] else NA
  
  if (length(codigo_idx) > 0 && !is.na(fin_codigo_idx)) {
    codigo_r <- paste(lineas[(codigo_idx[1] + 1):(fin_codigo_idx - 1)], collapse = "\n")
  } else {
    codigo_r <- NULL
  }
  
  texto <- paste(lineas[!grepl("```", lineas, fixed = TRUE)], collapse = "\n")
  texto <- trimws(texto)
  
  graficas_idx <- grep("ggplot", lineas, fixed = TRUE)
  graficas <- if (length(graficas_idx) > 0) lineas[graficas_idx] else NULL
  
  return(list(
    texto = texto,
    codigo_r = codigo_r,
    graficas = graficas,
    dataframe = NULL
  ))
}

############## CUSTOM CSS #################################################

custom_css <- "
  body, .content-wrapper, .main-sidebar {
    background-color: #2C3E50 !important;
    color: #ECF0F1 !important;
  }
  .skin-blue .main-header .navbar, .skin-blue .main-header .logo {
    background-color: #34495E !important;
    color: #ECF0F1 !important;
  }
  .box {
    background-color: #34495E !important;
    color: #ECF0F1 !important;
    border-top: 3px solid #3498DB !important;
  }
  .box .box-title {
    color: #FFFFFF !important; /* Asegura que los títulos de los boxes sean blancos */
  }
  .value-box {
    background-color: #34495E !important;
    color: #ECF0F1 !important;
  }
  .btn, .btn-default {
    background-color: #3498DB !important;
    color: #ECF0F1 !important;
    border-color: #2980B9 !important;
  }
  .btn:hover {
    background-color: #2980B9 !important;
  }
  .form-control, select, textarea {
    background-color: #ECF0F1 !important;
    color: #2C3E50 !important;
    border-color: #3498DB !important;
  }
  .dataTable, .dataTable thead th, .dataTable tbody td {
    background-color: #34495E !important;
    color: #FFFFFF !important;
    border-color: #2980B9 !important;
  }
  .dataTables_wrapper .dataTables_length, 
  .dataTables_wrapper .dataTables_filter, 
  .dataTables_wrapper .dataTables_info, 
  .dataTables_wrapper .dataTables_paginate {
    color: #FFFFFF !important;
  }
  .dataTables_wrapper .dataTables_filter input {
    background-color: #ECF0F1 !important;
    color: #2C3E50 !important;
    border: 1px solid #3498DB !important;
  }
  /* Estilo para el contenedor del lengthMenu */
  .dataTables_wrapper .dataTables_length {
    color: #FFFFFF !important; /* Texto 'Show X entries' en blanco */
  }
  /* Estilo para el select en su estado cerrado y desplegado */
  .dataTables_wrapper .dataTables_length select {
    background-color: #34495E !important; /* Fondo oscuro consistente con el tema */
    color: #FFFFFF !important; /* Texto blanco */
    border: 1px solid #2980B9 !important; /* Borde azul oscuro */
    padding: 4px 8px; /* Espaciado interno */
    -webkit-appearance: none; /* Elimina estilo nativo en Webkit */
    -moz-appearance: none; /* Elimina estilo nativo en Firefox */
    appearance: none; /* Elimina estilo nativo */
    text-align: center; /* Centra el texto seleccionado */
    text-align-last: center; /* Centra el texto en el estado cerrado */
  }
  /* Estilo para las opciones del desplegable */
  .dataTables_wrapper .dataTables_length select option {
    background-color: #34495E !important; /* Fondo oscuro para las opciones */
    color: #FFFFFF !important; /* Texto blanco para las opciones */
  }
  /* Hover sobre las opciones */
  .dataTables_wrapper .dataTables_length select option:hover {
    background-color: #2980B9 !important; /* Fondo más oscuro al pasar el ratón */
    color: #FFFFFF !important; /* Texto blanco */
  }
  /* Forzar estilo al abrir el select */
  .dataTables_wrapper .dataTables_length select:focus,
  .dataTables_wrapper .dataTables_length select:active {
    background-color: #34495E !important; /* Mantener fondo al abrir */
    color: #FFFFFF !important; /* Mantener texto blanco */
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button {
    color: #FFFFFF !important;
    background-color: #34495E !important;
    border: 1px solid #2980B9 !important;
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button:hover {
    background-color: #3498DB !important;
    color: #FFFFFF !important;
    border: 1px solid #2980B9 !important;
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button.current {
    background-color: #3498DB !important;
    color: #FFFFFF !important;
    border: 1px solid #2980B9 !important;
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button.disabled {
    color: #ECF0F1 !important;
    background-color: #34495E !important;
    border: 1px solid #2980B9 !important;
    opacity: 0.5;
  }
  .sidebar-menu .treeview-menu {
    background-color: #2C3E50 !important;
  }
  .sidebar-menu li a {
    color: #ECF0F1 !important;
  }
  .sidebar-menu li.active a {
    background-color: #3498DB !important;
  }
  .modal-content {
    background-color: #3498DB !important;
    color: #FFFFFF !important;
  }
  .modal-header, .modal-footer {
    border-color: #2980B9 !important;
  }
  .switches-container {
    display: inline-flex;
    align-items: center;
    margin-left: 10px;
  }
  #ia_ask:disabled {
    background-color: #95A5A6 !important;
    cursor: not-allowed !important;
  }
"

############### UI ###############################################################################




ui <- dashboardPage(
  dashboardHeader(title = "Análisis de Riesgo Crediticio"),
  dashboardSidebar(
    sidebarMenu(
      id = "sidebar_menu",
      menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
      menuItem("Tablas", tabName = "update", icon = icon("table")),
      menuItem("Asistente IA", tabName = "ia_config", icon = icon("robot")),
      menuItem("Actualizar Datos", tabName = "update_actions", icon = icon("sync"))
    ),
    conditionalPanel(
      condition = "input.sidebar_menu == 'dashboard' && input.data_loaded == true",
      conditionalPanel(
        condition = "input.tabset_dashboard == 'Overview' || input.tabset_dashboard == 'Deep-Dive'",
        div(
          id = "filters_container",
          style = "padding: 10px; display: none;",
          h4("Filtros", style = "color: #FFFFFF; margin-bottom: 16px;"),
          div(
            style = "margin-bottom: 10px;",
            dateRangeInput("fecha_rango", "Rango de Fechas:",
                           start = Sys.Date() - 365, end = Sys.Date(),
                           min = Sys.Date() - 365, max = Sys.Date(),
                           language = "es")
          ),
          div(
            style = "margin-bottom: 10px;",
            selectInput("filtro_edad", "Filtrar por Edad:",
                        choices = c("Todas", "A: 18-29", "B: 30-39", "C: 40-49", "D: 50-59", "E: 60+", "0: <18"),
                        selected = "Todas")
          ),
          div(
            style = "margin-bottom: 10px;",
            selectInput("filtro_empleo", "Filtrar por Tipo de Empleo:",
                        choices = c("Todas"),
                        selected = "Todas")
          ),
          div(
            style = "margin-bottom: 10px;",
            selectInput("filtro_estado_civil", "Filtrar por Estado Civil:",
                        choices = c("Todas"),
                        selected = "Todas")
          ),
          div(
            style = "margin-bottom: 10px;",
            selectInput("filtro_puntaje", "Filtrar por Puntaje Crediticio:",
                        choices = c("Todas", "6: Muy Alto", "5: Alto", "4: Medio Alto", "3: Medio Bajo", "2: Bajo", "1: Muy Bajo"),
                        selected = "Todas")
          ),
          div(
            style = "margin-bottom: 10px;",
            selectInput("filtro_ingresos", "Filtrar por Ingresos Anuales:",
                        choices = c("Todas", "6: Muy Alto", "5: Alto", "4: Medio Alto", "3: Medio Bajo", "2: Bajo", "1: Muy Bajo"),
                        selected = "Todas")
          ),
          div(
            style = "margin-bottom: 10px;",
            selectInput("filtro_historial_pagos", "Filtrar por Historial de Pagos:",
                        choices = c("Todas"),
                        selected = "Todas")
          ),
          div(
            style = "margin-bottom: 10px;",
            selectInput("filtro_clusters", "Filtrar por Cluster:",
                        choices = c("Todas"),
                        selected = "Todas")
          )
        )
      )
    )
  ),
  dashboardBody(
    useShinyjs(),
    tags$head(
      tags$style(HTML(paste0(custom_css, "
        .shiny-ace-editor {
          border: 1px solid #34495E;
          background-color: #2C3E50;
        }
        .radar-selector {
          margin-bottom: 15px;
          background-color: #34495E;
          padding: 10px;
          border-radius: 5px;
        }

      "))),
      tags$script(HTML("
        Shiny.addCustomMessageHandler('updateDataLoaded', function(value) {
          Shiny.setInputValue('data_loaded', value);
        });
      ")),
      tags$style(HTML("
        .datepicker table {
          background-color: #2C3E50; /* Fondo del calendario */
          color: #FFFFFF; /* Color del texto */
        }
        .datepicker table tr td.day {
          font-size: 14px; /* Tamaño de fuente de los días */
          color: #FFFFFF; /* Color de los números */
        }
        .datepicker table tr td.day:hover {
          background-color: #3498DB; /* Fondo al pasar el mouse */
          color: #FFFFFF; /* Color del texto al pasar el mouse */
        }
        .datepicker table tr td.active {
          background-color: #2ECC71 !important; /* Fondo del día seleccionado */
          color: #FFFFFF !important; /* Color del texto del día seleccionado */
        }
        .datepicker table tr td.today {
          background-color: #E74C3C !important; /* Fondo del día actual */
          color: #FFFFFF !important; /* Color del texto del día actual */
        }
        .datepicker table tr td.disabled {
          color: #7F8C8D !important; /* Color de los días deshabilitados */
        }
      "))
    ),
    tabItems(
      tabItem(
        tabName = "dashboard",
        tabsetPanel(
          id = "tabset_dashboard",
          tabPanel(
            "Overview",
            br(),
            fluidRow(
              valueBoxOutput("vb_aprobadas", width = 4),
              valueBoxOutput("vb_rechazadas", width = 4),
              valueBoxOutput("vb_tasa", width = 4)
            ),
            fluidRow(
              box(
                title = "Tendencias Mensuales",
                width = 12,
                plotlyOutput("plot_tendencias", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Tendencia de Tasa de Aprobación",
                width = 12,
                plotlyOutput("plot_tasa_aprobacion", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Tasa de Aprobación por Edad y Tipo de Empleo",
                width = 12,
                plotlyOutput("heatmap_edad_empleo", height = "400px")
              ),
              box(
                title = "Tasa de Aprobación por Estado Civil y Tipo de Empleo",
                width = 12,
                plotlyOutput("heatmap_estado_civil_empleo", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Tasa de Aprobación por Puntaje Crediticio e Ingresos Anuales",
                width = 12,
                plotlyOutput("heatmap_puntaje_ingresos", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Datos Filtrados",
                width = 12,
                DTOutput("table_data")
              )
            )
          ),
          tabPanel(
            "Deep-Dive",
            br(),
            fluidRow(
              valueBoxOutput("vb_pendientes_predichas", width = 4),
              valueBoxOutput("vb_aprobadas_predichas", width = 4),
              valueBoxOutput("vb_tasa_predicha", width = 4)
            ),
            fluidRow(
              box(
                title = "Tendencias Semanales (Predicciones)",
                width = 12,
                plotlyOutput("plot_tendencias_semanal_deep", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Tendencia de Tasa de Aprobación Semanal (Predicciones)",
                width = 12,
                plotlyOutput("plot_tasa_aprobacion_semanal_deep", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Tasa de Aprobación por Edad y Tipo de Empleo (Predicciones)",
                width = 12,
                plotlyOutput("heatmap_edad_empleo_deep", height = "400px")
              ),
              box(
                title = "Tasa de Aprobación por Estado Civil y Tipo de Empleo (Predicciones)",
                width = 12,
                plotlyOutput("heatmap_estado_civil_empleo_deep", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Tasa de Aprobación por Puntaje Crediticio e Ingresos Anuales (Predicciones)",
                width = 12,
                plotlyOutput("heatmap_puntaje_ingresos_deep", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Clusters: Media de Ingresos Anuales y Edad con Tasa de Aprobación",
                width = 12,
                plotlyOutput("bubble_clusters_mean", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Distribución de Clusters por Puntaje Crediticio y Deuda Actual",
                width = 12,
                plotlyOutput("scatter_clusters_puntaje_deuda", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Clusters: Media de Puntaje Crediticio y Deuda Actual con Tasa de Aprobación",
                width = 12,
                plotlyOutput("bubble_clusters_puntaje_deuda", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Distribución de Variables Numéricas por Cluster",
                width = 12,
                uiOutput("boxplots_variables_numericas")
              )
            ),
            fluidRow(
              box(
                title = "Tasa de Aprobación por Cluster",
                width = 12,
                plotlyOutput("bar_tasa_aprobacion", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Perfiles de Clusters (Tabla)",
                width = 12,
                tableOutput("perfiles_clusters")
              )
            ),
            fluidRow(
              box(
                title = "Perfil del Cluster",
                width = 12,
                div(
                  class = "radar-selector",
                  fluidRow(
                    column(
                      width = 6,
                      selectInput(
                        "cluster_seleccionado", 
                        "Seleccionar Cluster:",
                        choices = NULL,
                        selected = NULL,
                        width = "100%"
                      )
                    ),
                    column(
                      width = 6,
                      helpText(
                        "Seleccione un cluster para visualizar su perfil característico",
                        style = "color: #ECF0F1; padding-top: 5px;"
                      )
                    )
                  )
                ),
                plotlyOutput("radar_clusters", height = "500px")
              )
            ),
            fluidRow(
              box(
                title = "Datos Filtrados (Pendientes Predichas)",
                width = 12,
                div(
                  style = "margin-bottom: 10px;",
                  selectInput("filtro_prediccion", "Filtrar por Predicción:",
                              choices = c("Todas", "Aprobadas", "Rechazadas"),
                              selected = "Todas")
                ),
                DTOutput("table_data_deep")
              )
            )
          ),
          tabPanel(
            "Forecast",
            fluidRow(
              box(
                title = "Pronóstico de Solicitudes de Crédito",
                width = 12,
                plotlyOutput("forecast_plot_solicitudes", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Pronóstico de Aprobaciones",
                width = 12,
                plotlyOutput("forecast_plot_aprobaciones", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Pronóstico de Tasa de Aprobación",
                width = 12,
                plotlyOutput("forecast_plot_tasa", height = "400px")
              )
            ),
            fluidRow(
              box(
                title = "Datos de Pronósticos (Últimos 18 Meses)",
                width = 12,
                DTOutput("forecast_table")
              )
            )
          )
        )
      ),
      tabItem(
        tabName = "update",
        fluidRow(
          box(
            title = "Lista de Tablas",
            width = 12,
            selectInput("table_selector", "Seleccionar Tabla:", choices = NULL),
            DTOutput("table_list")
          )
        )
      ),
      tabItem(
        tabName = "ia_config",
        fluidRow(
          box(
            title = "Configuración de IA",
            width = 12,
            uiOutput("ai_status"),
            selectInput("ia_provider", "Proveedor de IA:", choices = c("OpenAI", "Gemini", "DeepSeek")),
            uiOutput("model_selector"),
            selectInput("ia_table", "Tabla a Analizar:", choices = NULL),
            textOutput("ia_table_columns"),
            br(),
            textAreaInput("ia_prompt", "Haz tu pregunta sobre los datos:", rows = 10),
            textOutput("analysis_message"),
            actionButton("ex1", "Tasa de Aprobación"),
            actionButton("ex2", "Clusters"),
            actionButton("ex3", "Forecast"),
            actionButton("ex4", "Heatmap"),
            actionButton("ex5", "Análisis de Rechazos"),
            actionButton("ia_ask", "Consultar", icon = icon("search")),
            uiOutput("ia_timer"),
            br(),
            aceEditor(
              outputId = "ia_response_text",
              value = "Esperando consulta o análisis...",
              mode = "r",
              theme = "monokai",
              readOnly = TRUE,
              height = "700px"
            ),
            br(),
            DTOutput("ia_response_df"),
            br(),
            uiOutput("ia_response_carousel")
          )
        )
      ),
      tabItem(
        tabName = "update_actions",
        fluidRow(
          box(
            title = "Actualización de Datos",
            width = 12,
            actionButton("btn_update_tables", "Actualizar Tablas", icon = icon("sync")),
            actionButton("btn_retrain_models", "Reentrenar Modelos", icon = icon("cogs")),
            actionButton("btn_reload_maestra", "Recargar Tabla Maestra", icon = icon("refresh")),
            br(),
            br(),
            textOutput("etl_status")
          )
        )
      )
    )
  )
)



################## SERVER #########################################################################

server <- function(input, output, session) {
  # ReactiveValues para el resto de la app
  rv <- reactiveValues(
    datos_tabla = NULL,
    selected_table = "solicitudes_maestra",
    last_update = NULL,
    tables = NULL,
    data_loaded = FALSE
  )
  
  # ReactiveValues independiente para la IA
  rv_ia <- reactiveValues(
    datos_tabla = NULL,
    selected_table = "solicitudes_maestra",
    ia_codigo_r = NULL,
    ia_dataframe = NULL,
    ia_visualizations = NULL,
    openai_online = FALSE,
    gemini_online = FALSE,
    deepseek_online = FALSE,
    last_ia_query_time = NULL,
    countdown = 0,
    plot_theme = "Oscuro",
    data_loaded = FALSE,
    tables = NULL,
    initialized = FALSE
  )
  
  # ReactiveValues para Forecast
  rv_forecast <- reactiveValues(
    datos_forecast = NULL,
    data_loaded = FALSE
  )
  
  # Función para simular la acción del botón "Actualizar Tablas"
  update_tables <- function() {
    withProgress(message = "Actualizando tablas...", value = 0, {
      incProgress(0.2)
      update_aggregated_table()
      incProgress(0.4)
      update_master_table()
      incProgress(0.6)
      rv$tables <- list_tables()
      incProgress(0.8)
      rv$last_update <- Sys.time()
      output$etl_status <- renderText({
        paste("Tablas actualizadas con éxito en:", rv$last_update)
      })
      incProgress(1.0)
    })
  }
  
  # Carga inicial consolidada con modal
  observe({
    showModal(modalDialog(
      title = "Inicializando Aplicación",
      "Por favor, espera mientras se cargan los datos...",
      footer = NULL,
      easyClose = FALSE,
      size = "m",
      tags$div(
        style = "text-align: center;",
        tags$img(src = "https://i.gifer.com/ZZ5H.gif", height = "40px")
      )
    ))
    
    withProgress(message = "Cargando datos iniciales...", value = 0, {
      incProgress(0.1, detail = "Actualizando tablas...")
      update_tables()
      
      incProgress(0.3, detail = "Cargando tabla maestra...")
      if ("solicitudes_maestra" %in% rv$tables) {
        rv$datos_tabla <- load_table_data("solicitudes_maestra")
        rv$selected_table <- "solicitudes_maestra"
        if (!is.null(rv$datos_tabla) && "fecha_solicitud" %in% names(rv$datos_tabla)) {
          rv$datos_tabla$fecha_solicitud <- as.Date(rv$datos_tabla$fecha_solicitud)
        }
        rv$data_loaded <- !is.null(rv$datos_tabla) && nrow(rv$datos_tabla) > 0
        if (rv$data_loaded) {
          showNotification(paste("Datos cargados de 'solicitudes_maestra':", nrow(rv$datos_tabla), "filas"), type = "message")
          message("Columnas disponibles en rv$datos_tabla: ", paste(names(rv$datos_tabla), collapse = ", ", "\n\n\n"))
          message("Filas en rv$datos_tabla: ", nrow(rv$datos_tabla))
        } else {
          showNotification("No se cargaron datos de 'solicitudes_maestra'.", type = "error", duration = 10)
        }
      } else {
        rv$datos_tabla <- data.frame()
        rv$data_loaded <- FALSE
        showNotification("La tabla 'solicitudes_maestra' no existe en BigQuery.", type = "error", duration = 10)
      }
      
      incProgress(0.5, detail = "Inicializando IA...")
      rv_ia$tables <- rv$tables
      rv_ia$datos_tabla <- rv$datos_tabla
      rv_ia$data_loaded <- rv$data_loaded
      rv_ia$openai_online <- check_openai_status()
      rv_ia$gemini_online <- check_gemini_status()
      rv_ia$deepseek_online <- check_deepseek_status()
      rv_ia$initialized <- TRUE
      
      if (!rv_ia$openai_online) showNotification("OpenAI no está disponible.", type = "warning", duration = 10)
      if (!rv_ia$gemini_online) showNotification("Gemini no está disponible.", type = "warning", duration = 10)
      if (!rv_ia$deepseek_online) showNotification("DeepSeek no está disponible.", type = "warning", duration = 10)
      
      incProgress(0.7, detail = "Cargando datos de forecast...")
      if ("forecast_combinado" %in% rv$tables) {
        rv_forecast$datos_forecast <- load_table_data("forecast_combinado")
        rv_forecast$data_loaded <- !is.null(rv_forecast$datos_forecast) && nrow(rv_forecast$datos_forecast) > 0
        if (rv_forecast$data_loaded) {
          message("Datos de forecast_combinado cargados. Filas: ", nrow(rv_forecast$datos_forecast))
        } else {
          showNotification("No se pudieron cargar datos de 'forecast_combinado'.", type = "warning")
        }
      } else {
        rv_forecast$datos_forecast <- data.frame()
        rv_forecast$data_loaded <- FALSE
        showNotification("La tabla 'forecast_combinado' no existe en BigQuery.", type = "error")
      }
      
      incProgress(0.9, detail = "Configurando interfaz...")
      updateSelectInput(session, "table_selector", choices = rv$tables, selected = "solicitudes_maestra")
      updateSelectInput(session, "ia_table", choices = rv_ia$tables, selected = "solicitudes_maestra")
      
      updateAceEditor(
        session,
        "ia_response_text",
        value = "Esperando consulta o análisis..."
      )
      
      incProgress(1.0, detail = "Finalizando...")
    })
    
    removeModal()
    
    if (rv$data_loaded) {
      runjs("document.getElementById('filters_container').style.display = 'block';")
    }
    
    session$sendCustomMessage("updateDataLoaded", rv$data_loaded)
  })
  
  # Actualizar los filtros dinámicamente
  observe({
    req(rv$data_loaded)
    req(rv$datos_tabla)
    req(nrow(rv$datos_tabla) > 0)
    
    min_date <- min(rv$datos_tabla$fecha_solicitud, na.rm = TRUE)
    max_date <- max(rv$datos_tabla$fecha_solicitud, na.rm = TRUE)
    
    if (is.na(min_date) || is.na(max_date)) {
      showNotification("No se encontraron fechas válidas en el dataset", type = "error")
      return()
    }
    
    updateDateRangeInput(session, "fecha_rango",
                         start = min_date, end = max_date,
                         min = min_date, max = max_date)
    
    tipos_empleo <- c("Todas", unique(rv$datos_tabla$tipo_empleo[!is.na(rv$datos_tabla$tipo_empleo)]))
    estados_civiles <- c("Todas", unique(rv$datos_tabla$estado_civil[!is.na(rv$datos_tabla$estado_civil)]))
    puntaje_buckets <- c("6: Muy Alto", "5: Alto", "4: Medio Alto", "3: Medio Bajo", "2: Bajo", "1: Muy Bajo")
    ingresos_buckets <- c("6: Muy Alto", "5: Alto", "4: Medio Alto", "3: Medio Bajo", "2: Bajo", "1: Muy Bajo")
    historial_pagos <- c("Todas", unique(rv$datos_tabla$historial_pagos[!is.na(rv$datos_tabla$historial_pagos)]))
    clusters <- c("Todas", sort(unique(na.omit(rv$datos_tabla$cluster))))
    
    updateSelectInput(session, "filtro_edad",
                      choices = c("Todas", "A: 18-29", "B: 30-39", "C: 40-49", "D: 50-59", "E: 60+", "0: <18"),
                      selected = "Todas")
    updateSelectInput(session, "filtro_empleo",
                      choices = tipos_empleo,
                      selected = "Todas")
    updateSelectInput(session, "filtro_estado_civil",
                      choices = estados_civiles,
                      selected = "Todas")
    updateSelectInput(session, "filtro_puntaje",
                      choices = c("Todas", puntaje_buckets),
                      selected = "Todas")
    updateSelectInput(session, "filtro_ingresos",
                      choices = c("Todas", ingresos_buckets),
                      selected = "Todas")
    updateSelectInput(session, "filtro_historial_pagos",
                      choices = historial_pagos,
                      selected = "Todas")
    updateSelectInput(session, "filtro_clusters",
                      choices = clusters,
                      selected = "Todas")
  })
  
  # Reactive para datos filtrados
  datos_filtrados <- reactive({
    req(rv$data_loaded)
    datos <- rv$datos_tabla
    if (is.null(datos) || nrow(datos) == 0) return(NULL)
    
    # Filtro por rango de fechas
    datos <- datos %>%
      filter(fecha_solicitud >= as.Date(input$fecha_rango[1]) & fecha_solicitud <= as.Date(input$fecha_rango[2]))
    
    # Filtro por edad
    if (input$filtro_edad != "Todas") {
      if (input$filtro_edad == "0: <18") {
        datos <- datos %>% filter(edad < 18)
      } else if (input$filtro_edad == "A: 18-29") {
        datos <- datos %>% filter(edad >= 18 & edad <= 29)
      } else if (input$filtro_edad == "B: 30-39") {
        datos <- datos %>% filter(edad >= 30 & edad <= 39)
      } else if (input$filtro_edad == "C: 40-49") {
        datos <- datos %>% filter(edad >= 40 & edad <= 49)
      } else if (input$filtro_edad == "D: 50-59") {
        datos <- datos %>% filter(edad >= 50 & edad <= 59)
      } else if (input$filtro_edad == "E: 60+") {
        datos <- datos %>% filter(edad >= 60)
      }
    }
    
    # Filtro por tipo de empleo
    if (input$filtro_empleo != "Todas") {
      datos <- datos %>% filter(tipo_empleo == input$filtro_empleo)
    }
    
    # Filtro por estado civil
    if (input$filtro_estado_civil != "Todas") {
      datos <- datos %>% filter(estado_civil == input$filtro_estado_civil)
    }
    
    # Filtro por puntaje crediticio
    if (input$filtro_puntaje != "Todas") {
      if (input$filtro_puntaje == "1: Muy Bajo") {
        datos <- datos %>% filter(puntaje_crediticio >= 300 & puntaje_crediticio <= 499)
      } else if (input$filtro_puntaje == "2: Bajo") {
        datos <- datos %>% filter(puntaje_crediticio >= 500 & puntaje_crediticio <= 599)
      } else if (input$filtro_puntaje == "3: Medio Bajo") {
        datos <- datos %>% filter(puntaje_crediticio >= 600 & puntaje_crediticio <= 649)
      } else if (input$filtro_puntaje == "4: Medio Alto") {
        datos <- datos %>% filter(puntaje_crediticio >= 650 & puntaje_crediticio <= 699)
      } else if (input$filtro_puntaje == "5: Alto") {
        datos <- datos %>% filter(puntaje_crediticio >= 700 & puntaje_crediticio <= 749)
      } else if (input$filtro_puntaje == "6: Muy Alto") {
        datos <- datos %>% filter(puntaje_crediticio >= 750 & puntaje_crediticio <= 850)
      }
    }
    
    # Filtro por ingresos anuales
    if (input$filtro_ingresos != "Todas") {
      if (input$filtro_ingresos == "1: Muy Bajo") {
        datos <- datos %>% filter(ingresos_anuales >= 0 & ingresos_anuales <= 19999)
      } else if (input$filtro_ingresos == "2: Bajo") {
        datos <- datos %>% filter(ingresos_anuales >= 20000 & ingresos_anuales <= 39999)
      } else if (input$filtro_ingresos == "3: Medio Bajo") {
        datos <- datos %>% filter(ingresos_anuales >= 40000 & ingresos_anuales <= 59999)
      } else if (input$filtro_ingresos == "4: Medio Alto") {
        datos <- datos %>% filter(ingresos_anuales >= 60000 & ingresos_anuales <= 79999)
      } else if (input$filtro_ingresos == "5: Alto") {
        datos <- datos %>% filter(ingresos_anuales >= 80000 & ingresos_anuales <= 99999)
      } else if (input$filtro_ingresos == "6: Muy Alto") {
        datos <- datos %>% filter(ingresos_anuales >= 100000)
      }
    }
    
    # Filtro por historial de pagos
    if (input$filtro_historial_pagos != "Todas") {
      datos <- datos %>% filter(historial_pagos == input$filtro_historial_pagos)
    }
    
    # Filtro por clusters
    if (input$filtro_clusters != "Todas") {
      datos <- datos %>% filter(cluster == as.numeric(input$filtro_clusters))
    }
    
    return(datos)
  })
  
  
  
  ###################### IA #############################################################
  
  # Selector de columnas para filtrar en búsqueda
  output$ia_column_selector <- renderUI({
    if (!is.null(rv_ia$ia_dataframe) && nrow(rv_ia$ia_dataframe) > 0) {
      selectInput("ia_search_column", "Filtrar en la columna:", 
                  choices = c("Todas", names(rv_ia$ia_dataframe)), 
                  selected = "Todas")
    } else {
      NULL
    }
  })
  
  # Cargar datos cuando se selecciona una tabla
  observeEvent(input$ia_table, {
    req(input$ia_table)
    withProgress(message = sprintf("Cargando '%s' para IA...", input$ia_table), value = 0, {
      if (rv_ia$selected_table != input$ia_table) {
        updateAceEditor(
          session,
          "ia_response_text",
          value = "Esperando consulta o análisis..."
        )
        rv_ia$ia_dataframe <- NULL
        
        output$ia_response_df <- renderDT({
          if (!is.null(rv_ia$ia_dataframe) && nrow(rv_ia$ia_dataframe) > 0) {
            dt <- datatable(
              rv_ia$ia_dataframe,
              options = list(
                dom = "lrtipB",
                buttons = c("copy", "csv", "excel", "pdf", "print"),
                pageLength = 10,
                lengthMenu = c(5, 10, 25, 50),
                searchHighlight = FALSE,
                autoWidth = FALSE,
                scrollX = TRUE,
                columnDefs = list(list(className = "dt-center", targets = "_all")),
                # Configurar búsqueda en columna específica
                searchCols = if (!is.null(input$ia_search_column) && input$ia_search_column != "Todas") {
                  lapply(names(rv_ia$ia_dataframe), function(col) {
                    if (col == input$ia_search_column) NULL else list(search = "")
                  })
                } else {
                  NULL
                }
              ),
              rownames = FALSE,
              class = "display",
              extensions = "Buttons",
              style = "bootstrap",
              caption = "Resultado del Análisis de IA"
            ) %>%
              formatStyle(
                columns = names(rv_ia$ia_dataframe),
                backgroundColor = "#34495E",
                color = "#FFFFFF",
                fontSize = "12px"
              ) %>%
              formatCurrency(columns = intersect(names(rv_ia$ia_dataframe), c("ingresos_anuales", "deuda_actual")), currency = "$", digits = 0) %>%
              formatDate(columns = intersect(names(rv_ia$ia_dataframe), c("fecha_solicitud", "fecha_mes")), method = "toLocaleDateString")
            dt
          } else {
            datatable(
              data.frame(Mensaje = "No se ha generado un DataFrame"),
              options = list(dom = "t", pageLength = 5),
              rownames = FALSE
            )
          }
        })
        
        output$ia_response_plot <- renderPlotly({
          plotly_empty() %>% layout(
            title = "",
            paper_bgcolor = ifelse(rv_ia$plot_theme == "Oscuro", "#34495E", "#FFFFFF"),
            plot_bgcolor = ifelse(rv_ia$plot_theme == "Oscuro", "#34495E", "#FFFFFF"),
            font = list(color = ifelse(rv_ia$plot_theme == "Oscuro", "#FFFFFF", "#000000"))
          )
        })
        
        rv_ia$selected_table <- input$ia_table
        rv_ia$data_loaded <- FALSE
        rv_ia$datos_tabla <- NULL
        
        datos <- load_table_data(input$ia_table)
        if (!is.null(datos) && "fecha_solicitud" %in% names(datos)) {
          datos$fecha_solicitud <- as.Date(datos$fecha_solicitud)
        }
        rv_ia$datos_tabla <- datos
        rv_ia$data_loaded <- !is.null(datos) && nrow(datos) > 0
        
        if (!rv_ia$data_loaded) {
          showNotification(sprintf("No se pudieron cargar datos de '%s'.", input$ia_table), type = "error")
          updateAceEditor(
            session,
            "ia_response_text",
            value = sprintf("No se pudieron cargar datos de '%s'.", input$ia_table)
          )
        } else {
          message("Datos cargados para IA desde ", input$ia_table, ". Filas: ", nrow(rv_ia$datos_tabla))
          updateAceEditor(
            session,
            "ia_response_text",
            value = sprintf("Datos cargados para IA desde '%s'. Filas: %d", input$ia_table, nrow(rv_ia$datos_tabla))
          )
        }
      }
    })
  })
  
  # Estado de carga de datos para IA
  output$data_loading_status <- renderUI({
    if (rv_ia$data_loaded && !is.null(rv_ia$datos_tabla)) {
      tags$div(sprintf("Datos de '%s' cargados exitosamente (%d filas).", 
                       rv_ia$selected_table, nrow(rv_ia$datos_tabla)),
               style = "color: #2ECC71; font-weight: bold;")
    } else {
      tags$div(sprintf("Sin datos cargados para '%s'.", input$ia_table %||% "ninguna tabla"),
               style = "color: #E74C3C; font-weight: bold;")
    }
  })
  
  # Mostrar columnas disponibles para IA
  output$ia_table_columns <- renderText({
    if (!rv_ia$data_loaded || is.null(rv_ia$datos_tabla) || nrow(rv_ia$datos_tabla) == 0) {
      sprintf("No hay datos cargados para '%s'.", input$ia_table %||% "ninguna tabla")
    } else {
      sprintf("Columnas disponibles en '%s' (%d filas): %s", 
              rv_ia$selected_table, 
              nrow(rv_ia$datos_tabla), 
              paste(colnames(rv_ia$datos_tabla), collapse = ", "),"\n")
    }
  })
  
  # Control del botón de consulta
  observe({
    if (!rv_ia$data_loaded || rv_ia$countdown > 0) {
      shinyjs::disable("ia_ask")
    } else {
      shinyjs::enable("ia_ask")
    }
  })
  
  output$ai_status <- renderUI({
    tagList(
      tags$span("OpenAI: ", style = "font-weight: bold;"),
      tags$span(ifelse(rv_ia$openai_online, "En línea", "No disponible"),
                style = sprintf("color: %s;", ifelse(rv_ia$openai_online, "#2ECC71", "#E74C3C"))),
      tags$br(),
      tags$span("Gemini: ", style = "font-weight: bold;"),
      tags$span(ifelse(rv_ia$gemini_online, "En línea", "No disponible"),
                style = sprintf("color: %s;", ifelse(rv_ia$gemini_online, "#2ECC71", "#E74C3C"))),
      tags$br(),
      tags$span("DeepSeek: ", style = "font-weight: bold;"),
      tags$span(ifelse(rv_ia$deepseek_online, "En línea", "No disponible"),
                style = sprintf("color: %s;", ifelse(rv_ia$deepseek_online, "#2ECC71", "#E74C3C")))
    )
  })
  
  output$model_selector <- renderUI({
    req(input$ia_provider)
    models <- if (input$ia_provider == "OpenAI") OPENAI_MODELS else if (input$ia_provider == "Gemini") GEMINI_MODELS else DEEPSEEK_MODELS
    selectInput("ia_model", "Modelo:", choices = models, selected = models[1])
  })
  
  output$analysis_message <- renderText({
    req(input$ia_table, input$ia_provider, input$ia_model)
    sprintf("La tabla '%s' se estará analizando con %s y el modelo %s.", input$ia_table, input$ia_provider, input$ia_model)
  })
  
  observe({
    invalidateLater(1000, session)
    if (!is.null(rv_ia$last_ia_query_time)) {
      elapsed <- as.numeric(difftime(Sys.time(), rv_ia$last_ia_query_time, units = "secs"))
      rv_ia$countdown <- max(30 - elapsed, 0)
      if (rv_ia$countdown <= 0) {
        rv_ia$last_ia_query_time <- NULL
        rv_ia$countdown <- 0
      }
    }
  })
  
  output$ia_timer <- renderUI({
    if (rv_ia$countdown > 0) {
      tags$div(sprintf("Espera %d segundos para la próxima consulta", ceiling(rv_ia$countdown)),
               style = "color: #E67E22; font-weight: bold;")
    } else {
      NULL
    }
  })
  
  # Theme Switch Logic
  observeEvent(input$theme_switch, {
    rv_ia$plot_theme <- ifelse(input$theme_switch, "Oscuro", "Claro")
  })
  
  # Example Queries for AI Assistant
  observeEvent(input$ex1, {
    updateTextAreaInput(session, "ia_prompt", value = 'Necesito una gráfica de línea con la tendencia agrupados mes a mes para los últimos 12 meses, en donde pueda ver la tasa de aprobaciones de los casos evaluados (Porcentaje con 1 decimal).')
  })
  
  observeEvent(input$ex2, {
    updateTextAreaInput(session, "ia_prompt", value = 'Genera un gráfico de dispersión que muestre en el eje X el promedio de edad y en el eje Y el promedio de ingresos anuales, con el tamaño de los puntos representando la tasa de aprobación evaluada, en porcentaje con un decimal), agrupados en clusters con colores distintos y una leyenda, incluyendo una escala de tamaños, un título descriptivo, etiquetas en los ejes, una cuadrícula opcional, asegurando puntos proporcionales y claros.')
  })
  
  observeEvent(input$ex3, {
    updateTextAreaInput(session, "ia_prompt", value = "Deseo un pronóstico utilizando ARIMA para la tasa de aprobación porcentual de casos evaluados durante los próximos 6 meses, comenzando desde marzo de 2025, basado en datos de 48 meses previos. Los datos deben estar agrupados por el inicio de cada mes. El resultado debe incluir primero el histórico de los últimos 12 meses, seguido del pronóstico de 6 meses, con intervalos de confianza al 95%. Todo debe representarse en un gráfico claro que muestre la tendencia junto con los intervalos de confianza. Además, el dataframe resultante debe tener una columna que indique si cada dato es histórico o pronosticado, incluyendo los intervalos de confianza para los valores pronosticados.")
  })
  
  observeEvent(input$ex4, {
    updateTextAreaInput(session, "ia_prompt", value = "Genera un heatmap de la tasa de aprobación evaluada, en porcentaje con un decimal, agrupados por historial de pago versus la segmentación de la deuda actual (6 segmentos basados en el máximo y el mínimo) señalando los rangos de valores de la deuda.")
  })
  
  observeEvent(input$ex5, {
    updateTextAreaInput(session, "ia_prompt", value = "Deseo saber la razón o las razones principales por las que se rechazaron las solicitudes evaluadas en febrero de 2025.  Cuál es el perfil de los clientes que fueron mayormente rechazados")
  })
  
  observeEvent(input$ia_ask, {
    req(input$ia_prompt, input$ia_provider, input$ia_model, rv_ia$data_loaded)
    if (!rv_ia$data_loaded) {
      showNotification("Los datos de la tabla no están cargados aún.", type = "warning")
      return()
    }
    if (rv_ia$countdown > 0) {
      showNotification("Espera a que termine el conteo para realizar otra consulta.", type = "warning")
      return()
    }
    
    showModal(modalDialog(title = "Analizando datos", "Procesando tu solicitud...", footer = NULL))
    
    respuesta <- tryCatch({
      consultar_ia(input$ia_prompt, rv_ia$datos_tabla, rv_ia$selected_table, input$ia_provider, input$ia_model)
    }, error = function(e) {
      list(
        texto = paste("Error al procesar la consulta:", e$message),
        codigo_r = NULL,
        graficas = NULL,
        dataframe = NULL
      )
    })
    
    removeModal()
    
    rv_ia$last_ia_query_time <- Sys.time()
    rv_ia$countdown <- 30
    
    if (!is.null(respuesta$codigo_r)) {
      rv_ia$ia_codigo_r <- respuesta$codigo_r
      message("Código R recibido: ", respuesta$codigo_r)
      
      resultado <- tryCatch({
        datos_tabla <- rv_ia$datos_tabla
        output_text <- capture.output({
          eval(parse(text = respuesta$codigo_r))
        })
        
        texto_extra <- if (length(output_text) > 0) paste(output_text, collapse = "\n") else ""
        
        if (exists("resultado_df")) {
          rv_ia$ia_dataframe <- resultado_df
        } else {
          rv_ia$ia_dataframe <- NULL
        }
        
        # Dynamically populate the list of visualizations based on the number of graphs
        if (exists("grafico")) {
          rv_ia$ia_visualizations <- mget(ls(pattern = "^grafico[0-9]*$"))
        } else {
          rv_ia$ia_visualizations <- NULL
        }
        
        texto_extra
      }, error = function(e) {
        paste("Error al ejecutar el código: ", e$message)
      })
      
      texto_final <- paste(respuesta$texto, resultado, sep = "\n")
    } else {
      texto_final <- respuesta$texto
      rv_ia$ia_dataframe <- NULL
      rv_ia$ia_visualizations <- NULL
    }
    
    updateAceEditor(
      session,
      "ia_response_text",
      value = texto_final,
      theme = ifelse(rv_ia$plot_theme == "Oscuro", "monokai", "chrome")
    )
    
    # Renderizar el DataFrame en la interfaz
    output$ia_response_df <- renderDT({
      if (!is.null(rv_ia$ia_dataframe) && nrow(rv_ia$ia_dataframe) > 0) {
        datatable(
          rv_ia$ia_dataframe,
          options = list(
            dom = "lrtipB",
            buttons = c("copy", "csv", "excel", "pdf", "print"),
            pageLength = 10,
            lengthMenu = c(5, 10, 25, 50),
            autoWidth = TRUE,
            scrollX = TRUE
          ),
          rownames = FALSE,
          class = "display",
          extensions = "Buttons"
        )
      } else {
        datatable(data.frame(Mensaje = "No se generó un DataFrame"), options = list(dom = "t"))
      }
    })
    
    # Renderizar el carrusel de gráficas
    output$ia_response_carousel <- renderUI({
      req(rv_ia$ia_visualizations)
      plot_list <- lapply(seq_along(rv_ia$ia_visualizations), function(i) {
        plotlyOutput(paste0("grafico", i), height = "500px")
      })
      do.call(tagList, plot_list)
    })
    
    observe({
      req(rv_ia$ia_visualizations)
      lapply(seq_along(rv_ia$ia_visualizations), function(i) {
        local({
          plot_data <- rv_ia$ia_visualizations[[i]]
          output[[paste0("grafico", i)]] <- renderPlotly({
            plot_data
          })
        })
      })
    })
    
    # Reactive value para almacenar las conclusiones y recomendaciones
    rv_ia$conclusiones <- reactiveVal(NULL)

    # Función para generar conclusiones basadas en datos
    generar_conclusiones <- function(datos_tabla, codigo_r) {
      # Evaluar el código R para obtener los resultados
      tryCatch({
        # Crear un nuevo ambiente para evaluar el código
        env <- new.env()
        # Asignar los datos al ambiente
        env$datos_tabla <- datos_tabla
        # Evaluar el código R en el ambiente
        eval(parse(text = codigo_r), envir = env)
        
        # Obtener los resultados del análisis
        resultado_df <- env$resultado_df
        
        # Generar conclusiones basadas en los datos reales
        conclusiones <- list(
          analisis = capture.output(print(resultado_df)),
          estadisticas = env$estadisticas_numericas,
          conclusiones = env$conclusiones_texto,
          recomendaciones = env$recomendaciones_texto
        )
        
        return(conclusiones)
      }, error = function(e) {
        return(list(
          analisis = "Error al procesar los datos",
          estadisticas = NULL,
          conclusiones = NULL,
          recomendaciones = NULL
        ))
      })
    }

    observeEvent(rv_ia$ia_visualizations, {
      if (!is.null(rv_ia$ia_visualizations)) {
        # Generar conclusiones basadas en los datos
        conclusiones <- generar_conclusiones(rv_ia$datos_tabla, rv_ia$ia_codigo_r)
        rv_ia$conclusiones(conclusiones)
        
        # Actualizar el editor con el código y las conclusiones
        texto_completo <- paste(
          texto_final,
          "\nGráficas generadas y renderizadas exitosamente.",
          "\n\nANÁLISIS ESTADÍSTICO:",
          paste(conclusiones$analisis, collapse = "\n"),
          "\n\nCONCLUSIONES:",
          paste(conclusiones$conclusiones, collapse = "\n"),
          "\n\nRECOMENDACIONES:",
          paste(conclusiones$recomendaciones, collapse = "\n")
        )
        
        updateAceEditor(session, "ia_response_text", value = texto_completo)
      }
    })
  })
  
  output$download_ia_df <- downloadHandler(
    filename = function() paste("ia_dataframe_", Sys.Date(), ".csv", sep = ""),
    content = function(file) {
      if (!is.null(rv_ia$ia_dataframe) && nrow(rv_ia$ia_dataframe) > 0) {
        write.csv(rv_ia$ia_dataframe, file, row.names = FALSE)
      } else {
        write.csv(data.frame(Mensaje = "No se generó un DataFrame"), file, row.names = FALSE)
      }
    }
  )
  
  observeEvent(input$btn_update_tables, {
    withProgress(message = 'Actualizando tablas...', value = 0, {
      success <- TRUE
      if (success && !update_aggregated_table()) success <- FALSE
      incProgress(1/4)
      if (success && !update_clustering_predictions()) success <- FALSE
      incProgress(2/4)
      if (success && !update_logistic_predictions()) success <- FALSE
      incProgress(3/4)
      if (success && !update_master_table()) success <- FALSE
      incProgress(4/4)
      
      if (success) {
        rv$tables <- list_tables()
        rv$last_update <- Sys.time()
        
        rv$datos_tabla <- load_table_data(rv$selected_table)
        if (!is.null(rv$datos_tabla) && "fecha_solicitud" %in% names(rv$datos_tabla)) {
          rv$datos_tabla$fecha_solicitud <- as.Date(rv$datos_tabla$fecha_solicitud)
        }
        rv$data_loaded <- !is.null(rv$datos_tabla) && nrow(rv$datos_tabla) > 0
        
        rv_ia$tables <- rv$tables
        updateSelectInput(session, "ia_table", choices = rv_ia$tables, selected = rv_ia$selected_table)
        rv_ia$datos_tabla <- load_table_data(rv_ia$selected_table)
        if (!is.null(rv_ia$datos_tabla) && "fecha_solicitud" %in% names(rv_ia$datos_tabla)) {
          rv_ia$datos_tabla$fecha_solicitud <- as.Date(rv_ia$datos_tabla$fecha_solicitud)
        }
        rv_ia$data_loaded <- !is.null(rv_ia$datos_tabla) && nrow(rv_ia$datos_tabla) > 0
        
        if ("forecast_combinado" %in% rv$tables) {
          rv_forecast$datos_forecast <- load_table_data("forecast_combinado")
          rv_forecast$data_loaded <- !is.null(rv_forecast$datos_forecast) && nrow(rv_forecast$datos_forecast) > 0
        }
        
        updateSelectInput(session, "table_selector", choices = rv$tables, selected = rv$selected_table)
        
        output$etl_status <- renderText("Tablas actualizadas correctamente")
      } else {
        output$etl_status <- renderText("Error al actualizar tablas")
      }
    })
  })
  
  observeEvent(input$btn_retrain_models, {
    withProgress(message = 'Reentrenando modelos...', value = 0, {
      success <- TRUE
      if (success && !update_aggregated_table()) success <- FALSE
      incProgress(1/6)
      if (success && !retrain_clustering_model()) success <- FALSE
      incProgress(2/6)
      if (success && !update_clustering_predictions()) success <- FALSE
      incProgress(3/6)
      if (success && !retrain_logistic_model()) success <- FALSE
      incProgress(4/6)
      if (success && !update_logistic_predictions()) success <- FALSE
      incProgress(5/6)
      if (success && !update_master_table()) success <- FALSE
      incProgress(6/6)
      
      if (success) {
        rv$tables <- list_tables()
        rv$last_update <- Sys.time()
        
        rv$datos_tabla <- load_table_data(rv$selected_table)
        if (!is.null(rv$datos_tabla) && "fecha_solicitud" %in% names(rv$datos_tabla)) {
          rv$datos_tabla$fecha_solicitud <- as.Date(rv$datos_tabla$fecha_solicitud)
        }
        rv$data_loaded <- !is.null(rv$datos_tabla) && nrow(rv$datos_tabla) > 0
        
        rv_ia$tables <- rv$tables
        updateSelectInput(session, "ia_table", choices = rv_ia$tables, selected = rv_ia$selected_table)
        rv_ia$datos_tabla <- load_table_data(rv_ia$selected_table)
        if (!is.null(rv_ia$datos_tabla) && "fecha_solicitud" %in% names(rv_ia$datos_tabla)) {
          rv_ia$datos_tabla$fecha_solicitud <- as.Date(rv_ia$datos_tabla$fecha_solicitud)
        }
        rv_ia$data_loaded <- !is.null(rv_ia$datos_tabla) && nrow(rv_ia$datos_tabla) > 0
        
        if ("forecast_combinado" %in% rv$tables) {
          rv_forecast$datos_forecast <- load_table_data("forecast_combinado")
          rv_forecast$data_loaded <- !is.null(rv_forecast$datos_forecast) && nrow(rv_forecast$datos_forecast) > 0
        }
        
        updateSelectInput(session, "table_selector", choices = rv$tables, selected = rv$selected_table)
        
        output$etl_status <- renderText("Modelos reentrenados y tablas actualizadas")
      } else {
        output$etl_status <- renderText("Error al reentrenar modelos o actualizar tablas")
      }
    })
  })
  
  observeEvent(input$btn_reload_maestra, {
    withProgress(message = sprintf("Recargando '%s'...", rv$selected_table), value = 0, {
      rv$datos_tabla <- load_table_data(rv$selected_table)
      if (!is.null(rv$datos_tabla) && "fecha_solicitud" %in% names(rv$datos_tabla)) {
        rv$datos_tabla$fecha_solicitud <- as.Date(rv$datos_tabla$fecha_solicitud)
      }
      rv$data_loaded <- !is.null(rv$datos_tabla) && nrow(rv$datos_tabla) > 0
      if (rv$data_loaded) {
        showNotification(sprintf("Datos de '%s' recargados. Filas: %d", rv$selected_table, nrow(rv$datos_tabla)), type = "message")
      } else {
        showNotification(sprintf("Error al recargar '%s'.", rv$selected_table), type = "warning")
      }
      if (rv_ia$selected_table == rv$selected_table) {
        rv_ia$datos_tabla <- load_table_data(rv_ia$selected_table)
        if (!is.null(rv_ia$datos_tabla) && "fecha_solicitud" %in% names(rv_ia$datos_tabla)) {
          rv_ia$datos_tabla$fecha_solicitud <- as.Date(rv_ia$datos_tabla$fecha_solicitud)
        }
        rv_ia$data_loaded <- !is.null(rv_ia$datos_tabla) && nrow(rv_ia$datos_tabla) > 0
      }
    })
  })
  
  
  ############################## PESTAÑA: OVERVIEW ##########################################
  
  output$filtros_ui <- renderUI({
    req(rv$datos_tabla)
    req(nrow(rv$datos_tabla) > 0)
    
    min_date <- min(rv$datos_tabla$fecha_solicitud, na.rm = TRUE)
    max_date <- max(rv$datos_tabla$fecha_solicitud, na.rm = TRUE)
    tipos_empleo <- c("Todas", unique(rv$datos_tabla$tipo_empleo))
    estados_civiles <- c("Todas", unique(rv$datos_tabla$estado_civil))
    
    min_score <- min(rv$datos_tabla$puntaje_crediticio, na.rm = TRUE)
    max_score <- max(rv$datos_tabla$puntaje_crediticio, na.rm = TRUE)
    score_range <- (max_score - min_score) / 6
    puntaje_buckets <- c("6: Muy Alto", "5: Alto", "4: Medio Alto", "3: Medio Bajo", "2: Bajo", "1: Muy Bajo")
    
    min_income <- min(rv$datos_tabla$ingresos_anuales, na.rm = TRUE)
    max_income <- max(rv$datos_tabla$ingresos_anuales, na.rm = TRUE)
    income_range <- (max_income - min_income) / 6
    ingresos_buckets <- c("6: Muy Alto", "5: Alto", "4: Medio Alto", "3: Medio Bajo", "2: Bajo", "1: Muy Bajo")
    
    tagList(
      dateRangeInput("fecha_rango", "Rango de Fechas:",
                     start = min_date, end = max_date,
                     min = min_date, max = max_date,
                     language = "es"),
      selectInput("filtro_edad", "Filtrar por Edad:",
                  choices = c("Todas", "A: 18-29", "B: 30-39", "C: 40-49", "D: 50-59", "E: 60+", "0: <18"),
                  selected = "Todas"),
      selectInput("filtro_empleo", "Filtrar por Tipo de Empleo:",
                  choices = tipos_empleo,
                  selected = "Todas"),
      selectInput("filtro_estado_civil", "Filtrar por Estado Civil:",
                  choices = estados_civiles,
                  selected = "Todas"),
      selectInput("filtro_puntaje", "Filtrar por Puntaje Crediticio:",
                  choices = c("Todas", puntaje_buckets),
                  selected = "Todas"),
      selectInput("filtro_ingresos", "Filtrar por Ingresos Anuales:",
                  choices = c("Todas", ingresos_buckets),
                  selected = "Todas")
    )
  })
  
  datos_filtrados <- reactive({
    req(rv$datos_tabla)
    datos <- rv$datos_tabla
    if (nrow(datos) == 0) return(datos)
    
    # Filtros existentes
    if (!is.null(input$fecha_rango) && length(input$fecha_rango) == 2) {
      datos <- datos %>%
        filter(fecha_solicitud >= input$fecha_rango[1] & fecha_solicitud <= input$fecha_rango[2])
    }
    
    if (!is.null(input$filtro_edad) && input$filtro_edad != "Todas") {
      datos <- datos %>%
        mutate(edad_grupo = case_when(
          edad >= 18 & edad <= 29 ~ 'A: 18-29',
          edad >= 30 & edad <= 39 ~ 'B: 30-39',
          edad >= 40 & edad <= 49 ~ 'C: 40-49',
          edad >= 50 & edad <= 59 ~ 'D: 50-59',
          edad >= 60 ~ 'E: 60+',
          TRUE ~ '0: <18'
        )) %>%
        filter(edad_grupo == input$filtro_edad)
    }
    
    if (!is.null(input$filtro_empleo) && input$filtro_empleo != "Todas") {
      datos <- datos %>% filter(tipo_empleo == input$filtro_empleo)
    }
    
    if (!is.null(input$filtro_estado_civil) && input$filtro_estado_civil != "Todas") {
      datos <- datos %>% filter(estado_civil == input$filtro_estado_civil)
    }
    
    if (!is.null(input$filtro_historial_pagos) && input$filtro_historial_pagos != "Todas") {
      datos <- datos %>% filter(historial_pagos == input$filtro_historial_pagos)
    }
    
    if (!is.null(input$filtro_clusters) && input$filtro_clusters != "Todas") {
      datos <- datos %>% filter(cluster == input$filtro_clusters)
    }
    
    min_score <- min(datos$puntaje_crediticio, na.rm = TRUE)
    max_score <- max(datos$puntaje_crediticio, na.rm = TRUE)
    score_range <- (max_score - min_score) / 6
    datos <- datos %>%
      mutate(
        puntaje_crediticio_bucket = case_when(
          puntaje_crediticio <= min_score + score_range ~ "1: Muy Bajo",
          puntaje_crediticio <= min_score + 2 * score_range ~ "2: Bajo",
          puntaje_crediticio <= min_score + 3 * score_range ~ "3: Medio Bajo",
          puntaje_crediticio <= min_score + 4 * score_range ~ "4: Medio Alto",
          puntaje_crediticio <= min_score + 5 * score_range ~ "5: Alto",
          TRUE ~ "6: Muy Alto"
        )
      )
    
    min_income <- min(datos$ingresos_anuales, na.rm = TRUE)
    max_income <- max(datos$ingresos_anuales, na.rm = TRUE)
    income_range <- (max_income - min_income) / 6
    datos <- datos %>%
      mutate(
        ingresos_anuales_bucket = case_when(
          ingresos_anuales <= min_income + income_range ~ "1: Muy Bajo",
          ingresos_anuales <= min_income + 2 * income_range ~ "2: Bajo",
          ingresos_anuales <= min_income + 3 * income_range ~ "3: Medio Bajo",
          ingresos_anuales <= min_income + 4 * income_range ~ "4: Medio Alto",
          ingresos_anuales <= min_income + 5 * income_range ~ "5: Alto",
          TRUE ~ "6: Muy Alto"
        )
      )
    
    if (!is.null(input$filtro_puntaje) && input$filtro_puntaje != "Todas") {
      datos <- datos %>% filter(puntaje_crediticio_bucket == input$filtro_puntaje)
    }
    
    if (!is.null(input$filtro_ingresos) && input$filtro_ingresos != "Todas") {
      datos <- datos %>% filter(ingresos_anuales_bucket == input$filtro_ingresos)
    }
    
    # Modificación de id_cliente: extraer los últimos caracteres después del último guion
    datos <- datos %>%
      mutate(id_cliente = sub(".*-([^-.]*)$", "\\1", id_cliente))
    
    datos
  })
  
  output$vb_aprobadas <- renderValueBox({
    datos <- datos_filtrados()
    total <- if (!is.null(datos) && "solicitud_credito" %in% names(datos) && nrow(datos) > 0) {
      sum(datos$solicitud_credito == 1, na.rm = TRUE)
    } else {
      NA
    }
    valueBox(
      ifelse(!is.na(total), format(total, big.mark = ",", scientific = FALSE), "Sin datos"),
      "Solicitudes Aprobadas",
      icon = icon("thumbs-up"),
      color = "green"
    )
  })
  
  output$vb_rechazadas <- renderValueBox({
    datos <- datos_filtrados()
    total <- if (!is.null(datos) && "solicitud_credito" %in% names(datos) && nrow(datos) > 0) {
      sum(datos$solicitud_credito == 0, na.rm = TRUE)
    } else {
      NA
    }
    valueBox(
      ifelse(!is.na(total), format(total, big.mark = ",", scientific = FALSE), "Sin datos"),
      "Solicitudes Rechazadas",
      icon = icon("thumbs-down"),
      color = "red"
    )
  })
  
  output$vb_tasa <- renderValueBox({
    datos <- datos_filtrados()
    if (!is.null(datos) && "solicitud_credito" %in% names(datos) && nrow(datos) > 0) {
      aprobadas <- sum(datos$solicitud_credito == 1, na.rm = TRUE)
      total <- sum(!is.na(datos$solicitud_credito))
      tasa <- ifelse(total > 0, round(aprobadas / total * 100, 1), 0)
    } else {
      tasa <- NA
    }
    valueBox(
      ifelse(!is.na(tasa), paste0(tasa, "%"), "Sin datos"),
      "Tasa de Aprobación",
      icon = icon("percent"),
      color = "blue"
    )
  })
  
  #--------------- TENDENCIAS -------------------------#
  
  output$plot_tendencias <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para mostrar tendencias",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos_agregados <- datos %>%
        mutate(fecha_mes = as.Date(cut(fecha_solicitud, "month"))) %>%
        group_by(fecha_mes) %>%
        summarise(
          total_solicitudes = n(),
          aprobadas = sum(solicitud_credito == 1, na.rm = TRUE),
          rechazadas = sum(solicitud_credito == 0, na.rm = TRUE),
          pendientes = sum(estado == "Pendiente (Predicha)", na.rm = TRUE)
        )
      
      if (nrow(datos_agregados) == 0) {
        return(plotly_empty() %>% 
                 layout(title = "No hay datos después de aplicar filtros",
                        paper_bgcolor = "#34495E",
                        plot_bgcolor = "#34495E",
                        font = list(color = "#FFFFFF")))
      }
      
      # Determinar si hay datos válidos de pendientes
      hay_pendientes <- any(!is.na(datos_agregados$pendientes) & datos_agregados$pendientes > 0)
      
      # Definir colores base
      colores <- c("Total Solicitudes" = "#3498DB", 
                   "Aprobadas" = "#2ECC71", 
                   "Rechazadas" = "#E74C3C")
      
      # Iniciar el gráfico
      p <- ggplot(datos_agregados, aes(x = fecha_mes)) +
        geom_line(aes(y = total_solicitudes, color = "Total Solicitudes"), size = 1) +
        geom_line(aes(y = aprobadas, color = "Aprobadas"), size = 1) +
        geom_line(aes(y = rechazadas, color = "Rechazadas"), size = 1)
      
      # Agregar línea de pendientes solo si hay datos
      if (hay_pendientes) {
        p <- p + geom_line(aes(y = pendientes, color = "Pendientes"), size = 1)
        colores <- c(colores, "Pendientes" = "#F1C40F")
      }
      
      # Finalizar el gráfico
      p <- p +
        labs(title = "Tendencias Mensuales", 
             x = "Fecha", 
             y = "Cantidad", 
             color = NULL) +
        scale_color_manual(values = colores) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.position = "bottom",
          legend.background = element_blank(),
          legend.box.background = element_blank()
        )
      
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "x unified",
          yaxis = list(tickformat = ",")
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # --------------- Tasa de Aprobación ---------------------#
  
  output$plot_tasa_aprobacion <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para mostrar la tasa de aprobación",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos_agregados <- datos %>%
        mutate(fecha_mes = as.Date(cut(fecha_solicitud, "month"))) %>%
        group_by(fecha_mes) %>%
        summarise(
          aprobadas = sum(solicitud_credito == 1, na.rm = TRUE),
          evaluadas = sum(estado == "Evaluada", na.rm = TRUE)
        ) %>%
        mutate(tasa_aprobacion = ifelse(evaluadas > 0, aprobadas / evaluadas, 0))
      
      if (nrow(datos_agregados) == 0) {
        return(plotly_empty() %>% 
                 layout(title = "No hay datos después de aplicar filtros",
                        paper_bgcolor = "#34495E",
                        plot_bgcolor = "#34495E",
                        font = list(color = "#FFFFFF")))
      }
      
      p <- ggplot(datos_agregados, aes(x = fecha_mes, y = tasa_aprobacion)) +
        geom_line(color = "#2ECC71", size = 1) + # Color directo, sin leyenda
        labs(title = "Tendencia de Tasa de Aprobación", 
             x = "Fecha", 
             y = "Porcentaje") +
        scale_y_continuous(labels = function(x) sprintf("%.1f%%", x * 100)) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF")
        )
      
      # Convertir a plotly y personalizar el tooltip
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "x unified",
          hoverlabel = list(bgcolor = "#2C3E50", font = list(color = "#FFFFFF")),
          yaxis = list(tickformat = ".1%"), # Formato del eje Y en porcentaje con 1 decimal
          showlegend = FALSE # Eliminar la leyenda
        ) %>%
        style(
          text = sprintf("%.1f%%", datos_agregados$tasa_aprobacion * 100), # Formato del tooltip
          hoverinfo = "x+text" # Mostrar fecha (x) y valor personalizado (text)
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # ---------- HEATMAP EDAD-TASA APROBACIÓN - TIPO EMPLEO
  
  output$heatmap_edad_empleo <- renderPlotly({
    datos <- datos_filtrados()
    
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para el heatmap",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      # Crear buckets de edad
      datos <- datos %>%
        mutate(edad_bucket = case_when(
          edad < 18 ~ "0: <18",
          edad >= 18 & edad <= 29 ~ "A: 18-29",
          edad >= 30 & edad <= 39 ~ "B: 30-39",
          edad >= 40 & edad <= 49 ~ "C: 40-49",
          edad >= 50 & edad <= 59 ~ "D: 50-59",
          edad >= 60 ~ "E: 60+",
          TRUE ~ "Desconocido"
        ))
      
      # Agrupar por edad_bucket y tipo_empleo, y calcular la tasa de aprobación
      datos_agregados <- datos %>%
        filter(estado == "Evaluada") %>%
        group_by(edad_bucket, tipo_empleo) %>%
        summarise(
          aprobadas = sum(solicitud_credito == 1, na.rm = TRUE),
          total_evaluadas = n(),
          .groups = "drop"
        ) %>%
        mutate(tasa_aprobacion = ifelse(total_evaluadas > 0, (aprobadas / total_evaluadas) * 100, 0)) %>%
        ungroup()
      
      # Filtrar para eliminar "Desconocido" si existe
      datos_agregados <- datos_agregados %>% 
        filter(edad_bucket != "Desconocido")
      
      # Ordenar los niveles de edad_bucket
      datos_agregados$edad_bucket <- factor(datos_agregados$edad_bucket,
                                            levels = c("0: <18", "A: 18-29", "B: 30-39", "C: 40-49", "D: 50-59", "E: 60+"))
      
      # Crear el heatmap con ggplot2
      p <- ggplot(datos_agregados, aes(x = tipo_empleo, y = edad_bucket, fill = tasa_aprobacion)) +
        geom_tile(color = "white") +
        geom_text(aes(label = sprintf("%.1f%%", tasa_aprobacion)), color = "white", size = 4) +
        scale_fill_gradient(low = "#E74C3C", high = "#2ECC71", name = "Tasa de Aprobación (%)") +
        labs(title = "Tasa de Aprobación por Edad y Tipo de Empleo",
             x = "Tipo de Empleo",
             y = "Edad (Buckets)") +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right",
          panel.grid = element_blank()
        )
      
      # Convertir a plotly
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest"
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el heatmap:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # ------------ Heatmap Tasa de Aprobación por Estado Civil y Tipo de Empleo
  
  output$heatmap_estado_civil_empleo <- renderPlotly({
    datos <- datos_filtrados()
    
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para el heatmap",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      # Agrupar por estado_civil y tipo_empleo, y calcular la tasa de aprobación
      datos_agregados <- datos %>%
        filter(estado == "Evaluada") %>%
        group_by(estado_civil, tipo_empleo) %>%
        summarise(
          aprobadas = sum(solicitud_credito == 1, na.rm = TRUE),
          total_evaluadas = n(),
          .groups = "drop"
        ) %>%
        mutate(tasa_aprobacion = ifelse(total_evaluadas > 0, (aprobadas / total_evaluadas) * 100, 0)) %>%
        ungroup()
      
      # Crear el heatmap con ggplot2
      p <- ggplot(datos_agregados, aes(x = tipo_empleo, y = estado_civil, fill = tasa_aprobacion)) +
        geom_tile(color = "white") +
        geom_text(aes(label = sprintf("%.1f%%", tasa_aprobacion)), color = "white", size = 4) +
        scale_fill_gradient(low = "#E74C3C", high = "#2ECC71", name = "Tasa de Aprobación (%)") +
        labs(title = "Tasa de Aprobación por Estado Civil y Tipo de Empleo",
             x = "Tipo de Empleo",
             y = "Estado Civil") +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right",
          panel.grid = element_blank()
        )
      
      # Convertir a plotly
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest"
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el heatmap:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # ------------ Heatmap Tasa de aprobacion por buckets de puntaje e ingresos
  
  output$heatmap_puntaje_ingresos <- renderPlotly({
    datos <- datos_filtrados()
    
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para el heatmap",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      # Crear buckets para puntaje_crediticio con rangos
      datos <- datos %>%
        mutate(puntaje_crediticio_bucket = case_when(
          puntaje_crediticio >= 300 & puntaje_crediticio <= 499 ~ "1: Muy Bajo (300-499)",
          puntaje_crediticio >= 500 & puntaje_crediticio <= 599 ~ "2: Bajo (500-599)",
          puntaje_crediticio >= 600 & puntaje_crediticio <= 649 ~ "3: Medio Bajo (600-649)",
          puntaje_crediticio >= 650 & puntaje_crediticio <= 699 ~ "4: Medio Alto (650-699)",
          puntaje_crediticio >= 700 & puntaje_crediticio <= 749 ~ "5: Alto (700-749)",
          puntaje_crediticio >= 750 & puntaje_crediticio <= 850 ~ "6: Muy Alto (750-850)",
          TRUE ~ "Desconocido"
        ))
      
      # Crear buckets para ingresos_anuales con rangos
      datos <- datos %>%
        mutate(ingresos_anuales_bucket = case_when(
          ingresos_anuales >= 0 & ingresos_anuales <= 19999 ~ "1: Muy Bajo (0-19,999)",
          ingresos_anuales >= 20000 & ingresos_anuales <= 39999 ~ "2: Bajo (20,000-39,999)",
          ingresos_anuales >= 40000 & ingresos_anuales <= 59999 ~ "3: Medio Bajo (40,000-59,999)",
          ingresos_anuales >= 60000 & ingresos_anuales <= 79999 ~ "4: Medio Alto (60,000-79,999)",
          ingresos_anuales >= 80000 & ingresos_anuales <= 99999 ~ "5: Alto (80,000-99,999)",
          ingresos_anuales >= 100000 ~ "6: Muy Alto (100,000+)",
          TRUE ~ "Desconocido"
        ))
      
      # Agrupar por buckets y calcular la tasa de aprobación
      datos_agregados <- datos %>%
        filter(estado == "Evaluada") %>%
        group_by(puntaje_crediticio_bucket, ingresos_anuales_bucket) %>%
        summarise(
          aprobadas = sum(solicitud_credito == 1, na.rm = TRUE),
          total_evaluadas = n(),
          tasa_aprobacion = ifelse(total_evaluadas > 0, (aprobadas / total_evaluadas) * 100, 0),
          .groups = "drop"
        ) %>%
        filter(puntaje_crediticio_bucket != "Desconocido" & ingresos_anuales_bucket != "Desconocido")
      
      # Ordenar los niveles de los buckets
      datos_agregados$puntaje_crediticio_bucket <- factor(datos_agregados$puntaje_crediticio_bucket,
                                                          levels = c("1: Muy Bajo (300-499)", "2: Bajo (500-599)", 
                                                                     "3: Medio Bajo (600-649)", "4: Medio Alto (650-699)", 
                                                                     "5: Alto (700-749)", "6: Muy Alto (750-850)"))
      
      datos_agregados$ingresos_anuales_bucket <- factor(datos_agregados$ingresos_anuales_bucket,
                                                        levels = c("1: Muy Bajo (0-19,999)", "2: Bajo (20,000-39,999)", 
                                                                   "3: Medio Bajo (40,000-59,999)", "4: Medio Alto (60,000-79,999)", 
                                                                   "5: Alto (80,000-99,999)", "6: Muy Alto (100,000+)"))
      
      # Crear el heatmap con ggplot2
      p <- ggplot(datos_agregados, aes(x = ingresos_anuales_bucket, y = puntaje_crediticio_bucket, fill = tasa_aprobacion)) +
        geom_tile(color = "white") +
        geom_text(aes(label = sprintf("%.1f%%", tasa_aprobacion)), color = "white", size = 4) +
        scale_fill_gradient(low = "#E74C3C", high = "#2ECC71", name = "Tasa de Aprobación (%)") +
        labs(title = "Tasa de Aprobación por Puntaje Crediticio e Ingresos Anuales",
             x = "Ingresos Anuales",
             y = "Puntaje Crediticio") +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right",
          panel.grid = element_blank(),
          axis.text.x = element_text(angle = 45, hjust = 1)
        )
      
      # Convertir a plotly
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest",
          xaxis = list(tickangle = -45)
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el heatmap:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # -------- TABLA ------------------------------------------------------#      
  
  # Selector de columnas para Overview
  output$overview_column_selector <- renderUI({
    datos <- datos_filtrados()
    if (!is.null(datos) && nrow(datos) > 0) {
      selectInput(
        "overview_search_column",
        "Filtrar en la columna:",
        choices = c("Todas", names(datos)),
        selected = "Todas"
      )
    } else {
      NULL
    }
  })
  
  # Tabla Overview modificada
  output$table_data <- renderDT({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(datatable(data.frame(Mensaje = "No hay datos disponibles"),
                       options = list(dom = "t", pageLength = 5),
                       rownames = FALSE))
    }
    
    # Transform id_cliente to match Deep-Dive style
    datos <- datos %>%
      mutate(id_cliente = sub(".*?-", "", id_cliente))
    
    datatable(
      datos,
      options = list(
        dom = "lrtipB",
        buttons = c("copy", "csv", "excel", "pdf", "print"),
        pageLength = 10,
        lengthMenu = c(5, 10, 25, 50),
        searchHighlight = FALSE,
        autoWidth = FALSE,
        scrollX = TRUE,
        columnDefs = list(list(className = "dt-center", targets = "_all")),
        # Configurar búsqueda en columna específica
        searchCols = if (!is.null(input$overview_search_column) && input$overview_search_column != "Todas") {
          lapply(names(datos), function(col) {
            if (col == input$overview_search_column) NULL else list(search = "")
          })
        } else {
          NULL
        }
      ),
      rownames = FALSE,
      class = "display",
      extensions = "Buttons",
      style = "bootstrap",
      caption = "Datos Filtrados de Solicitudes"
    ) %>%
      formatStyle(
        columns = names(datos),
        backgroundColor = "#34495E",
        color = "#FFFFFF",
        fontSize = "12px"
      ) %>%
      formatCurrency(columns = c("ingresos_anuales", "deuda_actual"), currency = "$", digits = 0) %>%
      formatDate(columns = "fecha_solicitud", method = "toLocaleDateString")
  })
  
  
  
  
  
  
  
  
  
  
  
  
  ######## PESTAÑA: DEEP-DIVE ###########
  
  
  
  
  output$vb_pendientes_predichas <- renderValueBox({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      valueBox(
        "N/A",
        "Solicitudes por Evaluar",
        icon = icon("hourglass-half"),
        color = "yellow"
      )
    } else {
      pendientes <- sum(datos$estado == "Pendiente (Predicha)", na.rm = TRUE)
      valueBox(
        format(pendientes, big.mark = ","),
        "Solicitudes por Evaluar",
        icon = icon("hourglass-half"),
        color = "yellow"
      )
    }
  })
  
  output$vb_aprobadas_predichas <- renderValueBox({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      valueBox(
        "N/A",
        "Aprobadas (Predicción)",
        icon = icon("check"),
        color = "green"
      )
    } else {
      aprobadas <- sum(datos$predicted_solicitud_credito == 1, na.rm = TRUE)
      valueBox(
        format(aprobadas, big.mark = ","),
        "Aprobadas (Predicción)",
        icon = icon("check"),
        color = "green"
      )
    }
  })
  
  output$vb_tasa_predicha <- renderValueBox({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      valueBox(
        "N/A",
        "Tasa de Aprobación (Predicción)",
        icon = icon("percent"),
        color = "blue"
      )
    } else {
      aprobadas <- sum(datos$predicted_solicitud_credito == 1, na.rm = TRUE)
      pendientes <- sum(datos$estado == "Pendiente (Predicha)", na.rm = TRUE)
      tasa <- ifelse(pendientes > 0, aprobadas / pendientes, 0)
      valueBox(
        sprintf("%.1f%%", tasa * 100),
        "Tasa de Aprobación (Predicción)",
        icon = icon("percent"),
        color = "blue"
      )
    }
  })
  
  
  
  
  # Tendencias Semanales (Predicciones) - Side-by-Side Bar Chart
  output$plot_tendencias_semanal_deep <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para mostrar tendencias semanales",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos_agregados <- datos %>%
        filter(estado == "Pendiente (Predicha)") %>%
        mutate(semana = as.Date(cut(fecha_solicitud, "week"))) %>%
        group_by(semana) %>%
        summarise(
          aprobadas_predichas = sum(predicted_solicitud_credito == 1, na.rm = TRUE),
          rechazadas_predichas = sum(predicted_solicitud_credito == 0, na.rm = TRUE),
          .groups = "drop"
        )
      
      if (nrow(datos_agregados) == 0) {
        return(plotly_empty() %>% 
                 layout(title = "No hay datos después de aplicar filtros",
                        paper_bgcolor = "#34495E",
                        plot_bgcolor = "#34495E",
                        font = list(color = "#FFFFFF")))
      }
      
      p <- plot_ly(datos_agregados, x = ~semana) %>%
        add_bars(y = ~aprobadas_predichas, name = "Aprobadas (Predichas)", 
                 marker = list(color = "#2ECC71"), text = ~aprobadas_predichas, 
                 textposition = "auto", textfont = list(color = "#FFFFFF")) %>%
        add_bars(y = ~rechazadas_predichas, name = "Rechazadas (Predichas)", 
                 marker = list(color = "#E74C3C"), text = ~rechazadas_predichas, 
                 textposition = "auto", textfont = list(color = "#FFFFFF")) %>%
        layout(
          title = "Tendencias Semanales de Predicciones",
          xaxis = list(title = "Semana", tickangle = -45),
          yaxis = list(title = "Cantidad", tickformat = ","),
          barmode = "group",  # Side-by-side bars
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          legend = list(orientation = "h", x = 0.5, xanchor = "center", y = -0.2),
          hovermode = "x unified"
        )
      
      p
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # Tasa de Aprobación Semanal (Predicciones) - Start from First Non-Zero
  output$plot_tasa_aprobacion_semanal_deep <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para la tasa de aprobación semanal",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos_agregados <- datos %>%
        filter(estado == "Pendiente (Predicha)") %>%
        mutate(semana = as.Date(cut(fecha_solicitud, "week"))) %>%
        group_by(semana) %>%
        summarise(
          aprobadas_predichas = sum(predicted_solicitud_credito == 1, na.rm = TRUE),
          total_predichas = sum(!is.na(predicted_solicitud_credito)),
          tasa_aprobacion = ifelse(total_predichas > 0, aprobadas_predichas / total_predichas, 0),
          .groups = "drop"
        ) %>%
        filter(total_predichas > 0)  # Remove weeks with no data
      
      if (nrow(datos_agregados) == 0) {
        return(plotly_empty() %>% 
                 layout(title = "No hay datos después de aplicar filtros",
                        paper_bgcolor = "#34495E",
                        plot_bgcolor = "#34495E",
                        font = list(color = "#FFFFFF")))
      }
      
      p <- ggplot(datos_agregados, aes(x = semana, y = tasa_aprobacion)) +
        geom_line(color = "#2ECC71", size = 1) +
        labs(title = "Tendencia de Tasa de Aprobación Semanal (Predicciones)", 
             x = "Semana", 
             y = "Porcentaje") +
        scale_y_continuous(labels = function(x) sprintf("%.1f%%", x * 100), limits = c(0, 1)) +
        scale_x_date(limits = c(min(datos_agregados$semana), max(datos_agregados$semana))) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF")
        )
      
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "x unified",
          yaxis = list(tickformat = ".1%"),
          showlegend = FALSE
        ) %>%
        style(
          text = sprintf("%.1f%%", datos_agregados$tasa_aprobacion * 100),
          hoverinfo = "x+text"
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # Heatmap: Tasa de Aprobación por Edad y Tipo de Empleo (Predicciones)
  output$heatmap_edad_empleo_deep <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para el heatmap",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos <- datos %>%
        filter(estado == "Pendiente (Predicha)") %>%
        mutate(edad_bucket = case_when(
          edad < 18 ~ "0: <18",
          edad >= 18 & edad <= 29 ~ "A: 18-29",
          edad >= 30 & edad <= 39 ~ "B: 30-39",
          edad >= 40 & edad <= 49 ~ "C: 40-49",
          edad >= 50 & edad <= 59 ~ "D: 50-59",
          edad >= 60 ~ "E: 60+",
          TRUE ~ "Desconocido"
        ))
      
      datos_agregados <- datos %>%
        group_by(edad_bucket, tipo_empleo) %>%
        summarise(
          aprobadas_predichas = sum(predicted_solicitud_credito == 1, na.rm = TRUE),
          total_predichas = n(),
          tasa_aprobacion = ifelse(total_predichas > 0, (aprobadas_predichas / total_predichas) * 100, 0),
          .groups = "drop"
        ) %>%
        filter(edad_bucket != "Desconocido")
      
      datos_agregados$edad_bucket <- factor(datos_agregados$edad_bucket,
                                            levels = c("0: <18", "A: 18-29", "B: 30-39", "C: 40-49", "D: 50-59", "E: 60+"))
      
      p <- ggplot(datos_agregados, aes(x = tipo_empleo, y = edad_bucket, fill = tasa_aprobacion)) +
        geom_tile(color = "white") +
        geom_text(aes(label = sprintf("%.1f%%", tasa_aprobacion)), color = "white", size = 4) +
        scale_fill_gradient(low = "#E74C3C", high = "#2ECC71", name = "Tasa de Aprobación (%)") +
        labs(title = "Tasa de Aprobación por Edad y Tipo de Empleo (Predicciones)",
             x = "Tipo de Empleo",
             y = "Edad (Buckets)") +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right",
          panel.grid = element_blank()
        )
      
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest"
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el heatmap:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # Heatmap: Tasa de Aprobación por Estado Civil y Tipo de Empleo (Predicciones)
  output$heatmap_estado_civil_empleo_deep <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para el heatmap",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos_agregados <- datos %>%
        filter(estado == "Pendiente (Predicha)") %>%
        group_by(estado_civil, tipo_empleo) %>%
        summarise(
          aprobadas_predichas = sum(predicted_solicitud_credito == 1, na.rm = TRUE),
          total_predichas = n(),
          tasa_aprobacion = ifelse(total_predichas > 0, (aprobadas_predichas / total_predichas) * 100, 0),
          .groups = "drop"
        )
      
      p <- ggplot(datos_agregados, aes(x = tipo_empleo, y = estado_civil, fill = tasa_aprobacion)) +
        geom_tile(color = "white") +
        geom_text(aes(label = sprintf("%.1f%%", tasa_aprobacion)), color = "white", size = 4) +
        scale_fill_gradient(low = "#E74C3C", high = "#2ECC71", name = "Tasa de Aprobación (%)") +
        labs(title = "Tasa de Aprobación por Estado Civil y Tipo de Empleo (Predicciones)",
             x = "Tipo de Empleo",
             y = "Estado Civil") +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right",
          panel.grid = element_blank()
        )
      
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest"
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el heatmap:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # Heatmap: Tasa de Aprobación por Puntaje Crediticio e Ingresos Anuales (Predicciones)
  output$heatmap_puntaje_ingresos_deep <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para el heatmap",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos <- datos %>%
        filter(estado == "Pendiente (Predicha)") %>%
        mutate(
          puntaje_crediticio_bucket = case_when(
            puntaje_crediticio >= 300 & puntaje_crediticio <= 499 ~ "1: Muy Bajo (300-499)",
            puntaje_crediticio >= 500 & puntaje_crediticio <= 599 ~ "2: Bajo (500-599)",
            puntaje_crediticio >= 600 & puntaje_crediticio <= 649 ~ "3: Medio Bajo (600-649)",
            puntaje_crediticio >= 650 & puntaje_crediticio <= 699 ~ "4: Medio Alto (650-699)",
            puntaje_crediticio >= 700 & puntaje_crediticio <= 749 ~ "5: Alto (700-749)",
            puntaje_crediticio >= 750 & puntaje_crediticio <= 850 ~ "6: Muy Alto (750-850)",
            TRUE ~ "Desconocido"
          ),
          ingresos_anuales_bucket = case_when(
            ingresos_anuales >= 0 & ingresos_anuales <= 19999 ~ "1: Muy Bajo (0-19,999)",
            ingresos_anuales >= 20000 & ingresos_anuales <= 39999 ~ "2: Bajo (20,000-39,999)",
            ingresos_anuales >= 40000 & ingresos_anuales <= 59999 ~ "3: Medio Bajo (40,000-59,999)",
            ingresos_anuales >= 60000 & ingresos_anuales <= 79999 ~ "4: Medio Alto (60,000-79,999)",
            ingresos_anuales >= 80000 & ingresos_anuales <= 99999 ~ "5: Alto (80,000-99,999)",
            ingresos_anuales >= 100000 ~ "6: Muy Alto (100,000+)",
            TRUE ~ "Desconocido"
          )
        )
      
      datos_agregados <- datos %>%
        group_by(puntaje_crediticio_bucket, ingresos_anuales_bucket) %>%
        summarise(
          aprobadas_predichas = sum(predicted_solicitud_credito == 1, na.rm = TRUE),
          total_predichas = n(),
          tasa_aprobacion = ifelse(total_predichas > 0, (aprobadas_predichas / total_predichas) * 100, 0),
          .groups = "drop"
        ) %>%
        filter(puntaje_crediticio_bucket != "Desconocido" & ingresos_anuales_bucket != "Desconocido")
      
      datos_agregados$puntaje_crediticio_bucket <- factor(datos_agregados$puntaje_crediticio_bucket,
                                                          levels = c("1: Muy Bajo (300-499)", "2: Bajo (500-599)", 
                                                                     "3: Medio Bajo (600-649)", "4: Medio Alto (650-699)", 
                                                                     "5: Alto (700-749)", "6: Muy Alto (750-850)"))
      datos_agregados$ingresos_anuales_bucket <- factor(datos_agregados$ingresos_anuales_bucket,
                                                        levels = c("1: Muy Bajo (0-19,999)", "2: Bajo (20,000-39,999)", 
                                                                   "3: Medio Bajo (40,000-59,999)", "4: Medio Alto (60,000-79,999)", 
                                                                   "5: Alto (80,000-99,999)", "6: Muy Alto (100,000+)"))
      
      p <- ggplot(datos_agregados, aes(x = ingresos_anuales_bucket, y = puntaje_crediticio_bucket, fill = tasa_aprobacion)) +
        geom_tile(color = "white") +
        geom_text(aes(label = sprintf("%.1f%%", tasa_aprobacion)), color = "white", size = 4) +
        scale_fill_gradient(low = "#E74C3C", high = "#2ECC71", name = "Tasa de Aprobación (%)") +
        labs(title = "Tasa de Aprobación por Puntaje Crediticio e Ingresos Anuales (Predicciones)",
             x = "Ingresos Anuales",
             y = "Puntaje Crediticio") +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right",
          panel.grid = element_blank(),
          axis.text.x = element_text(angle = 45, hjust = 1)
        )
      
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest",
          xaxis = list(tickangle = -45)
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el heatmap:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  
  
  
  ######### SCATTER EDAD-INGRESOS- TASA DE APROBACION ########################
  
  
  output$scatter_clusters_ingresos_edad <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para la distribución de clusters",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      # Opción 1: Muestreo aleatorio para reducir el número de puntos (si hay muchos datos)
      if (nrow(datos) > 1000) {
        datos <- datos %>% sample_n(1000)  # Muestreo de 5000 puntos
      }
      
      # Crear el gráfico base con ggplot
      p <- ggplot(datos, aes(x = ingresos_anuales, y = edad, color = factor(cluster))) +
        # Reducir aún más la opacidad y el tamaño para minimizar solapamiento
        geom_jitter(alpha = 0.2, size = 1, width = 0.1, height = 0.1) +
        # Añadir contornos de densidad para resaltar áreas de concentración
        geom_density_2d(aes(group = factor(cluster)), linewidth = 0.5, alpha = 0.5) +
        labs(title = "Distribución de Clusters: Ingresos Anuales vs Edad",
             x = "Ingresos Anuales",
             y = "Edad",
             color = "Cluster") +
        scale_x_continuous(labels = scales::comma) +
        scale_color_viridis_d(option = "C", begin = 0.2, end = 0.9) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right",
          panel.grid.major = element_line(color = "grey30", size = 0.2),
          panel.grid.minor = element_blank()
        )
      
      # Convertir a plotly con información adicional en hover
      ggplotly(p, tooltip = c("x", "y", "color")) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest",
          legend = list(
            bgcolor = "rgba(255,255,255,0.1)",
            bordercolor = "#FFFFFF",
            font = list(color = "#FFFFFF")
          )
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  
  
  ############## BURBUJA EDAD-INGRESOS-TASA DE APROBACIÓN ###############
  
  
  
  output$bubble_clusters_mean <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para la distribución de clusters",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos_agregados <- datos %>%
        group_by(cluster) %>%
        summarise(
          mean_ingresos = mean(ingresos_anuales, na.rm = TRUE),
          sd_ingresos = sd(ingresos_anuales, na.rm = TRUE),
          mean_edad = mean(edad, na.rm = TRUE),
          sd_edad = sd(edad, na.rm = TRUE),
          aprobadas = sum(ifelse(estado == "Evaluada", solicitud_credito == 1, predicted_solicitud_credito == 1), na.rm = TRUE),
          total = n(),
          tasa_aprobacion = ifelse(total > 0, (aprobadas / total) * 100, 0),
          .groups = "drop"
        ) %>%
        mutate(cluster = factor(cluster, levels = unique(cluster)))
      
      p <- ggplot(datos_agregados, aes(x = mean_ingresos, y = mean_edad, size = tasa_aprobacion, color = cluster, 
                                       text = paste(
                                         "Cluster:", cluster, "<br>",
                                         "Ingresos Anuales: ", round(mean_ingresos, 2), "±", round(sd_ingresos, 2), "<br>",
                                         "Edad: ", round(mean_edad, 2), "±", round(sd_edad, 2), "<br>",
                                         "Tasa de Aprobación:", round(tasa_aprobacion, 1), "%"
                                       ))) +
        geom_point(alpha = 0.8) +
        scale_size_continuous(range = c(5, 20), name = "Tasa de Aprobación (%)") +
        labs(title = "Clusters: Media de Ingresos Anuales y Edad con Tasa de Aprobación",
             x = "Ingresos Anuales (Media)",
             y = "Edad (Media)",
             color = "Cluster") +
        scale_x_continuous(labels = scales::comma) +
        scale_color_viridis(discrete = TRUE) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right"
        )
      
      ggplotly(p, tooltip = "text") %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest",
          hoverlabel = list(bgcolor = "#2C3E50", font = list(color = "#FFFFFF"))
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  
  
  ################ SCATTER PUNTAJE-DEUDA-EDAD  #######################################
  
  output$scatter_clusters_puntaje_deuda <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      if (nrow(datos) > 1000) datos <- datos %>% sample_n(1000)
      
      p <- ggplot(datos, aes(x = puntaje_crediticio, y = deuda_actual, color = factor(cluster), size = edad)) +
        geom_jitter(alpha = 0.3, width = 0.1, height = 0.1) +
        facet_wrap(~cluster, ncol = 2) +
        labs(title = "Distribución de Clusters: Puntaje Crediticio vs Deuda Actual",
             x = "Puntaje Crediticio",
             y = "Deuda Actual",
             color = "Cluster",
             size = "Edad") +
        scale_y_continuous(labels = scales::comma) +
        scale_color_viridis_d(option = "C", begin = 0.2, end = 0.9) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right",
          panel.grid.major = element_line(color = "grey30", size = 0.2),
          panel.grid.minor = element_blank(),
          strip.text = element_text(color = "#FFFFFF")
        )
      
      ggplotly(p, tooltip = c("x", "y", "color", "size")) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest",
          legend = list(bgcolor = "rgba(255,255,255,0.1)",
                        bordercolor = "#FFFFFF",
                        font = list(color = "#FFFFFF"))
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  
  
  
  
  
  
  ################ BURBUJA PUNTAJE-DEUDA-TASA DE APROBACIÓN #######################################
  
  
  output$bubble_clusters_puntaje_deuda <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para la distribución de clusters",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos_agregados <- datos %>%
        group_by(cluster) %>%
        summarise(
          mean_puntaje = mean(puntaje_crediticio, na.rm = TRUE),
          sd_puntaje = sd(puntaje_crediticio, na.rm = TRUE),
          mean_deuda = mean(deuda_actual, na.rm = TRUE),
          sd_deuda = sd(deuda_actual, na.rm = TRUE),
          aprobadas = sum(ifelse(estado == "Evaluada", solicitud_credito == 1, predicted_solicitud_credito == 1), na.rm = TRUE),
          total = n(),
          tasa_aprobacion = ifelse(total > 0, (aprobadas / total) * 100, 0),
          .groups = "drop"
        ) %>%
        mutate(cluster = factor(cluster, levels = unique(cluster)))
      
      p <- ggplot(datos_agregados, aes(x = mean_puntaje, y = mean_deuda, size = tasa_aprobacion, color = cluster, 
                                       text = paste(
                                         "Cluster:", cluster, "<br>",
                                         "Puntaje Crediticio: ", round(mean_puntaje, 2), "±", round(sd_puntaje, 2), "<br>",
                                         "Deuda Actual: ", round(mean_deuda, 2), "±", round(sd_deuda, 2), "<br>",
                                         "Tasa de Aprobación:", round(tasa_aprobacion, 1), "%"
                                       ))) +
        geom_point(alpha = 0.8) +
        scale_size_continuous(range = c(5, 20), name = "Tasa de Aprobación (%)") +
        labs(title = "Clusters: Media de Puntaje Crediticio y Deuda Actual con Tasa de Aprobación",
             x = "Puntaje Crediticio (Media)",
             y = "Deuda Actual (Media)",
             color = "Cluster") +
        scale_y_continuous(labels = scales::comma) +
        scale_color_viridis(discrete = TRUE) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.title = element_text(color = "#FFFFFF"),
          legend.position = "right"
        )
      
      ggplotly(p, tooltip = "text") %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "closest",
          hoverlabel = list(bgcolor = "#2C3E50", font = list(color = "#FFFFFF"))
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  
  
  
  
  
  
  
  ################################ BARPLOT #############################
  
  
  output$bar_tasa_aprobacion <- renderPlotly({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles para la tasa de aprobación",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      datos_agregados <- datos %>%
        group_by(cluster) %>%
        summarise(
          aprobadas = sum(ifelse(estado == "Evaluada", solicitud_credito == 1, predicted_solicitud_credito == 1), na.rm = TRUE),
          total = n(),
          tasa_aprobacion = ifelse(total > 0, (aprobadas / total) * 100, 0),
          .groups = "drop"
        ) %>%
        mutate(cluster = factor(cluster, levels = unique(cluster)))
      
      p <- ggplot(datos_agregados, aes(x = cluster, y = tasa_aprobacion, fill = cluster)) +
        geom_bar(stat = "identity") +
        scale_fill_viridis(discrete = TRUE) +
        labs(title = "Tasa de Aprobación por Cluster",
             x = "Cluster",
             y = "Tasa de Aprobación (%)") +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.position = "none"
        ) +
        geom_text(aes(label = paste0(round(tasa_aprobacion, 1), "%")), vjust = -0.5, color = "white") +
        scale_y_continuous(labels = function(x) paste0(x, "%"))
      
      ggplotly(p)
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico de tasa de aprobación:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  
  
  
  ############## BOXPLOT ###########################################################
  
  
  output$boxplots_variables_numericas <- renderUI({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>%
               layout(title = "No hay datos disponibles para los boxplots",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      variables_numericas <- c(
        "edad",
        "ingresos_anuales",
        "puntaje_crediticio",
        "deuda_actual",
        "antiguedad_laboral",
        "numero_dependientes"
      )
      variable_nombres <- c(
        "Edad",
        "Ingresos Anuales",
        "Puntaje Crediticio",
        "Deuda Actual",
        "Antigüedad Laboral",
        "Número de Dependientes"
      )
      
      plots <- lapply(seq_along(variables_numericas), function(i) {
        variable <- variables_numericas[i]
        nombre_variable <- variable_nombres[i]
        
        p <- ggplot(datos, aes(x = factor(cluster), y = .data[[variable]], fill = factor(cluster))) +
          geom_boxplot() +
          scale_fill_viridis(discrete = TRUE) +
          labs(
            title = paste("Distribución de", nombre_variable, "por Cluster"),
            x = "Cluster",
            y = nombre_variable
          ) +
          theme_minimal() +
          theme(
            plot.background = element_rect(fill = "#34495E", color = NA),
            panel.background = element_rect(fill = "#34495E", color = NA),
            text = element_text(color = "#FFFFFF"),
            axis.text = element_text(color = "#FFFFFF"),
            legend.position = "none"
          )
        
        plotlyOutput(paste0("boxplot_", variable), height = "400px")
      })
      plot_output_list <- lapply(seq_along(variables_numericas), function(i){
        renderPlotly({
          variable <- variables_numericas[i]
          nombre_variable <- variable_nombres[i]
          
          p <- ggplot(datos, aes(x = factor(cluster), y = .data[[variable]], fill = factor(cluster))) +
            geom_boxplot() +
            scale_fill_viridis(discrete = TRUE) +
            labs(
              title = paste("Distribución de", nombre_variable, "por Cluster"),
              x = "Cluster",
              y = nombre_variable
            ) +
            theme_minimal() +
            theme(
              plot.background = element_rect(fill = "#34495E", color = NA),
              panel.background = element_rect(fill = "#34495E", color = NA),
              text = element_text(color = "#FFFFFF"),
              axis.text = element_text(color = "#FFFFFF"),
              legend.position = "none"
            )
          
          ggplotly(p)
        })
      })
      
      for (i in seq_along(variables_numericas)) {
        output[[paste0("boxplot_", variables_numericas[i])]] <- plot_output_list[[i]]
      }
      
      tagList(lapply(seq_along(variables_numericas), function(i) {
        plotlyOutput(paste0("boxplot_", variables_numericas[i]), height = "400px")
      }))
      
    }, error = function(e) {
      plotly_empty() %>%
        layout(title = paste("Error en los boxplots:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  
  
  
  ############## PERFILES DEL CLUSTER (TABLA)   ################## 
  
  output$perfiles_clusters <- renderTable({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(NULL)
    }
    
    datos_agregados <- datos %>%
      filter(estado == "Pendiente (Predicha)") %>%
      group_by(cluster) %>%
      summarise_all(mean, na.rm = TRUE) %>%
      mutate(cluster = factor(cluster, levels = unique(cluster)))
    
    return(datos_agregados)
  })
  
  
  
  
  
  
  ###################### PERFIL CLUSTER (RADAR) #################################
  
  
  observe({
    datos <- datos_filtrados()  # Usamos los datos filtrados
    
    if (!is.null(datos) && "cluster" %in% names(datos)) {
      clusters_disponibles <- sort(unique(na.omit(datos$cluster)))
      
      updateSelectInput(
        session, 
        "cluster_seleccionado",
        choices = clusters_disponibles,
        selected = ifelse(length(clusters_disponibles) > 0, 
                          clusters_disponibles[1], 
                          NULL)
      )
    } else {
      updateSelectInput(
        session,
        "cluster_seleccionado",
        choices = character(0),
        selected = NULL
      )
    }
  })
  
  # Gráfico radar reactivo a la selección del cluster
  output$radar_clusters <- renderPlotly({
    datos <- datos_filtrados()  # Usamos los datos filtrados
    req(datos, input$cluster_seleccionado)  # Requerimos datos y selección
    
    # Verificar que existe la columna cluster
    if (!"cluster" %in% names(datos)) {
      return(plotly_empty() %>%
               layout(
                 title = "No se encontró información de clusters",
                 paper_bgcolor = "#34495E",
                 plot_bgcolor = "#34495E",
                 font = list(color = "#FFFFFF")
               ))
    }
    
    # Definir explícitamente las variables numéricas para el radar
    vars_radar <- c(
      "edad", "ingresos_anuales", "puntaje_crediticio",
      "deuda_actual", "antiguedad_laboral", "numero_dependientes"
    )
    
    # Filtrar variables disponibles y asegurarse de que sean numéricas
    vars_disponibles <- vars_radar[vars_radar %in% names(datos)]
    datos_numeric <- datos %>% select(all_of(vars_disponibles))
    vars_numeric <- names(datos_numeric)[sapply(datos_numeric, is.numeric)]
    
    if (length(vars_numeric) < 3) {
      return(plotly_empty() %>%
               layout(
                 title = "No hay suficientes variables numéricas para el radar (mínimo 3)",
                 paper_bgcolor = "#34495E",
                 plot_bgcolor = "#34495E",
                 font = list(color = "#FFFFFF")
               ))
    }
    
    tryCatch({
      # Filtrar datos por el cluster seleccionado y calcular promedios
      datos_radar <- datos %>%
        filter(cluster == input$cluster_seleccionado) %>%
        select(all_of(vars_numeric)) %>%
        summarise(across(everything(), ~ mean(., na.rm = TRUE)))
      
      # Depuración: Imprimir datos_radar
      message("Datos radar:")
      print(datos_radar)
      
      # Verificar si hay datos válidos
      if (nrow(datos_radar) == 0 || all(is.na(datos_radar))) {
        return(plotly_empty() %>%
                 layout(
                   title = paste("No hay datos para el cluster", input$cluster_seleccionado),
                   paper_bgcolor = "#34495E",
                   plot_bgcolor = "#34495E",
                   font = list(color = "#FFFFFF")
                 ))
      }
      
      # Normalización (0-100) basada en los valores de todos los datos filtrados
      datos_all <- datos %>% select(all_of(vars_numeric))
      rangos <- apply(datos_all, 2, function(x) range(x, na.rm = TRUE))
      datos_normalizados <- datos_radar %>%
        mutate(across(everything(), ~ scales::rescale(., to = c(0, 100), 
                                                      from = rangos[, cur_column()])))
      
      # Depuración: Imprimir datos_normalizados
      message("Datos normalizados:")
      print(datos_normalizados)
      
      # Verificar que los datos normalizados sean válidos
      if (any(is.na(datos_normalizados))) {
        return(plotly_empty() %>%
                 layout(
                   title = "Datos normalizados contienen valores NA",
                   paper_bgcolor = "#34495E",
                   plot_bgcolor = "#34495E",
                   font = list(color = "#FFFFFF")
                 ))
      }
      
      # Crear el gráfico radar
      p <- plot_ly(
        type = 'scatterpolar',
        r = as.numeric(datos_normalizados[1, ]),
        theta = names(datos_normalizados),
        fill = 'toself',
        mode = 'lines+markers',
        line = list(color = '#1ABC9C', width = 2),
        marker = list(color = '#1ABC9C', size = 8),
        hoverinfo = 'text',
        text = paste0(
          names(datos_radar), ": ",
          round(as.numeric(datos_radar[1, ]), 2),
          " (", round(as.numeric(datos_normalizados[1, ]), 0), "%)"
        ),
        name = paste("Cluster", input$cluster_seleccionado)
      )
      
      # Simplificar el layout para evitar problemas
      p <- p %>% layout(
        title = paste("Perfil del Cluster", input$cluster_seleccionado),
        polar = list(
          radialaxis = list(
            visible = TRUE,
            range = c(0, 100),
            tickvals = seq(0, 100, 25),
            ticktext = c("0%", "25%", "50%", "75%", "100%")
          )
        ),
        paper_bgcolor = "#34495E",
        plot_bgcolor = "#34495E",
        font = list(color = "black"),
        showlegend = TRUE
      )
      
      return(p)
      
    }, error = function(e) {
      message("Error en el radar: ", e$message)
      plotly_empty() %>%
        layout(
          title = paste("Error al generar el radar:", e$message),
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF")
        )
    })
  })
  
  
  
  
  
  ########################## TABLA (PREDICCIONES) #################################
  
  
  datos_deep <- reactive({
    datos <- datos_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(NULL)
    }
    
    # Filter for "Pendiente (Predicha)" and transform id_cliente
    datos <- datos %>%
      filter(estado == "Pendiente (Predicha)") %>%
      mutate(
        id_cliente = sub(".*?-", "", id_cliente),  # Extract characters after the first "-"
        prediccion = ifelse(predicted_solicitud_credito == 1, "Aprobada", "Rechazada")
      )
    
    # Apply prediction filter
    filtro <- input$filtro_prediccion
    if (filtro == "Aprobadas") {
      datos <- datos %>% filter(predicted_solicitud_credito == 1)
    } else if (filtro == "Rechazadas") {
      datos <- datos %>% filter(predicted_solicitud_credito == 0)
    }
    
    return(datos)
  })
  
  
  # Selector de columnas para table_data_deep
  output$deep_column_selector <- renderUI({
    req(datos_deep())  # Asegura que los datos estén disponibles
    datos <- datos_deep()
    if (!is.null(datos) && nrow(datos) > 0) {
      selectInput(
        "deep_search_column",
        "Filtrar en la columna:",
        choices = c("Todas", names(datos)),
        selected = "Todas"
      )
    } else {
      NULL
    }
  })
  
  # Render the Deep-Dive Table with Scrollbar, Export Buttons, and Column Search
  # Selector de columnas para table_data_deep
  output$deep_column_selector <- renderUI({
    req(datos_deep())  # Asegura que los datos estén disponibles
    datos <- datos_deep()
    if (!is.null(datos) && nrow(datos) > 0) {
      selectInput(
        "deep_search_column",
        "Filtrar en la columna:",
        choices = c("Todas", names(datos)),
        selected = "Todas"
      )
    } else {
      NULL
    }
  })
  
  # Render the Deep-Dive Table with Scrollbar, Export Buttons, and Column Search
  output$table_data_deep <- renderDT({
    datos <- datos_deep()
    if (is.null(datos) || nrow(datos) == 0) {
      return(datatable(data.frame(Mensaje = "No hay datos disponibles"),
                       options = list(dom = "t", pageLength = 5),
                       rownames = FALSE))
    }
    
    datatable(
      datos,
      options = list(
        dom = "lrtipB",
        buttons = c("copy", "csv", "excel", "pdf", "print"),
        pageLength = 10,
        lengthMenu = c(5, 10, 25, 50),
        searchHighlight = FALSE,
        autoWidth = FALSE,
        scrollX = TRUE,
        columnDefs = list(list(className = "dt-center", targets = "_all")),
        # Configurar búsqueda en columna específica
        searchCols = if (!is.null(input$deep_search_column) && input$deep_search_column != "Todas") {
          lapply(names(datos), function(col) {
            if (col == input$deep_search_column) {
              NULL  # Permitir búsqueda en la columna seleccionada
            } else {
              list(search = "")  # Desactivar búsqueda en otras columnas
            }
          })
        } else {
          NULL
        }
      ),
      rownames = FALSE,
      class = "display",
      extensions = "Buttons",
      style = "bootstrap",
      caption = "Datos Filtrados de Solicitudes Pendientes (Predichas)"
    ) %>%
      formatStyle(
        columns = names(datos),
        backgroundColor = "#34495E",
        color = "#FFFFFF",
        fontSize = "12px"
      ) %>%
      formatCurrency(columns = intersect(names(datos), c("ingresos_anuales", "deuda_actual")), currency = "$", digits = 0) %>%
      formatDate(columns = intersect(names(datos), "fecha_solicitud"), method = "toLocaleDateString")
  })
  
  ######## PESTAÑA: FORECAST #####################################################################
  
  datos_forecast_filtrados <- reactive({
    req(rv_forecast$datos_forecast)
    datos <- rv_forecast$datos_forecast
    
    if (nrow(datos) == 0) return(datos)
    
    # Asegurar que fecha_mes sea tipo Date
    datos <- datos %>% mutate(fecha_mes = as.Date(fecha_mes))
    
    # Filtrar los últimos 18 meses desde la fecha más reciente
    max_fecha <- max(datos$fecha_mes, na.rm = TRUE)
    datos <- datos %>%
      filter(fecha_mes >= max_fecha %m-% months(17))
    
    datos
  })
  
  # Gráfica 1: Pronóstico de Solicitudes de Crédito
  output$forecast_plot_solicitudes <- renderPlotly({
    datos <- datos_forecast_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      p <- ggplot(datos, aes(x = fecha_mes)) +
        geom_ribbon(aes(ymin = total_solicitudes_lower, 
                        ymax = total_solicitudes_upper),
                    fill = "grey70", alpha = 0.3) +
        geom_line(aes(y = total_solicitudes, color = tipo_dato), size = 1) +
        scale_color_manual(values = c("Histórico" = "#3498DB", "Predicción" = "#E74C3C")) +
        labs(title = "Pronóstico de Solicitudes de Crédito (Últimos 18 Meses)", 
             x = "Fecha", 
             y = "Cantidad",
             color = NULL) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.position = "bottom",
          legend.background = element_blank(),
          legend.box.background = element_blank()
        )
      
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "x unified",
          yaxis = list(tickformat = ",")
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # Gráfica 2: Pronóstico de Aprobaciones
  output$forecast_plot_aprobaciones <- renderPlotly({
    datos <- datos_forecast_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      p <- ggplot(datos, aes(x = fecha_mes)) +
        geom_ribbon(aes(ymin = solicitudes_aprobadas_lower, 
                        ymax = solicitudes_aprobadas_upper),
                    fill = "grey70", alpha = 0.3) +
        geom_line(aes(y = solicitudes_aprobadas, color = tipo_dato), size = 1) +
        scale_color_manual(values = c("Histórico" = "#3498DB", "Predicción" = "#E74C3C")) +
        labs(title = "Pronóstico de Aprobaciones (Últimos 18 Meses)", 
             x = "Fecha", 
             y = "Cantidad",
             color = NULL) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.position = "bottom",
          legend.background = element_blank(),
          legend.box.background = element_blank()
        )
      
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "x unified",
          yaxis = list(tickformat = ",")
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  # Gráfica 3: Pronóstico de Tasa de Aprobación
  output$forecast_plot_tasa <- renderPlotly({
    datos <- datos_forecast_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(plotly_empty() %>% 
               layout(title = "No hay datos disponibles",
                      paper_bgcolor = "#34495E",
                      plot_bgcolor = "#34495E",
                      font = list(color = "#FFFFFF")))
    }
    
    tryCatch({
      p <- ggplot(datos, aes(x = fecha_mes)) +
        geom_ribbon(aes(ymin = tasa_aprobacion_lower, 
                        ymax = tasa_aprobacion_upper),
                    fill = "grey70", alpha = 0.3) +
        geom_line(aes(y = tasa_aprobacion, color = tipo_dato), size = 1) +
        scale_color_manual(values = c("Histórico" = "#3498DB", "Predicción" = "#E74C3C")) +
        labs(title = "Pronóstico de Tasa de Aprobación (Últimos 18 Meses)", 
             x = "Fecha", 
             y = "Porcentaje",
             color = NULL) +
        scale_y_continuous(labels = function(x) sprintf("%.1f%%", x )) +
        theme_minimal() +
        theme(
          plot.background = element_rect(fill = "#34495E", color = NA),
          panel.background = element_rect(fill = "#34495E", color = NA),
          text = element_text(color = "#FFFFFF"),
          axis.text = element_text(color = "#FFFFFF"),
          legend.text = element_text(color = "#FFFFFF"),
          legend.position = "bottom",
          legend.background = element_blank(),
          legend.box.background = element_blank()
        )
      
      ggplotly(p) %>% 
        layout(
          paper_bgcolor = "#34495E",
          plot_bgcolor = "#34495E",
          font = list(color = "#FFFFFF"),
          hovermode = "x unified"
        )
    }, error = function(e) {
      plotly_empty() %>% 
        layout(title = paste("Error en el gráfico:", e$message),
               paper_bgcolor = "#34495E",
               plot_bgcolor = "#34495E",
               font = list(color = "#FFFFFF"))
    })
  })
  
  
  
  # ------------- TABLA --------------------
  
  
  # Selector de columnas para Forecast
  output$forecast_column_selector <- renderUI({
    datos <- datos_forecast_filtrados()
    if (!is.null(datos) && nrow(datos) > 0) {
      selectInput(
        "forecast_search_column",
        "Filtrar en la columna:",
        choices = c("Todas", names(datos)),
        selected = "Todas"
      )
    } else {
      NULL
    }
  })
  
  # Tabla Forecast modificada
  output$forecast_table <- renderDT({
    datos <- datos_forecast_filtrados()
    if (is.null(datos) || nrow(datos) == 0) {
      return(datatable(data.frame(Mensaje = "No hay datos disponibles"),
                       options = list(dom = "t", pageLength = 5),
                       rownames = FALSE))
    }
    
    # Formatear columnas numéricas para visualización
    datos_display <- datos %>%
      mutate(
        tasa_aprobacion = sprintf("%.1f%%", tasa_aprobacion),
        tasa_aprobacion_lower = sprintf("%.1f%%", tasa_aprobacion_lower),
        tasa_aprobacion_upper = sprintf("%.1f%%", tasa_aprobacion_upper),
        total_solicitudes = format(round(total_solicitudes), big.mark = ","),
        total_solicitudes_lower = format(round(total_solicitudes_lower), big.mark = ","),
        total_solicitudes_upper = format(round(total_solicitudes_upper), big.mark = ","),
        solicitudes_aprobadas = format(round(solicitudes_aprobadas), big.mark = ","),
        solicitudes_aprobadas_lower = format(round(solicitudes_aprobadas_lower), big.mark = ","),
        solicitudes_aprobadas_upper = format(round(solicitudes_aprobadas_upper), big.mark = ",")
      )
    
    datatable(
      datos_display,
      options = list(
        dom = "lrtipB",
        buttons = c("copy", "csv", "excel", "pdf", "print"),
        pageLength = 10,
        lengthMenu = c(5, 10, 25, 50),
        searchHighlight = FALSE,
        autoWidth = FALSE,
        scrollX = TRUE,
        order = list(list(0, 'desc')),
        columnDefs = list(list(className = "dt-center", targets = "_all")),
        # Configurar búsqueda en columna específica
        searchCols = if (!is.null(input$forecast_search_column) && input$forecast_search_column != "Todas") {
          lapply(names(datos_display), function(col) {
            if (col == input$forecast_search_column) NULL else list(search = "")
          })
        } else {
          NULL
        }
      ),
      rownames = FALSE,
      class = "display",
      extensions = "Buttons",
      style = "bootstrap",
      caption = "Datos de Pronósticos (Últimos 18 Meses)"
    ) %>%
      formatStyle(
        columns = names(datos_display),
        backgroundColor = "#34495E",
        color = "#FFFFFF",
        fontSize = "12px"
      ) %>%
      formatDate(columns = intersect(names(datos_display), "fecha_mes"), method = "toLocaleDateString")
  })
  
  
  
  
  ############################# MÓDULO: TABLAS ##############################################
  
  
  # Selector de columnas para Tablas
  output$tables_column_selector <- renderUI({
    req(input$table_selector)
    datos <- load_table_data(input$table_selector)
    if (!is.null(datos) && nrow(datos) > 0) {
      selectInput(
        "tables_search_column",
        "Filtrar en la columna:",
        choices = c("Todas", names(datos)),
        selected = "Todas"
      )
    } else {
      NULL
    }
  })
  
  # Tabla del módulo Tablas modificada
  output$table_list <- renderDT({
    req(input$table_selector)
    datos <- load_table_data(input$table_selector)
    if (is.null(datos) || nrow(datos) == 0) {
      return(datatable(data.frame(Mensaje = "No hay datos disponibles"),
                       options = list(dom = "t", pageLength = 5),
                       rownames = FALSE))
    }
    
    datatable(
      datos,
      options = list(
        dom = "lrtipB",
        buttons = c("copy", "csv", "excel", "pdf", "print"),
        pageLength = 10,
        lengthMenu = c(5, 10, 25, 50),
        searchHighlight = FALSE,
        autoWidth = FALSE,
        scrollX = TRUE,
        columnDefs = list(list(className = "dt-center", targets = "_all")),
        # Configurar búsqueda en columna específica
        searchCols = if (!is.null(input$tables_search_column) && input$tables_search_column != "Todas") {
          lapply(names(datos), function(col) {
            if (col == input$tables_search_column) NULL else list(search = "")
          })
        } else {
          NULL
        }
      ),
      rownames = FALSE,
      class = "display",
      extensions = "Buttons",
      style = "bootstrap",
      caption = paste("Datos de la Tabla:", input$table_selector)
    ) %>%
      formatStyle(
        columns = names(datos),
        backgroundColor = "#34495E",
        color = "#FFFFFF",
        fontSize = "12px"
      ) %>%
      formatCurrency(columns = intersect(names(datos), c("ingresos_anuales", "deuda_actual")), currency = "$", digits = 0) %>%
      formatDate(columns = intersect(names(datos), "fecha_solicitud"), method = "toLocaleDateString")
  })
  
}



# RUN APP ----
shinyApp(ui, server)