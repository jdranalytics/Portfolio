library(shiny)
library(bslib)
library(shinyWidgets)
library(dplyr)
library(tidygraph)
library(visNetwork)
library(plotly)
library(viridis)
library(lubridate)
library(corrplot)
library(forecast)
library(randomForest)
library(caret)
library(pROC)
library(rpart)
library(rpart.plot)
library(webshot)
library(ggplot2)
library(DT)
library(tm)
library(SnowballC)
library(wordcloud)
library(tidytext)
library(syuzhet)
library(tm)
library(text2vec)




DEFAULT_URL <- "https://raw.githubusercontent.com/ringoquimico/Portfolio/refs/heads/main/Data%20Sources/call_center_data.csv"
url_training <- "https://raw.githubusercontent.com/ringoquimico/Portfolio/refs/heads/main/Data%20Sources/call_center_data.csv"

ui <- fluidPage(
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  tags$head(
    tags$style(HTML("
      .dark-mode .nav-tabs > li > a { color: green !important; }
      .dark-mode .nav-tabs > li.active > a { color: white !important; }
      .compact-alert { padding: 5px; margin-bottom: 5px; font-size: 12px; }
      #run_status { margin-top: 10px; font-size: 14px; }
    "))
  ),
  titlePanel("ConectApp 1.0.0"),
  sidebarLayout(
    sidebarPanel(
      textInput("data_url", "URL de la Fuente de Datos:", 
                value = DEFAULT_URL,
                placeholder = "Ingrese la URL del archivo CSV"),
      uiOutput("loading_indicator"),
      dateRangeInput("date_range", "Rango de Fechas:",
                     start = NULL, end = NULL, min = NULL, max = NULL,
                     format = "yyyy-mm-dd"),
      selectInput("channel", "Channel:",
                  choices = c("Todos" = "All")),
      selectInput("group", "CSAT Rated Group Name:",
                  choices = c("Todos" = "All")),
      selectInput("issue", "Issue Classification:",
                  choices = c("Todos" = "All")),
      switchInput("toggle_mode", "Modo Oscuro", value = FALSE),
      actionButton("run_btn", "Ejecutar", class = "btn-primary"),
      uiOutput("run_status"),
      verbatimTextOutput("debug_info")
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Estadísticas",
                 fluidRow(
                   column(6, h3("CSAT Score"), 
                          verbatimTextOutput("csat_score"),
                          plotlyOutput("csat_histogram", height = "200px")),
                   column(6, h3("SLA"), 
                          verbatimTextOutput("sla_stats"),
                          plotlyOutput("sla_histogram", height = "200px")),
                   column(6, h3("Resolution Time"), 
                          verbatimTextOutput("resolution_time"),
                          plotlyOutput("rt_histogram", height = "200px")),
                   column(6, h3("Resolution Groups"), 
                          verbatimTextOutput("resolution_groups"),
                          plotlyOutput("rg_histogram", height = "200px"))
                 )),
        tabPanel("Tendencias",
                 plotlyOutput("csat_trend", height = "500px"),
                 plotlyOutput("resolution_time_trend", height = "300px"),
                 plotlyOutput("resolution_groups_trend", height = "300px"),
                 plotlyOutput("sla_trend", height = "500px")),
        tabPanel("Correlograma",
                 plotOutput("corr_plot", height = "400px"),
                 plotlyOutput("impact_bar", height = "200px")),
        tabPanel("Boxplots",
                 selectInput("boxplot_metric", "Seleccionar Métrica:",
                             choices = c("CSAT Score" = "CSAT_Score",
                                         "CSAT Rating Received" = "csat_rating_received",
                                         "SLA" = "resolved_in_sla",
                                         "Resolution Groups" = "total_groups",
                                         "Resolution Time" = "resolution_time_day")),
                 plotlyOutput("channel_boxplot", height = "300px"),
                 plotlyOutput("group_boxplot", height = "300px"),
                 checkboxGroupInput("issue_filter", "Filtrar Issue Classification:",
                                    choices = NULL, selected = NULL),
                 plotlyOutput("issue_boxplot", height = "300px")),
        
        tabPanel("Red de Interacciones", 
                 visNetworkOutput("network", height = "600px")),
        tabPanel("Análisis de Redes (Deep Dive)",
                 fluidRow(
                   column(12, 
                          h3("Top 10 Interacciones (Frecuencia)"),
                          plotlyOutput("top_interactions"),
                          h3("Top 10 Interacciones Problemáticas"),
                          plotlyOutput("top_problematic_interactions"))
                 ),
                 fluidRow(
                   column(12, h3("Red Interactiva con Impacto"),
                          visNetworkOutput("network_deep_dive", height = "600px"))
                 ),
                 fluidRow(
                   column(12, h3("Top 5 Nodos Problemáticos (Centralidad)"),
                          DT::DTOutput("centrality_table"))
                 ),
                 fluidRow(
                   column(12, h3("Top 5 Caminos Problemáticos"),
                          DT::DTOutput("critical_paths_table"))
                 ),
                 fluidRow(
                   column(12, h3("Clústeres de Interacciones"),
                          visNetworkOutput("cluster_network", height = "400px"))
                 )),
        tabPanel("Random Forest",
                 fluidRow(
                   column(12, h3("Resumen"), 
                          verbatimTextOutput("rf_summary"))
                 ),
                 fluidRow(
                   column(12, h3("Predicción"), 
                          numericInput("pred_groups_rf", "Número de Grupos:", value = 1, min = 1),
                          numericInput("pred_time_rf", "Tiempo de Resolución (días):", value = 1, min = 0, step = 0.1),
                          selectInput("pred_sla_rf", "Resuelto en SLA:", choices = c("Sí" = 1, "No" = 0)),
                          actionButton("predict_btn_rf", "Predecir"),
                          verbatimTextOutput("rf_prediction"))
                 )),
        tabPanel("Árbol de Decisión",
                 fluidRow(
                   column(12, style = "width: 1200px;", h3("Resumen"), 
                          verbatimTextOutput("dt_summary"))
                 ),
                 fluidRow(
                   column(12, h3("Predicción"), 
                          numericInput("pred_groups_dt", "Número de Grupos:", value = 1, min = 1),
                          numericInput("pred_time_dt", "Tiempo de Resolución (días):", value = 1, min = 0, step = 0.1),
                          selectInput("pred_sla_dt", "Resuelto en SLA:", choices = c("Sí" = 1, "No" = 0)),
                          actionButton("predict_btn_dt", "Predecir"),
                          downloadButton("download_dt_plot", "Descargar Árbol (PNG)"),
                          verbatimTextOutput("dt_prediction"))
                 )),
        tabPanel("Análisis de Sentimiento",
                 fluidRow(
                   column(12,
                          actionButton("sentiment_run_btn", "Iniciar Análisis de Sentimiento", class = "btn-primary"),
                          uiOutput("sentiment_run_status"),
                          h3("Resultados del Modelo"),
                          verbatimTextOutput("sentiment_results"),
                          h3("Comparación Train vs Test"),
                          plotOutput("sentiment_train_test_plot", height = "300"),
                          h3("Proporciones de Sentimientos Predichos"),
                          plotOutput("sentiment_proportions_plot", height = "300px"),
                          h3("Distribución de Issues"),
                          plotOutput("sentiment_issues_plot", height = "300px"),
                          h3("Tendencia de Percepción Positiva"),
                          plotlyOutput("sentiment_positive_trend", height = "350px"),
                          h3("Wordcloud de Palabras Frecuentes"),
                          plotOutput("sentiment_wordcloud", height = "80px"),
                          column(12, plotOutput("sentiment_wordcloud")),
                          h3("Resumen de casos clasificados Negativos y Neutrales"),
                          column(12, dataTableOutput("sentiment_details_table")),
                          column(12, downloadButton("download_details", "Descargar Detalles"))
                          ) )
        ),
        tabPanel("Datos Mensuales",
                 h3("Datos Tabulados por Mes"),
                 DT::DTOutput("monthly_table"),
                 downloadButton("download_csv", "Descargar CSV"))
      )
    )
  )
)


server <- function(input, output, session) {
  light_theme <- bs_theme(version = 5, bootswatch = "flatly")
  dark_theme <- bs_theme(version = 5, bootswatch = "darkly")
  
  run_status <- reactiveVal("")
  sentiment_run_status <- reactiveVal("")
  
  data <- reactive({
    req(input$data_url)
    tryCatch({
      df <- read.csv(input$data_url, sep = ";", quote = '"', encoding = "UTF-8") %>%
        mutate(
          date = as.Date(date, format = "%Y-%m-%d"),
          start_of_week = as.Date(start_of_week, format = "%Y-%m-%d"),
          start_of_month = as.Date(start_of_month, format = "%Y-%m-%d"),
          resolution_time_day = as.double(gsub(",", ".", resolution_time_min)) / 1440,
          month_year = format(start_of_month, "%Y-%m"),
          resolved_in_sla = case_when(
            is.na(resolved_in_sla) & resolution_time_day < 60 / 1440 ~ 1,
            is.na(resolved_in_sla) ~ 0,
            TRUE ~ as.numeric(resolved_in_sla)
          ),
          resolution_time_day = case_when(
            is.na(resolution_time_day) & (resolved_in_sla == 1 | classification == "PROMOTER") ~ 0.5,
            is.na(resolution_time_day) ~ 1,
            TRUE ~ resolution_time_day
          )
        )
      df
    }, error = function(e) {
      showNotification("Error al cargar los datos. Verifique la URL.", type = "error")
      NULL
    })
  })
  
  observeEvent(data(), {
    df <- data()
    if (!is.null(df) && nrow(df) > 0) {
      updateDateRangeInput(session, "date_range",
                           start = min(df$date, na.rm = TRUE),
                           end = max(df$date, na.rm = TRUE),
                           min = min(df$date, na.rm = TRUE),
                           max = max(df$date, na.rm = TRUE))
      updateSelectInput(session, "channel",
                        choices = c("Todos" = "All", sort(unique(df$channel))))
      updateSelectInput(session, "group",
                        choices = c("Todos" = "All", sort(unique(df$csat_rated_group_name))))
      updateSelectInput(session, "issue",
                        choices = c("Todos" = "All", sort(unique(df$issue_classification))))
      
      output$loading_indicator <- renderUI({
        div(class = "alert alert-success compact-alert",
            "Datos cargados: ", nrow(df), " filas")
      })
    }
  })
  
  observeEvent(input$toggle_mode, {
    if (input$toggle_mode) {
      session$setCurrentTheme(dark_theme)
      session$sendCustomMessage(type = "toggleDarkMode", message = TRUE)
    } else {
      session$setCurrentTheme(light_theme)
      session$sendCustomMessage(type = "toggleDarkMode", message = FALSE)
    }
  })
  
  filtered_data <- reactive({
    req(data(), input$date_range[1], input$date_range[2])
    df <- data()
    
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    df <- df %>% filter(date >= input$date_range[1],
                        date <= input$date_range[2])
    
    if (input$channel != "All") {
      df <- df %>% filter(channel == input$channel)
    }
    
    if (input$group != "All") {
      df <- df %>% filter(csat_rated_group_name == input$group)
    }
    
    if (input$issue != "All") {
      df <- df %>% filter(issue_classification == input$issue)
    }
    
    df %>% filter(!is.na(group_name_history) & group_name_history != "")
  })
  
  processed_data <- eventReactive(input$run_btn, {
    run_status("Ejecutando...")
    result <- filtered_data()
    run_status("¡Listo!")
    result
  })
  
  output$run_status <- renderUI({
    status <- run_status()
    if (status == "Ejecutando...") {
      div(id = "run_status", style = "color: orange;", status)
    } else if (status == "¡Listo!") {
      div(id = "run_status", style = "color: green;", status)
    } else {
      div(id = "run_status", "")
    }
  })
  
  output$debug_info <- renderPrint({
    df <- processed_data()
    if (is.null(df)) {
      cat("No hay datos disponibles o no se ha ejecutado\n")
    } else {
      cat("Encuestas analizadas:", nrow(df), "\n")
    }
  })
  

 
  ################## SENTIMENT ANALYSIS #####################################
  
  # Estado inicial
  sentiment_run_status <- reactiveVal("")
  
  # Resultado del análisis (usar eventReactive para persistir)
  sentiment_analysis_result <- eventReactive(input$sentiment_run_btn, {
    sentiment_run_status("Ejecutando análisis de sentimiento...")
    message("Botón presionado: Iniciando análisis de sentimiento")
    
    withProgress(message = "Procesando análisis de sentimiento...", value = 0, {
      set.seed(123)
      
      incProgress(0.1, detail = "Cargando datos de entrenamiento...")
      df_train <- tryCatch({
        read.csv(url_training, sep = ";", quote = "\"", encoding = "UTF-8") %>%
          select(case_id, translated_comments, sentiment_rate, start_of_month) %>%
          rename(
            CASE.ID = case_id,
            TRANSLATED.COMMENTS = translated_comments,
            SENTIMENT_RATE = sentiment_rate,
            START_OF_MONTH = start_of_month
          ) %>%
          mutate(sentiment_category = factor(
            case_when(
              SENTIMENT_RATE %in% c(1, 2) ~ "Negativo",
              SENTIMENT_RATE == 3 ~ "Neutral",
              SENTIMENT_RATE %in% c(4, 5) ~ "Positivo"
            ),
            levels = c("Negativo", "Neutral", "Positivo")
          ))
      }, error = function(e) {
        sentiment_run_status(paste("Error al cargar datos:", e$message))
        message("Error cargando datos:", e$message)
        return(NULL)
      })
      
      if (is.null(df_train)) {
        message("df_train es NULL, terminando ejecución")
        return(NULL)
      }
      message("Datos de entrenamiento cargados: ", nrow(df_train), " filas")
      
      trainIndex <- createDataPartition(df_train$sentiment_category, p = 0.8, list = FALSE)
      train_data <- df_train[trainIndex, ]
      test_data <- df_train[-trainIndex, ]
      
      preprocess_text <- function(text) {
        text <- ifelse(is.na(text) | text == "", "empty", text)
        corpus <- VCorpus(VectorSource(text))
        corpus <- tm_map(corpus, content_transformer(tolower))
        corpus <- tm_map(corpus, removePunctuation)
        corpus <- tm_map(corpus, removeNumbers)
        corpus <- tm_map(corpus, removeWords, stopwords("english"))
        corpus <- tm_map(corpus, stemDocument)
        corpus <- tm_map(corpus, stripWhitespace)
        return(corpus)
      }
      
      incProgress(0.3, detail = "Preprocesando texto...")
      train_corpus <- preprocess_text(train_data$TRANSLATED.COMMENTS)
      test_corpus <- preprocess_text(test_data$TRANSLATED.COMMENTS)
      
      dtm_train <- DocumentTermMatrix(train_corpus, control = list(weighting = weightTfIdf))
      dtm_test <- DocumentTermMatrix(
        test_corpus,
        control = list(dictionary = Terms(dtm_train), weighting = weightTfIdf)
      )
      
      train_matrix <- as.data.frame(as.matrix(dtm_train))
      test_matrix <- as.data.frame(as.matrix(dtm_test))
      colnames(train_matrix) <- make.names(colnames(train_matrix))
      colnames(test_matrix) <- make.names(colnames(test_matrix))
      common_cols <- intersect(colnames(train_matrix), colnames(test_matrix))
      train_matrix <- train_matrix[, common_cols, drop = FALSE]
      test_matrix <- test_matrix[, common_cols, drop = FALSE]
      train_matrix$sentiment_category <- train_data$sentiment_category
      test_matrix$sentiment_category <- test_data$sentiment_category
      message("Matrices creadas: train=", nrow(train_matrix), " filas, test=", nrow(test_matrix), " filas")
      
      incProgress(0.6, detail = "Entrenando modelo...")
      train_control <- trainControl(method = "cv", number = 5)
      rf_model <- tryCatch({
        train(
          sentiment_category ~ .,
          data = train_matrix,
          method = "rf",
          trControl = train_control,
          tuneGrid = data.frame(mtry = sqrt(ncol(train_matrix) - 1)),
          ntree = 50
        )
      }, error = function(e) {
        sentiment_run_status(paste("Error al entrenar modelo:", e$message))
        message("Error entrenando modelo:", e$message)
        return(NULL)
      })
      
      if (is.null(rf_model)) {
        message("rf_model es NULL, terminando ejecución")
        return(NULL)
      }
      message("Modelo entrenado exitosamente")
      
      train_pred <- predict(rf_model, train_matrix)
      test_pred <- predict(rf_model, test_matrix)
      
      train_accuracy <- mean(train_pred == train_data$sentiment_category, na.rm = TRUE)
      test_accuracy <- mean(test_pred == test_data$sentiment_category, na.rm = TRUE)
      cm <- confusionMatrix(test_pred, test_data$sentiment_category)
      
      train_test_plot <- tryCatch({
        results <- data.frame(
          Set = c(rep("Train", length(train_pred)), rep("Test", length(test_pred))),
          Actual = c(as.character(train_data$sentiment_category), as.character(test_data$sentiment_category)),
          Predicted = c(as.character(train_pred), as.character(test_pred))
        )
        message("Datos para train_test_plot: ", nrow(results), " filas")
        if (nrow(results) == 0) {
          message("No hay datos para generar train_test_plot")
          return(NULL)
        }
        max_y <- max(table(results$Actual, results$Predicted))
        max_y_expanded <- max_y + (max_y * 0.5)  # Aumentar 50% para más espacio
        message("Máximo Y para train_test_plot (expandido): ", max_y_expanded)
        ggplot(results, aes(x = Actual, fill = Predicted)) +
          geom_bar(position = "dodge") +
          facet_wrap(~Set) +
          labs(x = "Sentimiento Real", y = "Frecuencia", title = NULL) +  # Eliminar título
          theme_minimal() +
          theme(
            plot.title = element_blank(),  # Asegurar que el título esté eliminado
            axis.title = element_text(size = 14),
            axis.text = element_text(size = 14),
            strip.text = element_text(size = 14)
          ) +
          scale_fill_brewer(palette = "Set1") +
          geom_text(
            stat = "count",
            aes(label = after_stat(count)),
            position = position_dodge(width = 1),
            vjust = -0.5,
            size = 5
          ) +
          ylim(0, max_y_expanded)  # Escala Y más amplia
      }, error = function(e) {
        message("Error generando train_test_plot: ", e$message)
        return(NULL)
      })
      message("Gráfico train_test_plot generado: ", ifelse(is.null(train_test_plot), "NULL", "OK"))
      
      incProgress(0.8, detail = "Analizando datos de producción...")
      df_produccion <- processed_data()
      if (is.null(df_produccion) || nrow(df_produccion) == 0) {
        sentiment_run_status("No hay datos de producción con los filtros actuales.")
        message("No hay datos de producción")
        return(list(
          train_accuracy = train_accuracy,
          test_accuracy = test_accuracy,
          cm = cm,
          ntree = rf_model$finalModel$ntree,
          mtry = rf_model$finalModel$mtry,
          cv_folds = train_control$number,
          train_test_plot = train_test_plot,
          proportions_plot = NULL,
          issues_plot = NULL,
          sentiment_trend_data = NULL,
          wordcloud_corpus = NULL,
          df_produccion = NULL
        ))
      }
      message("Datos de producción cargados: ", nrow(df_produccion), " filas")
      
      df_produccion <- df_produccion %>%
        select(case_id, translated_comments, start_of_month) %>%
        rename(
          CASE.ID = case_id,
          TRANSLATED.COMMENTS = translated_comments,
          START_OF_MONTH = start_of_month
        )
      
      prod_corpus <- preprocess_text(df_produccion$TRANSLATED.COMMENTS)
      dtm_prod <- DocumentTermMatrix(
        prod_corpus,
        control = list(dictionary = Terms(dtm_train), weighting = weightTfIdf)
      )
      prod_matrix <- as.data.frame(as.matrix(dtm_prod))
      colnames(prod_matrix) <- make.names(colnames(prod_matrix))
      prod_matrix <- prod_matrix[, common_cols, drop = FALSE]
      prod_pred <- predict(rf_model, prod_matrix)
      df_produccion$predicted_sentiment <- prod_pred
      
      message("Predicciones generadas. Valores únicos de predicted_sentiment: ", 
              paste(unique(df_produccion$predicted_sentiment), collapse = ", "))
      
      sentiment_trend_data <- df_produccion %>%
        mutate(month_year = format(START_OF_MONTH, "%Y-%m")) %>%
        group_by(month_year) %>%
        summarise(
          total_surveys = n(),
          positive_count = sum(predicted_sentiment == "Positivo", na.rm = TRUE),
          positive_percent = ifelse(total_surveys > 0, round(positive_count / total_surveys * 100, 1), NA),
          .groups = "drop"
        ) %>%
        arrange(as.Date(paste0(month_year, "-01")))
      
      last_date <- as.Date(paste0(max(sentiment_trend_data$month_year, na.rm = TRUE), "-01"))
      future_months <- format(seq(last_date %m+% months(1), by = "month", length.out = 5), "%Y-%m")
      
      if (nrow(sentiment_trend_data) >= 24) {
        ts_positive <- ts(sentiment_trend_data$positive_percent, frequency = 12)
        hw_positive <- tryCatch({
          HoltWinters(ts_positive, seasonal = "additive")
        }, error = function(e) {
          HoltWinters(ts_positive, gamma = FALSE)
        })
        pred_positive <- forecast(hw_positive, h = 5, level = 95)
        future_df <- data.frame(
          month_year = future_months,
          positive_percent = pmin(100, as.numeric(pred_positive$mean)),
          CI_Lower = pmin(100, as.numeric(pred_positive$lower)),
          CI_Upper = pmin(100, as.numeric(pred_positive$upper)),
          Is_Projection = TRUE
        )
      } else if (nrow(sentiment_trend_data) >= 2) {
        ts_positive <- ts(sentiment_trend_data$positive_percent)
        hw_positive <- HoltWinters(ts_positive, gamma = FALSE)
        pred_positive <- forecast(hw_positive, h = 5, level = 95)
        future_df <- data.frame(
          month_year = future_months,
          positive_percent = pmin(100, as.numeric(pred_positive$mean)),
          CI_Lower = pmin(100, as.numeric(pred_positive$lower)),
          CI_Upper = pmin(100, as.numeric(pred_positive$upper)),
          Is_Projection = TRUE
        )
      } else {
        future_df <- data.frame()
      }
      
      sentiment_trend_data$Is_Projection <- FALSE
      sentiment_trend_data <- bind_rows(sentiment_trend_data, future_df)
      
      wordcloud_corpus <- prod_corpus
      
      proportions_plot <- tryCatch({
        sentiment_proportions <- df_produccion %>%
          group_by(predicted_sentiment) %>%
          summarise(n = n()) %>%
          mutate(percentage = n / sum(n) * 100)
        message("Datos para proportions_plot: ", nrow(sentiment_proportions), " filas")
        if (nrow(sentiment_proportions) == 0) {
          message("No hay datos para generar proportions_plot")
          return(NULL)
        }
        max_y <- max(sentiment_proportions$percentage)
        max_y_expanded <- max_y + (max_y * 0.5)  # Aumentar 50% para más espacio
        message("Máximo Y para proportions_plot (expandido): ", max_y_expanded)
        ggplot(sentiment_proportions, aes(x = predicted_sentiment, y = percentage, fill = predicted_sentiment)) +
          geom_bar(stat = "identity") +
          labs(x = "Sentimiento Predicho", y = "Porcentaje (%)", title = NULL) +  # Eliminar título
          theme_minimal() +
          theme(
            plot.title = element_blank(),  # Asegurar que el título esté eliminado
            axis.title = element_text(size = 16),
            axis.text = element_text(size = 14)
          ) +
          scale_fill_brewer(palette = "Set2") +
          geom_text(
            aes(label = sprintf("%.1f%%", percentage)),
            vjust = -0.5,
            size = 8
          ) +
          ylim(0, max_y_expanded)  # Escala Y más amplia
      }, error = function(e) {
        message("Error generando proportions_plot: ", e$message)
        return(NULL)
      })
      message("Gráfico proportions_plot generado: ", ifelse(is.null(proportions_plot), "NULL", "OK"))
      
      predefined_issues <- list(
        "Slow Response/Delay Issues" = c("slow", "wait", "long", "delay", "late", "queue", "hold", "response", "time", "forever"),
        "Poor Agent Performance" = c("rude", "unhelpful", "confused", "incompetent", "untrained", "knowledge", "attitude", "agent"),
        "Fraud/Security Concerns" = c("fraud", "scam", "hack", "security", "trust", "safe", "unauthorized", "phishing", "breach"),
        "Account Management Issues" = c("account", "balance", "transaction", "transfer", "deposit", "withdrawal", "access", "block", "fee"),
        "Payment Processing Problems" = c("payment", "pay", "card", "declined", "charge", "refund", "billing", "overdraft", "dispute"),
        "Digital Banking Issues" = c("app", "online", "website", "login", "crash", "error", "system", "technical", "down"),
        "Lack of Communication" = c("call", "email", "chat", "chatbot" ,"contact", "unreachable", "no reply", "response", "silent", "ignored"),
        "Resolution Failures" = c("unresolved", "fix", "solve", "problem", "issue", "escalate", "nothing", "pending", "failure")
      )
      
      assign_issue <- function(comment) {
        words <- unlist(strsplit(tolower(comment), " "))
        max_overlap <- 0
        best_issue <- "Other Issues"
        for (issue in names(predefined_issues)) {
          overlap <- length(intersect(words, predefined_issues[[issue]]))
          if (overlap > max_overlap) {
            max_overlap <- overlap
            best_issue <- issue
          }
        }
        return(best_issue)
      }
      
      df_produccion$issue <- sapply(df_produccion$TRANSLATED.COMMENTS, assign_issue)
      
      message("Issues asignados. Valores únicos de issue: ", 
              paste(unique(df_produccion$issue), collapse = ", "))
      
      issues_plot <- tryCatch({
        issue_counts <- df_produccion %>%
          filter(predicted_sentiment %in% c("Neutral", "Negativo")) %>%
          group_by(issue) %>%
          summarise(n = n()) %>%
          mutate(percentage = n / sum(n) * 100) %>%
          arrange(desc(percentage))
        message("Datos para issues_plot: ", nrow(issue_counts), " filas")
        if (nrow(issue_counts) == 0) {
          message("No hay datos para generar issues_plot")
          return(NULL)
        }
        max_y <- max(issue_counts$percentage)
        max_y_expanded <- max_y + (max_y * 0.5)  # Aumentar 50% para más espacio
        message("Máximo Y para issues_plot (expandido): ", max_y_expanded)
        ggplot(issue_counts, aes(x = reorder(issue, percentage), y = percentage, fill = percentage)) +
          geom_bar(stat = "identity") +
          coord_flip() +
          labs(x = "Issues", y = "Porcentaje (%)", title = NULL) +  # Eliminar título
          theme_minimal() +
          theme(
            plot.title = element_blank(),  # Asegurar que el título esté eliminado
            axis.title = element_text(size = 14),
            axis.text = element_text(size = 14)
          ) +
          scale_fill_viridis_c() +
          geom_text(
            aes(label = sprintf("%.1f%%", percentage)),
            hjust = -0.2,
            size = 5
          ) +
          ylim(0, max_y_expanded)  # Escala Y más amplia
      }, error = function(e) {
        message("Error generando issues_plot: ", e$message)
        return(NULL)
      })
      message("Gráfico issues_plot generado: ", ifelse(is.null(issues_plot), "NULL", "OK"))
      
      incProgress(1.0, detail = "¡Completado!")
      sentiment_run_status("¡Análisis de sentimiento completado!")
      message("Análisis completado, retornando resultados")
      
      list(
        train_accuracy = train_accuracy,
        test_accuracy = test_accuracy,
        cm = cm,
        ntree = rf_model$finalModel$ntree,
        mtry = rf_model$finalModel$mtry,
        cv_folds = train_control$number,
        train_test_plot = train_test_plot,
        proportions_plot = proportions_plot,
        issues_plot = issues_plot,
        sentiment_trend_data = sentiment_trend_data,
        wordcloud_corpus = wordcloud_corpus,
        df_produccion = df_produccion
      )
    })
  })
  
  # Renderizar el estado en la UI
  output$sentiment_run_status <- renderUI({
    status <- sentiment_run_status()
    message("Renderizando sentiment_run_status: ", status)
    if (status == "Ejecutando análisis de sentimiento...") {
      div(style = "color: orange; font-weight: bold; font-size: 16px;", status)
    } else if (status == "¡Análisis de sentimiento completado!") {
      div(style = "color: green; font-weight: bold; font-size: 16px;", status)
    } else if (status == "No hay datos de producción con los filtros actuales.") {
      div(style = "color: red; font-weight: bold; font-size: 16px;", status)
    } else if (grepl("Error", status)) {
      div(style = "color: red; font-weight: bold; font-size: 16px;", status)
    } else {
      div("")
    }
  })
  
  # Vincular las salidas del análisis con manejo de errores
  output$sentiment_results <- renderPrint({
    sa <- sentiment_analysis_result()
    message("Renderizando sentiment_results, sa es ", ifelse(is.null(sa), "NULL", "no NULL"))
    if (is.null(sa)) {
      cat("El análisis de sentimiento no se ha ejecutado o falló.\n")
      message("Mostrando mensaje de error en sentiment_results")
    } else {
      tryCatch({
        message("Intentando mostrar resultados de sentiment_results")
        cat("Resultados del Análisis de Sentimiento:\n")
        cat("Matriz de Confusión:\n")
        print(sa$cm$table)
        cat("\nPrecisión en Entrenamiento:", round(sa$train_accuracy * 100, 2), "%\n")
        cat("Precisión en Prueba:", round(sa$test_accuracy * 100, 2), "%\n")
        cat("Número de Árboles:", sa$ntree, "\n")
        cat("mtry:", sa$mtry, "\n")
        cat("Pliegues CV:", sa$cv_folds, "\n")
        message("Resultados de sentiment_results mostrados")
      }, error = function(e) {
        message("Error al renderizar sentiment_results: ", e$message)
        cat("Error al mostrar los resultados: ", e$message, "\n")
      })
    }
  })
  
  output$sentiment_train_test_plot <- renderPlot({
    sa <- sentiment_analysis_result()
    message("Renderizando sentiment_train_test_plot, sa es ", ifelse(is.null(sa), "NULL", "no NULL"))
    tryCatch({
      if (is.null(sa)) {
        plot.new()
        text(0.5, 0.5, "No hay datos para mostrar el gráfico Train vs Test.", cex = 3)
        message("Mostrando mensaje de no datos en train_test_plot")
      } else if (is.null(sa$train_test_plot)) {
        plot.new()
        text(0.5, 0.5, "Gráfico Train vs Test no disponible. Revisa los mensajes de depuración.", cex = 3)
        message("Mostrando mensaje de gráfico no disponible en train_test_plot")
      } else {
        print(sa$train_test_plot)
        message("Gráfico train_test_plot renderizado")
      }
    }, error = function(e) {
      message("Error al renderizar sentiment_train_test_plot: ", e$message)
      plot.new()
      text(0.5, 0.5, paste("Error al renderizar gráfico: ", e$message), cex = 3)
    })
  })
  
  output$sentiment_proportions_plot <- renderPlot({
    sa <- sentiment_analysis_result()
    message("Renderizando sentiment_proportions_plot, sa es ", ifelse(is.null(sa), "NULL", "no NULL"))
    tryCatch({
      if (is.null(sa)) {
        plot.new()
        text(0.5, 0.5, "No hay datos para mostrar el gráfico de proporciones.", cex = 3)
        message("Mostrando mensaje de no datos en proportions_plot")
      } else if (is.null(sa$proportions_plot)) {
        plot.new()
        text(0.5, 0.5, "Gráfico de proporciones no disponible. Revisa los mensajes de depuración.", cex = 3)
        message("Mostrando mensaje de gráfico no disponible en proportions_plot")
      } else {
        print(sa$proportions_plot)
        message("Gráfico proportions_plot renderizado")
      }
    }, error = function(e) {
      message("Error al renderizar sentiment_proportions_plot: ", e$message)
      plot.new()
      text(0.5, 0.5, paste("Error al renderizar gráfico: ", e$message), cex = 3)
    })
  })
  
  output$sentiment_issues_plot <- renderPlot({
    sa <- sentiment_analysis_result()
    message("Renderizando sentiment_issues_plot, sa es ", ifelse(is.null(sa), "NULL", "no NULL"))
    tryCatch({
      if (is.null(sa)) {
        plot.new()
        text(0.5, 0.5, "No hay datos para mostrar el gráfico de issues.", cex = 3)
        message("Mostrando mensaje de no datos en issues_plot")
      } else if (is.null(sa$issues_plot)) {
        plot.new()
        text(0.5, 0.5, "Gráfico de issues no disponible. Revisa los mensajes de depuración.", cex = 3)
        message("Mostrando mensaje de gráfico no disponible en issues_plot")
      } else {
        print(sa$issues_plot)
        message("Gráfico issues_plot renderizado")
      }
    }, error = function(e) {
      message("Error al renderizar sentiment_issues_plot: ", e$message)
      plot.new()
      text(0.5, 0.5, paste("Error al renderizar gráfico: ", e$message), cex = 3)
    })
  })
  
  # Nueva gráfica de tendencia de percepción positiva
  output$sentiment_positive_trend <- renderPlotly({
    sa <- sentiment_analysis_result()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    legend_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(sa) || is.null(sa$sentiment_trend_data) || nrow(sa$sentiment_trend_data) == 0 || all(is.na(sa$sentiment_trend_data$positive_percent))) {
      return(plot_ly() %>% layout(
        title = list(text = ""),
        xaxis = list(title = "Mes"),
        yaxis = list(title = "Percepción Positiva (%)")
      ))
    }
    
    actual_data <- sa$sentiment_trend_data[!sa$sentiment_trend_data$Is_Projection, ]
    proj_data <- sa$sentiment_trend_data[sa$sentiment_trend_data$Is_Projection, ]
    
    max_y <- max(c(actual_data$positive_percent, proj_data$positive_percent, proj_data$CI_Upper), na.rm = TRUE)
    max_y_expanded <- max_y + (max_y * 0.5)  # Aumentar 50% para más espacio
    
    if (nrow(proj_data) == 0) {
      plot_ly(
        actual_data,
        x = ~month_year,
        y = ~positive_percent,
        type = "scatter",
        mode = "markers",
        marker = list(color = trend_color(), size = 12)
      ) %>%
        layout(
          title = list(text = ""),  # Eliminar título
          xaxis = list(
            title = "Mes",
            color = axis_color,
            showgrid = FALSE,
            tickfont = list(size = 12)
          ),
          yaxis = list(
            title = "Percepción Positiva (%)",
            color = axis_color,
            showgrid = FALSE,
            tickfont = list(size = 12),
            range = list(0, max_y_expanded)  # Escala Y más amplia
          ),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)",
          legend = list(
            x = 0.3,  # Centrar la leyenda horizontalmente
            y = -0.5,  # Colocar la leyenda en la parte inferior
            orientation = "h",  # Orientación horizontal
            font = list(size = 11, color = legend_color))
        )
    } else {
      plot_ly() %>%
        add_trace(
          data = actual_data,
          x = ~month_year,
          y = ~positive_percent,
          type = "scatter",
          mode = "lines+markers",
          name = "Actual",
          marker = list(color = trend_color(), size = 12),
          line = list(color = trend_color(), width = 2)
        ) %>%
        add_trace(
          data = proj_data,
          x = ~month_year,
          y = ~positive_percent,
          type = "scatter",
          mode = "lines",
          name = "Proyección",
          line = list(color = trend_color(), dash = "dash", width = 2)
        ) %>%
        add_ribbons(
          data = proj_data,
          x = ~month_year,
          ymin = ~CI_Lower,
          ymax = ~CI_Upper,
          name = "IC 95%",
          fillcolor = "rgba(128, 128, 128, 0.2)",
          line = list(color = "transparent")
        ) %>%
        layout(
          title = list(text = ""),  # Eliminar título
          xaxis = list(
            title = "Mes",
            color = axis_color,
            showgrid = FALSE,
            tickfont = list(size = 12)
          ),
          yaxis = list(
            title = "Percepción Positiva (%)",
            color = axis_color,
            showgrid = FALSE,
            tickfont = list(size = 12),
            range = list(0, max_y_expanded)  # Escala Y más amplia
          ),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)",
          legend = list(
            x = 0.3,  # Centrar la leyenda horizontalmente
            y = -0.5,  # Colocar la leyenda en la parte inferior
            orientation = "h",  # Orientación horizontal
            font = list(size = 11, color = legend_color)
          )
        )
    }
  })
  
  # Wordcloud
  output$sentiment_wordcloud <- renderPlot({
    sa <- sentiment_analysis_result()
    if (is.null(sa) || is.null(sa$wordcloud_corpus) || length(sa$wordcloud_corpus) == 0) {
      plot.new()
      text(0.5, 0.5, "No hay datos para generar el wordcloud.", cex = 3)
      return()
    }
    
    dtm_wordcloud <- DocumentTermMatrix(sa$wordcloud_corpus)
    word_freq <- colSums(as.matrix(dtm_wordcloud))
    word_freq <- sort(word_freq, decreasing = TRUE)
    if (length(word_freq) == 0) {
      plot.new()
      text(0.5, 0.5, "No hay palabras frecuentes para mostrar.", cex = 3)
      return()
    }
    
    wordcloud(
      names(word_freq),
      word_freq,
      min.freq = 5,
      max.words = 50,
      random.order = FALSE,
      colors = brewer.pal(8, "Dark2"),
      scale = c(6, 0.8)
    )
  })
  
  # Dataframe con detalles (solo Negativo y Neutral)
  output$sentiment_details_table <- renderDataTable({
    sa <- sentiment_analysis_result()
    
    if (is.null(sa) || is.null(sa$df_produccion) || nrow(sa$df_produccion) == 0) {
      return(data.frame(Message = "El análisis de sentimiento no se ha ejecutado o no hay datos de producción."))
    }
    
    df_produccion <- sa$df_produccion
    
    df_base <- processed_data()
    if (is.null(df_base) || nrow(df_base) == 0) {
      return(data.frame(Message = "No hay datos procesados disponibles. Verifique los filtros aplicados (por ejemplo, rango de fechas)."))
    }
    
    required_cols <- c("case_id", "csat_rated_group_name", "channel", "csat_rating_received", "issue_classification")
    missing_cols <- required_cols[!required_cols %in% colnames(df_base)]
    if (length(missing_cols) > 0) {
      return(data.frame(Message = paste("Faltan las siguientes columnas en los datos: ", 
                                        paste(missing_cols, collapse = ", "))))
    }
    
    details_df <- df_produccion %>%
      filter(predicted_sentiment %in% c("Neutral", "Negativo")) %>%
      inner_join(df_base %>% 
                   select(any_of(required_cols)), 
                 by = c("CASE.ID" = "case_id")) %>%
      rename(
        `Sentimiento Predicho` = predicted_sentiment,
        `CSAT Rating Received` = csat_rating_received,
        `Issue Predicho` = issue,
        `Grupo CSAT` = csat_rated_group_name,
        `Canal` = channel,
        `Issue Clásico` = issue_classification
      ) %>%
      select(CASE.ID, `Grupo CSAT`, `Canal`, TRANSLATED.COMMENTS, `Sentimiento Predicho`, 
             `CSAT Rating Received`, `Issue Clásico`, `Issue Predicho`)
    
    message("Tabla de detalles generada con ", nrow(details_df), " filas para Neutral/Negativo.")
    
    details_df
  })
  
  # Botón de descarga (solo Negativo y Neutral)
  output$download_details <- downloadHandler(
    filename = function() {
      paste("sentiment_details_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      sa <- sentiment_analysis_result()
      
      if (is.null(sa) || is.null(sa$df_produccion) || nrow(sa$df_produccion) == 0) {
        write.csv(data.frame(Message = "El análisis de sentimiento no se ha ejecutado o no hay datos de producción."), 
                  file, row.names = FALSE)
        return()
      }
      
      df_produccion <- sa$df_produccion
      
      df_base <- processed_data()
      if (is.null(df_base) || nrow(df_base) == 0) {
        write.csv(data.frame(Message = "No hay datos procesados disponibles. Verifique los filtros aplicados (por ejemplo, rango de fechas)."), 
                  file, row.names = FALSE)
        return()
      }
      
      required_cols <- c("case_id", "csat_rated_group_name", "channel", "csat_rating_received", "issue_classification")
      missing_cols <- required_cols[!required_cols %in% colnames(df_base)]
      if (length(missing_cols) > 0) {
        write.csv(data.frame(Message = paste("Faltan las siguientes columnas en los datos: ", 
                                             paste(missing_cols, collapse = ", "))), 
                  file, row.names = FALSE)
        return()
      }
      
      details_df <- df_produccion %>%
        filter(predicted_sentiment %in% c("Neutral", "Negativo")) %>%
        inner_join(df_base %>% 
                     select(any_of(required_cols)), 
                   by = c("CASE.ID" = "case_id")) %>%
        rename(
          `Sentimiento Predicho` = predicted_sentiment,
          `CSAT Rating Received` = csat_rating_received,
          `Issue Predicho` = issue,
          `Grupo CSAT` = csat_rated_group_name,
          `Canal` = channel,
          `Issue Clásico` = issue_classification
        ) %>%
        select(CASE.ID, `Grupo CSAT`, `Canal`, TRANSLATED.COMMENTS, `Sentimiento Predicho`, 
               `CSAT Rating Received`, `Issue Clásico`, `Issue Predicho`)
      
      write.csv(details_df, file, row.names = FALSE)
    }
  )
  
  
  
################# NETWORK ANALYSIS ##################################################
  
  network_data <- eventReactive(input$run_btn, {
    data <- processed_data()
    
    if (is.null(data) || nrow(data) == 0) return(list(nodes = data.frame(), edges = data.frame()))
    
    data$group_name_history <- gsub(", ", ",", data$group_name_history)
    group_lists <- lapply(strsplit(data$group_name_history, ","), as.vector)
    
    edges <- data.frame(from = character(), to = character(), weight = numeric(),
                        detractor_count = numeric(), sla_fail_count = numeric())
    for (i in seq_along(group_lists)) {
      groups <- group_lists[[i]]
      is_detractor <- data$classification[i] == "DETRACTOR"
      is_sla_fail <- data$resolved_in_sla[i] == 0
      if (length(groups) == 1) {
        edges <- rbind(edges, data.frame(from = groups[1], to = groups[1], weight = 1,
                                         detractor_count = as.numeric(is_detractor),
                                         sla_fail_count = as.numeric(is_sla_fail)))
      } else if (length(groups) > 1) {
        for (j in 1:(length(groups) - 1)) {
          edges <- rbind(edges, data.frame(from = groups[j], to = groups[j + 1], weight = 1,
                                           detractor_count = as.numeric(is_detractor),
                                           sla_fail_count = as.numeric(is_sla_fail)))
        }
      }
    }
    
    edges <- edges %>%
      group_by(from, to) %>%
      summarise(weight = sum(weight),
                detractor_count = sum(detractor_count),
                sla_fail_count = sum(sla_fail_count),
                .groups = "drop") %>%
      mutate(detractor_rate = detractor_count / weight,
             problem_score = detractor_rate * weight + sla_fail_count)
    
    library(igraph)
    g <- graph_from_data_frame(edges, directed = TRUE)
    centrality <- data.frame(
      id = V(g)$name,
      degree = degree(g, mode = "all"),
      betweenness = betweenness(g, directed = TRUE, normalized = TRUE)
    )
    
    node_occurrences <- table(unlist(group_lists))
    if (length(node_occurrences) == 0) return(list(nodes = data.frame(), edges = edges))
    
    occurrences <- as.numeric(node_occurrences)
    normalized_sizes <- (occurrences - min(occurrences)) / (max(occurrences) - min(occurrences))
    node_sizes <- 20 + (100 - 20) * normalized_sizes
    
    nodes <- data.frame(
      id = names(node_occurrences),
      label = names(node_occurrences),
      value = node_sizes,
      color = viridis(length(node_occurrences), alpha = 0.8)[rank(normalized_sizes)]
    ) %>% left_join(centrality, by = "id")
    
    list(nodes = nodes, edges = edges %>% rename(value = weight))
  })
  
  # Red de Interacciones
  output$network <- renderVisNetwork({
    data <- network_data()
    if (nrow(data$nodes) == 0) {
      return(visNetwork(data.frame(id = 1, label = "Sin datos"), data.frame(), width = "100%", height = "600px") %>%
               visNodes(shape = "dot", font = list(size = 15, color = ifelse(input$toggle_mode, "white", "black"))))
    }
    visNetwork(data$nodes, data$edges %>% select(from, to, value), width = "100%", height = "600px") %>%
      visEdges(arrows = "to", color = list(color = "gray", opacity = 0.5)) %>%
      visNodes(shape = "dot", font = list(size = 15, color = ifelse(input$toggle_mode, "white", "black"))) %>%
      visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
      visPhysics(solver = "forceAtlas2Based", forceAtlas2Based = list(gravitationalConstant = -50, centralGravity = 0.01))
  })
  
  # Top 10 Interacciones (Frecuencia)
  top_interactions_data <- eventReactive(input$run_btn, {
    edges <- network_data()$edges
    if (nrow(edges) == 0) return(data.frame(group_pairs = character(), weights = numeric()))
    top_10 <- edges %>% arrange(desc(value)) %>% head(10)
    group_pairs <- paste(top_10$from, "-", top_10$to)
    data.frame(group_pairs, weights = top_10$value) %>% arrange(weights)
  })
  
  output$top_interactions <- renderPlotly({
    df_top <- top_interactions_data()
    if (nrow(df_top) == 0) {
      return(plot_ly() %>% layout(title = "No hay datos disponibles"))
    }
    df_top$group_pairs <- factor(df_top$group_pairs, levels = df_top$group_pairs)
    
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    plot_ly(
      y = df_top$group_pairs,
      x = df_top$weights,
      type = "bar",
      orientation = "h",
      marker = list(color = viridis(length(df_top$group_pairs))),
      text = df_top$weights,
      textposition = "outside",  # Etiquetas fuera de las barras
      textfont = list(size = 12, color = text_color)
    ) %>%
      layout(
        title = list(text = "Frecuencia", font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "", showticklabels = FALSE, showgrid = FALSE, color = axis_color),
        yaxis = list(title = "", categoryorder = "array", categoryarray = df_top$group_pairs, showgrid = FALSE, color = axis_color),
        margin = list(l = 150, r = 50, t = 50, b = 50),
        bargap = 0.2,
        showlegend = FALSE,
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  # Top 10 Interacciones Problemáticas
  top_problematic_interactions_data <- eventReactive(input$run_btn, {
    edges <- network_data()$edges
    if (nrow(edges) == 0) return(data.frame(group_pairs = character(), detractor_rates = numeric()))
    
    top_10_problematic <- edges %>%
      filter(value > 1) %>%
      arrange(desc(detractor_rate)) %>%
      head(10)
    
    group_pairs <- paste(top_10_problematic$from, "-", top_10_problematic$to)
    data.frame(group_pairs, detractor_rates = top_10_problematic$detractor_rate * 100) %>% arrange(detractor_rates)
  })
  
  output$top_problematic_interactions <- renderPlotly({
    df_problematic <- top_problematic_interactions_data()
    if (nrow(df_problematic) == 0) {
      return(plot_ly() %>% layout(title = "No hay datos suficientes"))
    }
    df_problematic$group_pairs <- factor(df_problematic$group_pairs, levels = df_problematic$group_pairs)
    
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    plot_ly(
      y = df_problematic$group_pairs,
      x = df_problematic$detractor_rates,
      type = "bar",
      orientation = "h",
      marker = list(color = viridis(length(df_problematic$group_pairs), direction = -1)),
      text = round(df_problematic$detractor_rates, 1),
      textposition = "outside",  # Etiquetas fuera de las barras
      textfont = list(size = 12, color = text_color)
    ) %>%
      layout(
        title = list(text = "Tasa de DETRACTOR (%)", font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "", showticklabels = FALSE, showgrid = FALSE, color = axis_color),
        yaxis = list(title = "", categoryorder = "array", categoryarray = df_problematic$group_pairs, showgrid = FALSE, color = axis_color),
        margin = list(l = 150, r = 50, t = 50, b = 50),
        bargap = 0.2,
        showlegend = FALSE,
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  # Red Interactiva con Impacto
  output$network_deep_dive <- renderVisNetwork({
    data <- network_data()
    if (nrow(data$nodes) == 0) {
      return(visNetwork(data.frame(id = 1, label = "Sin datos"), data.frame(), width = "100%", height = "600px") %>%
               visNodes(shape = "dot", font = list(size = 15, color = ifelse(input$toggle_mode, "white", "black"))))
    }
    
    edges <- data$edges %>%
      mutate(width = pmin(problem_score / max(problem_score, na.rm = TRUE) * 10, 10),
             title = paste("Frecuencia:", value, "<br>DETRACTORs:", detractor_count, 
                           "<br>SLA Fails:", sla_fail_count, "<br>Tasa DETRACTOR:", round(detractor_rate * 100, 1), "%"))
    
    nodes <- data$nodes %>%
      mutate(title = paste("Grupo:", id, "<br>Incidencias:", value, "<br>Betweenness:", round(betweenness, 3)))
    
    visNetwork(nodes, edges, width = "100%", height = "600px") %>%
      visEdges(arrows = "to", color = list(color = "gray", opacity = 0.5)) %>%
      visNodes(shape = "dot", font = list(size = 15, color = ifelse(input$toggle_mode, "white", "black"))) %>%
      visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
      visPhysics(solver = "forceAtlas2Based", forceAtlas2Based = list(gravitationalConstant = -50, centralGravity = 0.01)) %>%
      visInteraction(tooltipDelay = 0)
  })
  
  # Top 5 Nodos Problemáticos (Centralidad)
  centrality_data <- eventReactive(input$run_btn, {
    data <- network_data()
    if (nrow(data$nodes) == 0) return(data.frame())
    
    # Expandir group_name_history sin unnest
    group_data <- processed_data()
    group_list_expanded <- do.call(rbind, lapply(seq_along(group_data$group_name_history), function(i) {
      groups <- strsplit(gsub(", ", ",", group_data$group_name_history[i]), ",")[[1]]
      data.frame(
        row_id = i,
        group_list = groups,
        classification = group_data$classification[i],
        stringsAsFactors = FALSE
      )
    }))
    
    node_problems <- group_list_expanded %>%
      group_by(group_list) %>%
      summarise(
        detractor_rate = mean(classification == "DETRACTOR", na.rm = TRUE),
        total_cases = n(),
        .groups = "drop"
      ) %>%
      rename(id = group_list)
    
    data$nodes %>%
      left_join(node_problems, by = "id") %>%
      mutate(
        problem_score = detractor_rate * total_cases,
        detractor_rate = round(detractor_rate * 100, 1),  # Multiplicar por 100 y 1 decimal
        betweenness = round(betweenness, 4)  # 4 decimales para betweenness
      ) %>%
      arrange(desc(betweenness), desc(problem_score)) %>%
      select(id, degree, betweenness, detractor_rate, total_cases, problem_score) %>%
      head(5)
  })
  
  output$centrality_table <- DT::renderDT({
    DT::datatable(centrality_data(),
                  options = list(pageLength = 5, autoWidth = TRUE),
                  rownames = FALSE,
                  colnames = c("Grupo", "Grado", "Betweenness", "Tasa DETRACTOR (%)", "Casos Totales", "Puntaje Problema"))
  })
  
  # Top 5 Caminos Problemáticos
  critical_paths_data <- eventReactive(input$run_btn, {
    data <- processed_data()
    if (is.null(data) || nrow(data) == 0) return(data.frame())
    
    paths <- data %>%
      mutate(path = group_name_history,
             detractor = classification == "DETRACTOR") %>%
      group_by(path) %>%
      summarise(
        total_cases = n(),
        detractor_count = sum(detractor, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(detractor_rate = round((detractor_count / total_cases) * 100, 1)) %>%  # Multiplicar por 100 y 1 decimal
      filter(total_cases > 1) %>%
      arrange(desc(detractor_rate), desc(total_cases)) %>%
      head(5)
    
    paths
  })
  
  output$critical_paths_table <- DT::renderDT({
    DT::datatable(critical_paths_data(),
                  options = list(pageLength = 5, autoWidth = TRUE),
                  rownames = FALSE,
                  colnames = c("Camino", "Casos Totales", "DETRACTORs", "Tasa DETRACTOR (%)"))
  })
  
  output$critical_paths_table <- DT::renderDT({
    DT::datatable(critical_paths_data(),
                  options = list(pageLength = 5, autoWidth = TRUE),
                  rownames = FALSE,
                  colnames = c("Camino", "Casos Totales", "DETRACTORs", "Tasa DETRACTOR (%)"))
  })
  
  # Clústeres de Interacciones - Corrección para grafo no dirigido
  cluster_data <- eventReactive(input$run_btn, {
    data <- network_data()
    if (nrow(data$edges) == 0) return(list(nodes = data.frame(), edges = data.frame()))
    
    library(igraph)
    g <- graph_from_data_frame(data$edges, directed = TRUE, vertices = data$nodes)
    g_undirected <- as.undirected(g, mode = "collapse", edge.attr.comb = list(value = "sum"))  # Convertir a no dirigido
    clusters <- cluster_louvain(g_undirected, weights = E(g_undirected)$value)
    
    nodes <- data$nodes %>%
      mutate(group = membership(clusters),
             color = viridis(max(membership(clusters)))[group])
    
    list(nodes = nodes, edges = data$edges)
  })
  
  output$cluster_network <- renderVisNetwork({
    data <- cluster_data()
    if (nrow(data$nodes) == 0) {
      return(visNetwork(data.frame(id = 1, label = "Sin datos"), data.frame(), width = "100%", height = "400px"))
    }
    
    visNetwork(data$nodes, data$edges, width = "100%", height = "400px") %>%
      visEdges(arrows = "to", color = list(color = "gray", opacity = 0.5)) %>%
      visNodes(shape = "dot", font = list(size = 15, color = ifelse(input$toggle_mode, "white", "black"))) %>%
      visOptions(highlightNearest = TRUE) %>%
      visPhysics(solver = "forceAtlas2Based")
  })
  
  
  
  
  
#################### ESTADISTICAS #############################################  
  
  
  # Process Capability Function
  calculate_process_capability <- function(data, lsl, usl = 100) {
    mean_val <- mean(data, na.rm = TRUE)
    sd_val <- sd(data, na.rm = TRUE)
    
    if (sd_val == 0 || is.na(sd_val)) {
      return(list(cp = NA, cpk = NA, sigma_level = NA))
    }
    
    cp <- (usl - lsl) / (6 * sd_val)
    cpk <- min((usl - mean_val) / (3 * sd_val), (mean_val - lsl) / (3 * sd_val))
    sigma_level <- min(6, max(0, 3 * cpk))
    
    list(cp = round(cp, 2), cpk = round(cpk, 2), sigma_level = round(sigma_level, 1))
  }
  
  # Stats Data
  stats_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(list(csat_stats = list(avg = NA), sla_stats = list(avg = NA), resolution_time = list(), resolution_groups = list()))
    surveys_df <- nrow(df)
    
    csat_by_month <- df %>%
      group_by(month_year) %>%
      summarise(CSAT_Score = ifelse(n() > 0 && sum(!is.na(classification)) > 0, 
                                    round(sum(classification == "PROMOTER", na.rm = TRUE) / n() * 100, 1), NA),
                .groups = "drop") %>%
      pull(CSAT_Score)
    csat_quartiles <- quantile(csat_by_month, probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
    csat_iqr <- csat_quartiles[3] - csat_quartiles[1]
    csat_lower_bound <- csat_quartiles[1] - 1.5 * csat_iqr
    csat_upper_bound <- csat_quartiles[3] + 1.5 * csat_iqr
    csat_outliers_lower <- sum(csat_by_month < csat_lower_bound, na.rm = TRUE)
    csat_outliers_upper <- sum(csat_by_month > csat_upper_bound, na.rm = TRUE)
    csat_mean <- mean(csat_by_month, na.rm = TRUE)
    csat_median <- median(csat_by_month, na.rm = TRUE)
    csat_skew <- ifelse(abs(csat_mean - csat_median) < 0.1, "Centrada", 
                        ifelse(csat_mean > csat_median, "Sesgada a la derecha", "Sesgada a la izquierda"))
    csat_stats <- list(
      min = min(csat_by_month, na.rm = TRUE),
      max = max(csat_by_month, na.rm = TRUE),
      avg = round(csat_mean, 1),
      std = round(sd(csat_by_month, na.rm = TRUE), 1),
      q1 = round(csat_quartiles[1], 1),
      q2 = round(csat_quartiles[2], 1),
      q3 = round(csat_quartiles[3], 1),
      outliers_lower = csat_outliers_lower,
      outliers_upper = csat_outliers_upper,
      skew = csat_skew,
      surveys = surveys_df,
      promoters = sum(df$classification == "PROMOTER", na.rm = TRUE),
      detractors = sum(df$classification == "DETRACTOR", na.rm = TRUE)
    )
    csat_capability <- calculate_process_capability(csat_by_month, 75)
    csat_stats$cp <- csat_capability$cp
    csat_stats$cpk <- csat_capability$cpk
    csat_stats$sigma_level <- csat_capability$sigma_level
    
    sla_by_month <- df %>%
      group_by(month_year) %>%
      summarise(SLA_Percent = ifelse(n() > 0 && sum(!is.na(resolved_in_sla)) > 0, 
                                     round(sum(resolved_in_sla == 1, na.rm = TRUE) / n() * 100, 1), NA),
                .groups = "drop") %>%
      pull(SLA_Percent)
    sla_quartiles <- quantile(sla_by_month, probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
    sla_iqr <- sla_quartiles[3] - sla_quartiles[1]
    sla_lower_bound <- sla_quartiles[1] - 1.5 * sla_iqr
    sla_upper_bound <- sla_quartiles[3] + 1.5 * sla_iqr
    sla_outliers_lower <- sum(sla_by_month < sla_lower_bound, na.rm = TRUE)
    sla_outliers_upper <- sum(sla_by_month > sla_upper_bound, na.rm = TRUE)
    sla_mean <- mean(sla_by_month, na.rm = TRUE)
    sla_median <- median(sla_by_month, na.rm = TRUE)
    sla_skew <- ifelse(abs(sla_mean - sla_median) < 0.1, "Centrada", 
                       ifelse(sla_mean > sla_median, "Sesgada a la derecha", "Sesgada a la izquierda"))
    sla_stats <- list(
      min = min(sla_by_month, na.rm = TRUE),
      max = max(sla_by_month, na.rm = TRUE),
      avg = round(sla_mean, 1),
      std = round(sd(sla_by_month, na.rm = TRUE), 1),
      q1 = round(sla_quartiles[1], 1),
      q2 = round(sla_quartiles[2], 1),
      q3 = round(sla_quartiles[3], 1),
      outliers_lower = sla_outliers_lower,
      outliers_upper = sla_outliers_upper,
      skew = sla_skew
    )
    sla_capability <- calculate_process_capability(sla_by_month, 90)
    sla_stats$cp <- sla_capability$cp
    sla_stats$cpk <- sla_capability$cpk
    sla_stats$sigma_level <- sla_capability$sigma_level
    
    rt <- df$resolution_time_day
    rt_quartiles <- quantile(rt, probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
    rt_iqr <- rt_quartiles[3] - rt_quartiles[1]
    rt_lower_bound <- rt_quartiles[1] - 1.5 * rt_iqr
    rt_upper_bound <- rt_quartiles[3] + 1.5 * rt_iqr
    rt_outliers_lower <- sum(rt < rt_lower_bound, na.rm = TRUE)
    rt_outliers_upper <- sum(rt > rt_upper_bound, na.rm = TRUE)
    rt_mean <- mean(rt, na.rm = TRUE)
    rt_median <- median(rt, na.rm = TRUE)
    rt_skew <- ifelse(abs(rt_mean - rt_median) < 0.1, "Centrada", 
                      ifelse(rt_mean > rt_median, "Sesgada a la derecha", "Sesgada a la izquierda"))
    resolution_time <- list(
      min = min(rt, na.rm = TRUE),
      max = max(rt, na.rm = TRUE),
      avg = round(rt_mean, 3),
      std = round(sd(rt, na.rm = TRUE), 3),
      q1 = round(rt_quartiles[1], 3),
      q2 = round(rt_quartiles[2], 3),
      q3 = round(rt_quartiles[3], 3),
      outliers_lower = rt_outliers_lower,
      outliers_upper = rt_outliers_upper,
      skew = rt_skew
    )
    
    rg <- df$total_groups
    rg_quartiles <- quantile(rg, probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
    rg_iqr <- rg_quartiles[3] - rg_quartiles[1]
    rg_lower_bound <- rg_quartiles[1] - 1.5 * rg_iqr
    rg_upper_bound <- rg_quartiles[3] + 1.5 * rg_iqr
    rg_outliers_lower <- sum(rg < rg_lower_bound, na.rm = TRUE)
    rg_outliers_upper <- sum(rg > rg_upper_bound, na.rm = TRUE)
    rg_mean <- mean(rg, na.rm = TRUE)
    rg_median <- median(rg, na.rm = TRUE)
    rg_skew <- ifelse(abs(rg_mean - rg_median) < 0.1, "Centrada", 
                      ifelse(rg_mean > rg_median, "Sesgada a la derecha", "Sesgada a la izquierda"))
    resolution_groups <- list(
      min = min(rg, na.rm = TRUE),
      max = max(rg, na.rm = TRUE),
      avg = floor(rg_mean),
      std = floor(sd(rg, na.rm = TRUE)),
      q1 = floor(rg_quartiles[1]),
      q2 = floor(rg_quartiles[2]),
      q3 = floor(rg_quartiles[3]),
      outliers_lower = rg_outliers_lower,
      outliers_upper = rg_outliers_upper,
      skew = rg_skew
    )
    
    list(csat_stats = csat_stats, sla_stats = sla_stats, resolution_time = resolution_time, resolution_groups = resolution_groups)
  })
  
  # Stats Outputs
  output$csat_score <- renderPrint({
    csat <- stats_data()$csat_stats
    if (is.na(csat$avg)) {
      cat("No data")
    } else {
      cat(sprintf("Min: %.1f\nQ1: %.1f\nMedian: %.1f\nQ3: %.1f\nMax: %.1f\nAvg: %.1f\nStd: %.1f\nOutliers Inferiores: %d\nOutliers Superiores: %d\nDistribución: %s\nSurveys: %d\nPromoters: %d\nDetractors: %d\nCp: %.2f\nCpk: %.2f\nSigma Level: %.1f",
                  csat$min, csat$q1, csat$q2, csat$q3, csat$max, csat$avg, csat$std, csat$outliers_lower, csat$outliers_upper, csat$skew,
                  csat$surveys, csat$promoters, csat$detractors, csat$cp, csat$cpk, csat$sigma_level))
    }
  })
  
  output$sla_stats <- renderPrint({
    sla <- stats_data()$sla_stats
    if (is.na(sla$avg)) {
      cat("No data")
    } else {
      cat(sprintf("Min: %.1f\nQ1: %.1f\nMedian: %.1f\nQ3: %.1f\nMax: %.1f\nAvg: %.1f\nStd: %.1f\nOutliers Inferiores: %d\nOutliers Superiores: %d\nDistribución: %s\nCp: %.2f\nCpk: %.2f\nSigma Level: %.1f",
                  sla$min, sla$q1, sla$q2, sla$q3, sla$max, sla$avg, sla$std, sla$outliers_lower, sla$outliers_upper, sla$skew,
                  sla$cp, sla$cpk, sla$sigma_level))
    }
  })
  
  output$resolution_time <- renderPrint({
    rt <- stats_data()$resolution_time
    if (length(rt) == 0) {
      cat("No data")
    } else {
      cat(sprintf("Min: %.3f\nQ1: %.3f\nMedian: %.3f\nQ3: %.3f\nMax: %.3f\nAvg: %.3f\nStd: %.3f\nOutliers Inferiores: %d\nOutliers Superiores: %d\nDistribución: %s",
                  rt$min, rt$q1, rt$q2, rt$q3, rt$max, rt$avg, rt$std, rt$outliers_lower, rt$outliers_upper, rt$skew))
    }
  })
  
  output$resolution_groups <- renderPrint({
    rg <- stats_data()$resolution_groups
    if (length(rg) == 0) {
      cat("No data")
    } else {
      cat(sprintf("Min: %d\nQ1: %d\nMedian: %d\nQ3: %d\nMax: %d\nAvg: %d\nStd: %d\nOutliers Inferiores: %d\nOutliers Superiores: %d\nDistribución: %s",
                  rg$min, rg$q1, rg$q2, rg$q3, rg$max, rg$avg, rg$std, rg$outliers_lower, rg$outliers_upper, rg$skew))
    }
  })
  
  # Histograms
  output$csat_histogram <- renderPlotly({
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(plot_ly())
    
    csat_by_month <- df %>%
      group_by(month_year) %>%
      summarise(CSAT_Score = ifelse(n() > 0 && sum(!is.na(classification)) > 0, 
                                    round(sum(classification == "PROMOTER", na.rm = TRUE) / n() * 100, 1), NA),
                .groups = "drop") %>%
      pull(CSAT_Score)
    
    mean_val <- mean(csat_by_month, na.rm = TRUE)
    median_val <- median(csat_by_month, na.rm = TRUE)
    hist_data <- hist(csat_by_month, plot = FALSE)
    max_height <- max(hist_data$counts)
    
    plot_ly(x = csat_by_month, type = "histogram", 
            color = I(trend_color())) %>%
      add_segments(x = mean_val, xend = mean_val, y = 0, yend = max_height, 
                   name = "Media", line = list(color = "red")) %>%
      add_segments(x = median_val, xend = median_val, y = 0, yend = max_height, 
                   name = "Mediana", line = list(color = "blue", dash = "dash")) %>%
      add_segments(x = 75, xend = 75, y = 0, yend = max_height, 
                   name = "Target (75)", line = list(color = "green")) %>%
      layout(
        xaxis = list(showgrid = FALSE),
        yaxis = list(showgrid = FALSE),
        showlegend = TRUE
      )
  })
  
  output$sla_histogram <- renderPlotly({
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(plot_ly())
    
    sla_by_month <- df %>%
      group_by(month_year) %>%
      summarise(SLA_Percent = ifelse(n() > 0 && sum(!is.na(resolved_in_sla)) > 0, 
                                     round(sum(resolved_in_sla == 1, na.rm = TRUE) / n() * 100, 1), NA),
                .groups = "drop") %>%
      pull(SLA_Percent)
    
    mean_val <- mean(sla_by_month, na.rm = TRUE)
    median_val <- median(sla_by_month, na.rm = TRUE)
    hist_data <- hist(sla_by_month, plot = FALSE)
    max_height <- max(hist_data$counts)
    
    plot_ly(x = sla_by_month, type = "histogram", 
            color = I(trend_color())) %>%
      add_segments(x = mean_val, xend = mean_val, y = 0, yend = max_height, 
                   name = "Media", line = list(color = "red")) %>%
      add_segments(x = median_val, xend = median_val, y = 0, yend = max_height, 
                   name = "Mediana", line = list(color = "blue", dash = "dash")) %>%
      add_segments(x = 90, xend = 90, y = 0, yend = max_height, 
                   name = "Target (90)", line = list(color = "green")) %>%
      layout(
        xaxis = list(showgrid = FALSE),
        yaxis = list(showgrid = FALSE),
        showlegend = TRUE
      )
  })
  
  output$rt_histogram <- renderPlotly({
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(plot_ly())
    
    rt <- df$resolution_time_day
    mean_val <- mean(rt, na.rm = TRUE)
    median_val <- median(rt, na.rm = TRUE)
    hist_data <- hist(rt, plot = FALSE)
    max_height <- max(hist_data$counts)
    
    plot_ly(x = rt, type = "histogram", 
            color = I(trend_color())) %>%
      add_segments(x = mean_val, xend = mean_val, y = 0, yend = max_height, 
                   name = "Media", line = list(color = "red")) %>%
      add_segments(x = median_val, xend = median_val, y = 0, yend = max_height, 
                   name = "Mediana", line = list(color = "blue", dash = "dash")) %>%
      layout(
        xaxis = list(showgrid = FALSE),
        yaxis = list(showgrid = FALSE),
        showlegend = TRUE
      )
  })
  
  output$rg_histogram <- renderPlotly({
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(plot_ly())
    
    rg <- df$total_groups
    mean_val <- mean(rg, na.rm = TRUE)
    median_val <- median(rg, na.rm = TRUE)
    hist_data <- hist(rg, plot = FALSE)
    max_height <- max(hist_data$counts)
    
    plot_ly(x = rg, type = "histogram", 
            color = I(trend_color())) %>%
      add_segments(x = mean_val, xend = mean_val, y = 0, yend = max_height, 
                   name = "Media", line = list(color = "red")) %>%
      add_segments(x = median_val, xend = median_val, y = 0, yend = max_height, 
                   name = "Mediana", line = list(color = "blue", dash = "dash")) %>%
      layout(
        xaxis = list(showgrid = FALSE),
        yaxis = list(showgrid = FALSE),
        showlegend = TRUE
      )
  })
  

  ################## TRENDS ########################################################## 
  
  trends_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(data.frame())
    
    trends <- df %>%
      group_by(month_year) %>%
      summarise(
        CSAT_Score = ifelse(n() > 0 && sum(!is.na(classification)) > 0, 
                            round(sum(classification == "PROMOTER", na.rm = TRUE) / n() * 100, 1), NA),
        Median_Resolution_Time = ifelse(sum(!is.na(resolution_time_day)) > 0, 
                                        median(resolution_time_day, na.rm = TRUE), NA),
        Mean_Resolution_Groups = ifelse(sum(!is.na(total_groups)) > 0, 
                                        mean(total_groups, na.rm = TRUE), NA),
        SLA_Percent = ifelse(n() > 0 && sum(!is.na(resolved_in_sla)) > 0, 
                             round(sum(resolved_in_sla == 1, na.rm = TRUE) / n() * 100, 1), NA),
        .groups = "drop"
      ) %>%
      arrange(as.Date(paste0(month_year, "-01")))
    
    last_date <- as.Date(paste0(max(trends$month_year), "-01"))
    future_months <- format(seq(last_date %m+% months(1), by = "month", length.out = 5), "%Y-%m")
    
    if (nrow(trends) >= 24) {
      ts_csat <- ts(trends$CSAT_Score, frequency = 12)
      hw_csat <- tryCatch({
        HoltWinters(ts_csat, seasonal = "additive")
      }, error = function(e) {
        HoltWinters(ts_csat, gamma = FALSE)
      })
      pred_csat <- forecast(hw_csat, h = 5, level = 95)
      
      ts_sla <- ts(trends$SLA_Percent, frequency = 12)
      hw_sla <- tryCatch({
        HoltWinters(ts_sla, seasonal = "additive")
      }, error = function(e) {
        HoltWinters(ts_sla, gamma = FALSE)
      })
      pred_sla <- forecast(hw_sla, h = 5, level = 95)
      
      future_df_csat <- data.frame(
        month_year = future_months,
        CSAT_Score = pmin(100, as.numeric(pred_csat$mean)),  # Limitar al 100%
        CI_Lower_CSAT = pmin(100, as.numeric(pred_csat$lower)),  # Limitar al 100%
        CI_Upper_CSAT = pmin(100, as.numeric(pred_csat$upper)),  # Limitar al 100%
        Is_Projection_CSAT = TRUE
      )
      
      future_df_sla <- data.frame(
        month_year = future_months,
        SLA_Percent = pmin(100, as.numeric(pred_sla$mean)),  # Limitar al 100%
        CI_Lower_SLA = pmin(100, as.numeric(pred_sla$lower)),  # Limitar al 100%
        CI_Upper_SLA = pmin(100, as.numeric(pred_sla$upper)),  # Limitar al 100%
        Is_Projection_SLA = TRUE
      )
      
      trends$Is_Projection_CSAT <- FALSE
      trends$Is_Projection_SLA <- FALSE
      
      future_df <- full_join(future_df_csat, future_df_sla, by = "month_year")
      return(bind_rows(trends, future_df))
    } else if (nrow(trends) >= 2) {
      ts_csat <- ts(trends$CSAT_Score)
      hw_csat <- HoltWinters(ts_csat, gamma = FALSE)
      pred_csat <- forecast(hw_csat, h = 5, level = 95)
      
      ts_sla <- ts(trends$SLA_Percent)
      hw_sla <- HoltWinters(ts_sla, gamma = FALSE)
      pred_sla <- forecast(hw_sla, h = 5, level = 95)
      
      future_df_csat <- data.frame(
        month_year = future_months,
        CSAT_Score = pmin(100, as.numeric(pred_csat$mean)),  # Limitar al 100%
        CI_Lower_CSAT = pmin(100, as.numeric(pred_csat$lower)),  # Limitar al 100%
        CI_Upper_CSAT = pmin(100, as.numeric(pred_csat$upper)),  # Limitar al 100%
        Is_Projection_CSAT = TRUE
      )
      
      future_df_sla <- data.frame(
        month_year = future_months,
        SLA_Percent = pmin(100, as.numeric(pred_sla$mean)),  # Limitar al 100%
        CI_Lower_SLA = pmin(100, as.numeric(pred_sla$lower)),  # Limitar al 100%
        CI_Upper_SLA = pmin(100, as.numeric(pred_sla$upper)),  # Limitar al 100%
        Is_Projection_SLA = TRUE
      )
      
      trends$Is_Projection_CSAT <- FALSE
      trends$Is_Projection_SLA <- FALSE
      
      future_df <- full_join(future_df_csat, future_df_sla, by = "month_year")
      return(bind_rows(trends, future_df))
    } else {
      trends$Is_Projection_CSAT <- FALSE
      trends$Is_Projection_SLA <- FALSE
      return(trends)
    }
  })
  
  trend_color <- reactive({
    if (input$toggle_mode) "#11EFE3" else "#635BFF"
  })
  
  output$csat_trend <- renderPlotly({
    df <- trends_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    legend_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (nrow(df) == 0 || all(is.na(df$CSAT_Score))) {
      return(plot_ly() %>% layout(title = "No hay datos disponibles para CSAT"))
    }
    
    actual_data <- df[!df$Is_Projection_CSAT %in% TRUE, ]
    proj_data <- df[df$Is_Projection_CSAT %in% TRUE, ]
    
    title_text <- if (nrow(proj_data) > 0 && nrow(actual_data) >= 24) {
      "Tendencia CSAT Score + Proyección (Holt-Winters)"
    } else if (nrow(proj_data) > 0) {
      "Tendencia CSAT Score + Proyección (Holt)"
    } else {
      "CSAT Score (Sin Proyección)"
    }
    
    if (nrow(proj_data) == 0) {
      plot_ly(actual_data, x = ~month_year, y = ~CSAT_Score, type = "scatter", mode = "markers",
              marker = list(color = trend_color(), size = 10)) %>%
        layout(
          title = list(text = title_text, font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "CSAT Score (%)", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)"
        )
    } else {
      plot_ly() %>%
        add_trace(data = actual_data, x = ~month_year, y = ~CSAT_Score, type = "scatter", mode = "lines+markers",
                  name = "Actual", marker = list(color = trend_color(), size = 8), line = list(color = trend_color())) %>%
        add_trace(data = proj_data, x = ~month_year, y = ~CSAT_Score, type = "scatter", mode = "lines",
                  name = "Proyección", line = list(color = trend_color(), dash = "dash")) %>%
        add_ribbons(data = proj_data, x = ~month_year, ymin = ~CI_Lower_CSAT, ymax = ~CI_Upper_CSAT,
                    name = "IC 95%", fillcolor = "rgba(128, 128, 128, 0.2)", line = list(color = "transparent")) %>%
        layout(
          title = list(text = title_text, font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "CSAT Score (%)", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)",
          legend = list(
            x = 0.3,  # Centrar la leyenda horizontalmente
            y = -0.2,  # Colocar la leyenda en la parte inferior
            orientation = "h",  # Orientación horizontal
            font = list(color = legend_color)
          )
        )
    }
  })
  
  output$resolution_time_trend <- renderPlotly({
    df <- trends_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (nrow(df) == 0 || all(is.na(df$Median_Resolution_Time))) {
      return(plot_ly() %>% layout(title = "No hay datos disponibles para Tiempo de Resolución"))
    }
    
    if (nrow(df[!is.na(df$Median_Resolution_Time), ]) < 2) {
      plot_ly(df, x = ~month_year, y = ~Median_Resolution_Time, type = "scatter", mode = "markers",
              marker = list(color = trend_color(), size = 10)) %>%
        layout(
          title = list(text = "Tiempo de Resolución (Sin Tendencia)", font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "Días", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)"
        )
    } else {
      plot_ly(df, x = ~month_year, y = ~Median_Resolution_Time, type = "scatter", mode = "lines",
              line = list(color = trend_color())) %>%
        layout(
          title = list(text = "Tendencia Mediana Tiempo de Resolución", font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "Días", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)",
          legend = list(
            x = 0.3,  # Centrar la leyenda horizontalmente
            y = -0.2,  # Colocar la leyenda en la parte inferior
            orientation = "h",  # Orientación horizontal
            font = list(color = text_color)
          )
        )
    }
  })
  
  output$resolution_groups_trend <- renderPlotly({
    df <- trends_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (nrow(df) == 0 || all(is.na(df$Mean_Resolution_Groups))) {
      return(plot_ly() %>% layout(title = "No hay datos disponibles para Grupos de Resolución"))
    }
    
    if (nrow(df[!is.na(df$Mean_Resolution_Groups), ]) < 2) {
      plot_ly(df, x = ~month_year, y = ~Mean_Resolution_Groups, type = "scatter", mode = "markers",
              marker = list(color = trend_color(), size = 10)) %>%
        layout(
          title = list(text = "Grupos de Resolución (Sin Tendencia)", font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "Número Promedio de Grupos", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)"
        )
    } else {
      plot_ly(df, x = ~month_year, y = ~Mean_Resolution_Groups, type = "scatter", mode = "lines",
              line = list(color = trend_color())) %>%
        layout(
          title = list(text = "Tendencia Promedio Grupos de Resolución", font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "Número Promedio de Grupos", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)",
          legend = list(
            x = 0.3,  # Centrar la leyenda horizontalmente
            y = -0.2,  # Colocar la leyenda en la parte inferior
            orientation = "h",  # Orientación horizontal
            font = list(color = text_color)
          )
        )
    }
  })
  
  output$sla_trend <- renderPlotly({
    df <- trends_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    legend_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (nrow(df) == 0 || all(is.na(df$SLA_Percent))) {
      return(plot_ly() %>% layout(title = "No hay datos disponibles para SLA"))
    }
    
    actual_data <- df[!df$Is_Projection_SLA %in% TRUE, ]
    proj_data <- df[df$Is_Projection_SLA %in% TRUE, ]
    
    title_text <- if (nrow(proj_data) > 0 && nrow(actual_data) >= 24) {
      "Tendencia % SLA + Proyección (Holt-Winters)"
    } else if (nrow(proj_data) > 0) {
      "Tendencia % SLA + Proyección (Holt)"
    } else {
      "% SLA (Sin Proyección)"
    }
    
    if (nrow(proj_data) == 0) {
      plot_ly(actual_data, x = ~month_year, y = ~SLA_Percent, type = "scatter", mode = "markers",
              marker = list(color = trend_color(), size = 10)) %>%
        layout(
          title = list(text = title_text, font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "% SLA", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)"
        )
    } else {
      plot_ly() %>%
        add_trace(data = actual_data, x = ~month_year, y = ~SLA_Percent, type = "scatter", mode = "lines+markers",
                  name = "Actual", marker = list(color = trend_color(), size = 8), line = list(color = trend_color())) %>%
        add_trace(data = proj_data, x = ~month_year, y = ~SLA_Percent, type = "scatter", mode = "lines",
                  name = "Proyección", line = list(color = trend_color(), dash = "dash")) %>%
        add_ribbons(data = proj_data, x = ~month_year, ymin = ~CI_Lower_SLA, ymax = ~CI_Upper_SLA,
                    name = "IC 95%", fillcolor = "rgba(128, 128, 128, 0.2)", line = list(color = "transparent")) %>%
        layout(
          title = list(text = title_text, font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "% SLA", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)",
          legend = list(
            x = 0.3,  # Centrar la leyenda horizontalmente
            y = -0.2,  # Colocar la leyenda en la parte inferior
            orientation = "h",  # Orientación horizontal
            font = list(color = legend_color)
          )
        )
    }
  })
  
  
  
  ############### CORRELACION #########################################
  
  
  corr_data <- eventReactive(input$run_btn, {
    df <- trends_data()
    if (nrow(df) == 0) return(list(matrix = NULL, warning = "No data available from trends_data"))
    
    df <- df %>%
      filter(!Is_Projection_CSAT %in% TRUE, !Is_Projection_SLA %in% TRUE) %>%
      select(CSAT_Score, SLA_Percent, Median_Resolution_Time, Mean_Resolution_Groups) %>%
      na.omit()
    
    if (nrow(df) < 2) {
      return(list(matrix = NULL, warning = "Fewer than 2 complete rows after removing NAs"))
    }
    
    variances <- apply(df, 2, var, na.rm = TRUE)
    valid_cols <- names(variances)[!is.na(variances) & variances > 0]
    
    if (length(valid_cols) < 2) {
      return(list(matrix = NULL, warning = "Fewer than 2 columns with non-zero variance"))
    }
    
    df <- df[, valid_cols, drop = FALSE]
    
    if (any(sapply(df, function(x) any(is.infinite(x))))) {
      return(list(matrix = NULL, warning = "Infinite values detected in the data"))
    }
    
    cor_matrix <- cor(df, use = "pairwise.complete.obs")
    warning_msg <- if (length(valid_cols) < 4) {
      paste("Excluded columns with zero variance:", 
            paste(setdiff(c("CSAT_Score", "SLA_Percent", "Median_Resolution_Time", "Mean_Resolution_Groups"), 
                          valid_cols), collapse = ", "))
    } else {
      NULL
    }
    
    list(matrix = cor_matrix, warning = warning_msg)
  })
  
  output$corr_plot <- renderPlot({
    corr <- corr_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(corr$matrix) || !is.matrix(corr$matrix)) {
      plot.new()
      text(0.5, 0.5, ifelse(is.null(corr$warning), 
                            "No hay datos suficientes para el correlograma", 
                            corr$warning), 
           cex = 1.5, col = text_color)
    } else {
      colx <- colorRampPalette(c("#BB4444", "#EE9988", "#FFFFFF", "#4477AA", "#203864"))
      corrplot(corr$matrix, method = "shade", shade.col = NA, tl.col = text_color, tl.srt = 45,
               col = colx(200), addCoef.col = "black", number.cex = 1.5, order = "AOE", 
               addgrid.col = NA, bg = "transparent")
      if (!is.null(corr$warning)) {
        mtext(corr$warning, side = 3, line = 0, cex = 0.8, col = text_color)
      }
    }
  }, bg = "transparent")
  
  output$impact_bar <- renderPlotly({
    corr <- corr_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(corr$matrix) || !is.matrix(corr$matrix) || all(is.na(corr$matrix))) {
      return(plot_ly() %>% layout(title = "No hay datos suficientes para el impacto en CSAT"))
    }
    
    if (!"CSAT_Score" %in% rownames(corr$matrix)) {
      return(plot_ly() %>% layout(title = "CSAT_Score no está en la matriz de correlación"))
    }
    
    csat_cor <- corr$matrix["CSAT_Score", , drop = FALSE]
    valid_cols <- colnames(csat_cor)[colnames(csat_cor) != "CSAT_Score"]
    
    if (length(valid_cols) == 0) {
      return(plot_ly() %>% layout(title = "No hay variables para correlacionar con CSAT Score"))
    }
    
    csat_cor <- csat_cor[, valid_cols, drop = FALSE]
    
    impact_df <- data.frame(
      Variable = valid_cols,
      Correlation = abs(csat_cor[1, ])
    ) %>%
      arrange(desc(Correlation))
    
    plot_ly(impact_df, x = ~reorder(Variable, Correlation), y = ~Correlation, type = "bar",
            marker = list(color = trend_color())) %>%
      layout(
        title = list(text = "Impacto en CSAT Score", font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "Variable", color = axis_color, showgrid = FALSE),
        yaxis = list(title = "Correlación Absoluta", color = axis_color, showgrid = FALSE),
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  
 ######################### BOXPLOTS ###################################################### 
  
  
  # Boxplot Data and Checkbox Filter
  observeEvent(processed_data(), {
    df <- processed_data()
    if (!is.null(df)) {
      updateCheckboxGroupInput(session, "issue_filter",
                               choices = unique(df$issue_classification),
                               selected = unique(df$issue_classification))
    }
  })
  
  boxplot_data <- reactive({
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    df <- df %>%
      group_by(channel, csat_rated_group_name, issue_classification, month_year) %>%
      summarise(
        CSAT_Score = ifelse(n() > 0 && sum(!is.na(classification)) > 0,
                            round(sum(classification == "PROMOTER", na.rm = TRUE) / n() * 100, 1), NA),
        csat_rating_received = mean(csat_rating_received, na.rm = TRUE),  # Added csat_rating_received
        resolved_in_sla = mean(resolved_in_sla, na.rm = TRUE),
        total_groups = mean(total_groups, na.rm = TRUE),
        resolution_time_day = mean(resolution_time_day, na.rm = TRUE),
        .groups = "drop"
      )
    
    if (!is.null(input$issue_filter) && length(input$issue_filter) > 0) {
      df <- df %>% filter(issue_classification %in% input$issue_filter)
    }
    
    df
  })
  
  # Boxplot Outputs
  output$channel_boxplot <- renderPlotly({
    df <- boxplot_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(df) || nrow(df) == 0) return(plot_ly())
    
    plot_ly(df, x = ~channel, y = as.formula(paste0("~", input$boxplot_metric)), 
            type = "box", marker = list(color = trend_color())) %>%
      layout(
        title = list(text = paste("Boxplot por Channel -", 
                                  names(which(c("CSAT_Score" = "CSAT Score",
                                                "csat_rating_received" = "CSAT Rating Received",
                                                "resolved_in_sla" = "SLA",
                                                "total_groups" = "Resolution Groups",
                                                "resolution_time_day" = "Resolution Time") == input$boxplot_metric))),
                     font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "Channel", color = axis_color, showgrid = FALSE),
        yaxis = list(title = input$boxplot_metric, color = axis_color, showgrid = FALSE),
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  output$group_boxplot <- renderPlotly({
    df <- boxplot_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(df) || nrow(df) == 0) return(plot_ly())
    
    plot_ly(df, x = ~csat_rated_group_name, y = as.formula(paste0("~", input$boxplot_metric)), 
            type = "box", marker = list(color = trend_color())) %>%
      layout(
        title = list(text = paste("Boxplot por CSAT Group -", 
                                  names(which(c("CSAT_Score" = "CSAT Score",
                                                "csat_rating_received" = "CSAT Rating Received",
                                                "resolved_in_sla" = "SLA",
                                                "total_groups" = "Resolution Groups",
                                                "resolution_time_day" = "Resolution Time") == input$boxplot_metric))),
                     font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "CSAT Rated Group", color = axis_color, showgrid = FALSE),
        yaxis = list(title = input$boxplot_metric, color = axis_color, showgrid = FALSE),
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  output$issue_boxplot <- renderPlotly({
    df <- boxplot_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(df) || nrow(df) == 0) return(plot_ly())
    
    plot_ly(df, x = ~issue_classification, y = as.formula(paste0("~", input$boxplot_metric)), 
            type = "box", marker = list(color = trend_color())) %>%
      layout(
        title = list(text = paste("Boxplot por Issue Classification -", 
                                  names(which(c("CSAT_Score" = "CSAT Score",
                                                "csat_rating_received" = "CSAT Rating Received",
                                                "resolved_in_sla" = "SLA",
                                                "total_groups" = "Resolution Groups",
                                                "resolution_time_day" = "Resolution Time") == input$boxplot_metric))),
                     font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "Issue Classification", color = axis_color, showgrid = FALSE),
        yaxis = list(title = input$boxplot_metric, color = axis_color, showgrid = FALSE),
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  
  
  
  rf_model_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    rf_data <- df %>%
      filter(classification %in% c("PROMOTER", "DETRACTOR")) %>%
      select(classification, total_groups, resolution_time_day, resolved_in_sla) %>%
      na.omit()
    
    if (nrow(rf_data) < 10) return(NULL)
    
    rf_data$classification <- as.factor(rf_data$classification)
    rf_data$total_groups <- as.numeric(rf_data$total_groups)
    rf_data$resolution_time_day <- as.numeric(rf_data$resolution_time_day)
    rf_data$resolved_in_sla <- as.numeric(rf_data$resolved_in_sla)
    
    sla_values <- unique(rf_data$resolved_in_sla)
    if (!all(sla_values %in% c(0, 1))) {
      message("Advertencia: resolved_in_sla contiene valores no binarios: ", paste(sla_values, collapse = ", "))
    }
    
    set.seed(123)
    balanced_data <- upSample(rf_data[, -which(names(rf_data) == "classification")], rf_data$classification, 
                              list = FALSE, yname = "classification")
    
    train_control <- trainControl(method = "cv", number = 5, 
                                  classProbs = TRUE, 
                                  summaryFunction = twoClassSummary, 
                                  savePredictions = TRUE)
    
    tune_grid <- expand.grid(mtry = c(1:3))
    
    rf_model_cv <- tryCatch({
      train(classification ~ total_groups + resolution_time_day + resolved_in_sla,
            data = balanced_data,
            method = "rf",
            trControl = train_control,
            tuneGrid = tune_grid,
            ntree = 50,
            metric = "ROC",
            importance = TRUE)
    }, error = function(e) {
      message("Error en el entrenamiento del modelo RF: ", e$message)
      return(NULL)
    })
    
    if (is.null(rf_model_cv)) return(NULL)
    
    best_model_idx <- which.max(rf_model_cv$results$ROC)
    accuracy <- rf_model_cv$results$Accuracy[best_model_idx]
    sensitivity <- rf_model_cv$results$Sens[best_model_idx]
    specificity <- rf_model_cv$results$Spec[best_model_idx]
    auc <- rf_model_cv$results$ROC[best_model_idx]
    
    is_good_model <- ifelse(!is.na(auc) && auc > 0.75, "Sí", "No")
    
    cm_table <- table(rf_model_cv$pred$pred, rf_model_cv$pred$obs)
    
    importance <- tryCatch({
      imp <- importance(rf_model_cv$finalModel, type = 1)
      if (is.null(imp) || all(is.na(imp))) {
        imp <- rep(0, length(c("total_groups", "resolution_time_day", "resolved_in_sla")))
        names(imp) <- c("total_groups", "resolution_time_day", "resolved_in_sla")
      }
      imp
    }, error = function(e) {
      message("Error al calcular importancia: ", e$message)
      imp <- rep(0, length(c("total_groups", "resolution_time_day", "resolved_in_sla")))
      names(imp) <- c("total_groups", "resolution_time_day", "resolved_in_sla")
      imp
    })
    
    max_imp <- max(importance, na.rm = TRUE)
    importance_scaled <- if (is.na(max_imp) || max_imp == 0) {
      importance
    } else {
      round(100 * importance / max_imp, 2)
    }
    
    promoter_data <- balanced_data[balanced_data$classification == "PROMOTER", ]
    detractor_data <- balanced_data[balanced_data$classification == "DETRACTOR", ]
    
    ranges <- list(
      total_groups = quantile(promoter_data$total_groups, probs = c(0.25, 0.75), na.rm = TRUE),
      resolution_time_day = quantile(promoter_data$resolution_time_day, probs = c(0.25, 0.75), na.rm = TRUE)
    )
    
    sla_counts_promoter <- table(promoter_data$resolved_in_sla)
    sla_total_promoter <- sum(sla_counts_promoter)
    sla_prop_1_promoter <- ifelse(is.na(sla_counts_promoter["1"]), 0, sla_counts_promoter["1"] / sla_total_promoter)
    sla_prop_0_promoter <- ifelse(is.na(sla_counts_promoter["0"]), 0, sla_counts_promoter["0"] / sla_total_promoter)
    recommended_sla <- ifelse(sla_prop_1_promoter > 0.5, 1, 0)
    
    sla_counts_detractor <- table(detractor_data$resolved_in_sla)
    sla_total_detractor <- sum(sla_counts_detractor)
    sla_prop_1_detractor <- ifelse(is.na(sla_counts_detractor["1"]), 0, sla_counts_detractor["1"] / sla_total_detractor)
    sla_prop_0_detractor <- ifelse(is.na(sla_counts_detractor["0"]), 0, sla_counts_detractor["0"] / sla_total_detractor)
    
    sla_is_relevant <- if (sla_prop_1_promoter > 0.5 && sla_prop_0_detractor > 0.5) {
      "Sí (PROMOTER tiende a 1, DETRACTOR tiende a 0)"
    } else {
      "No (sin tendencia clara entre clases)"
    }
    
    ranges$resolved_in_sla <- list(
      value = recommended_sla,
      prop_1_promoter = sla_prop_1_promoter,
      prop_0_promoter = sla_prop_0_promoter,
      prop_1_detractor = sla_prop_1_detractor,
      prop_0_detractor = sla_prop_0_detractor,
      is_relevant = sla_is_relevant
    )
    
    midpoints <- data.frame(
      total_groups = mean(ranges$total_groups),
      resolution_time_day = mean(ranges$resolution_time_day),
      resolved_in_sla = recommended_sla
    )
    
    prob <- predict(rf_model_cv, newdata = midpoints, type = "prob")
    promoter_prob <- prob[1, "PROMOTER"]
    
    list(model = rf_model_cv, data = balanced_data, accuracy = accuracy, sensitivity = sensitivity, 
         specificity = specificity, auc = auc, is_good_model = is_good_model, cm_table = cm_table, 
         importance = importance_scaled, promoter_ranges = ranges, promoter_prob = promoter_prob)
  })
  
  output$rf_summary <- renderPrint({
    rf <- rf_model_data()
    group_name <- ifelse(input$group == "All", "Todos", input$group)
    issue_name <- ifelse(input$issue == "All", "Todos", input$issue)
    
    if (is.null(rf)) {
      cat("No hay datos suficientes para entrenar el modelo Random Forest con CV=5 para los filtros seleccionados\n")
      cat("Modelo Random Forest\n")
      cat("CSAT Rated Group:", group_name, "\n")
      cat("Issue Classification:", issue_name, "\n", "\n")
    } else {
      cm <- rf$cm_table
      sensitivity <- cm["PROMOTER", "PROMOTER"] / (cm["PROMOTER", "PROMOTER"] + cm["DETRACTOR", "PROMOTER"])
      specificity <- cm["DETRACTOR", "DETRACTOR"] / (cm["DETRACTOR", "DETRACTOR"] + cm["PROMOTER", "DETRACTOR"])
      precision_promoter <- cm["PROMOTER", "PROMOTER"] / (cm["PROMOTER", "PROMOTER"] + cm["PROMOTER", "DETRACTOR"])
      
      cat("Modelo Random Forest\n")
      cat("CSAT Rated Group:", group_name, "\n")
      cat("Issue Classification:", issue_name, "\n", "\n")
      
      cat("Resultados del mejor modelo (CV=5):\n")
      cat("Precisión (PROMOTER):", sprintf("%.2f%%", precision_promoter * 100), "\n")
      cat("Sensibilidad (TPR para PROMOTER):", sprintf("%.2f%%", sensitivity * 100), "\n")
      cat("Especificidad (TNR para DETRACTOR):", sprintf("%.2f%%", specificity * 100), "\n")
      cat("AUC:", round(rf$auc, 3), "\n")
      cat("¿Es un buen modelo?:", rf$is_good_model, "(basado en AUC > 0.75)\n")
      cat("\nMatriz de confusión (mejor modelo):\n")
      print(cm)
      cat("\nRecomendación de rangos para clasificación PROMOTER:\n")
      cat("GRUPOS DE RESOLUCIÓN (STEPS):", sprintf("%.2f - %.2f", rf$promoter_ranges$total_groups[1], rf$promoter_ranges$total_groups[2]), "\n")
      cat("TIEMPO DE RESOLUCIÓN (DÍAS):", sprintf("%.3f - %.3f", rf$promoter_ranges$resolution_time_day[1], rf$promoter_ranges$resolution_time_day[2]), "\n")
      cat("RESOLUCIÓN EN SLA: Recomendado =", rf$promoter_ranges$resolved_in_sla$value, "\n")
      cat("  PROMOTER - SLA=1:", sprintf("%.2f%%", rf$promoter_ranges$resolved_in_sla$prop_1_promoter * 100), 
          ", SLA=0:", sprintf("%.2f%%", rf$promoter_ranges$resolved_in_sla$prop_0_promoter * 100), "\n")
      cat("  DETRACTOR - SLA=1:", sprintf("%.2f%%", rf$promoter_ranges$resolved_in_sla$prop_1_detractor * 100), 
          ", SLA=0:", sprintf("%.2f%%", rf$promoter_ranges$resolved_in_sla$prop_0_detractor * 100), "\n")
      cat("  ¿Es relevante?:", rf$promoter_ranges$resolved_in_sla$is_relevant, "\n")
      cat("\nImportancia de variables (escalada, referencia del modelo):\n")
      print(round(rf$importance, 2))
    }
  })
  
  rf_prediction <- eventReactive(input$predict_btn_rf, {
    rf <- rf_model_data()
    if (is.null(rf)) return("No hay modelo disponible para predecir. Presione 'Ejecutar' con filtros válidos.")
    
    new_data <- data.frame(
      total_groups = as.numeric(input$pred_groups_rf),
      resolution_time_day = as.double(input$pred_time_rf),
      resolved_in_sla = as.numeric(input$pred_sla_rf)
    )
    
    pred <- predict(rf$model, new_data, type = "raw")
    prob <- predict(rf$model, new_data, type = "prob")
    
    cat(
      " Predicción:", as.character(pred), "\n",
      "Probabilidad PROMOTER:", sprintf("%.2f%%", prob[,"PROMOTER"] * 100), "\n",
      "Probabilidad DETRACTOR:", sprintf("%.2f%%", prob[,"DETRACTOR"] * 100)
    )
  })
  
  output$rf_prediction <- renderPrint({
    rf_prediction()
  })
  
  dt_model_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    
    if (is.null(df) || nrow(df) == 0) {
      message("No hay datos disponibles en processed_data()")
      return(NULL)
    }
    
    cols_to_select <- c("classification", "total_groups", "resolution_time_day", "resolved_in_sla")
    if (input$channel == "All") cols_to_select <- c(cols_to_select, "channel")
    if (input$issue == "All") cols_to_select <- c(cols_to_select, "issue_classification")
    
    dt_data <- df %>%
      select(all_of(cols_to_select)) %>%
      filter(classification %in% c("PROMOTER", "DETRACTOR")) %>%
      na.omit()
    
    if (nrow(dt_data) < 10) {
      message("Menos de 10 filas después de filtrar. Se requiere más datos.")
      return(NULL)
    }
    
    predictors <- c("total_groups", "resolution_time_day", "resolved_in_sla")
    if (input$channel == "All") predictors <- c(predictors, "channel")
    if (input$issue == "All") predictors <- c(predictors, "issue_classification")
    
    unique_sla <- unique(dt_data$resolved_in_sla)
    if (length(unique_sla) != 2 || !all(unique_sla %in% c(0, 1))) {
      message("Error: 'resolved_in_sla' debe ser binario (0, 1). Valores encontrados: ", paste(unique_sla, collapse = ", "))
      return(NULL)
    }
    
    dt_data$classification <- as.factor(dt_data$classification)
    dt_data$total_groups <- as.numeric(dt_data$total_groups)
    dt_data$resolution_time_day <- as.numeric(dt_data$resolution_time_day)
    dt_data$resolved_in_sla <- factor(dt_data$resolved_in_sla, levels = c("0", "1"))
    
    if ("channel" %in% predictors) dt_data$channel <- as.factor(dt_data$channel)
    if ("issue_classification" %in% predictors) dt_data$issue_classification <- as.factor(dt_data$issue_classification)
    
    set.seed(123)
    balanced_data <- tryCatch({
      upSample(dt_data[, -which(names(dt_data) == "classification")], 
               dt_data$classification, 
               list = FALSE, 
               yname = "classification")
    }, error = function(e) {
      message("Error al balancear datos: ", e$message)
      return(NULL)
    })
    
    if (is.null(balanced_data)) return(NULL)
    
    train_control <- trainControl(method = "cv", 
                                  number = 5, 
                                  classProbs = TRUE, 
                                  summaryFunction = twoClassSummary, 
                                  savePredictions = TRUE)
    
    tune_grid <- expand.grid(cp = seq(0.01, 0.5, by = 0.05))
    
    formula <- as.formula(paste("classification ~", paste(predictors, collapse = " + ")))
    
    dt_model_cv <- tryCatch({
      train(formula,
            data = balanced_data,
            method = "rpart",
            trControl = train_control,
            tuneGrid = tune_grid,
            metric = "ROC")
    }, error = function(e) {
      message("Error al entrenar el modelo DT: ", e$message)
      return(NULL)
    })
    
    if (is.null(dt_model_cv)) return(NULL)
    
    best_model_idx <- which.max(dt_model_cv$results$ROC)
    optimal_cp <- dt_model_cv$results$cp[best_model_idx]
    accuracy <- dt_model_cv$results$Accuracy[best_model_idx]
    sensitivity <- dt_model_cv$results$Sens[best_model_idx]
    specificity <- dt_model_cv$results$Spec[best_model_idx]
    auc <- dt_model_cv$results$ROC[best_model_idx]
    
    is_good_model <- ifelse(!is.na(auc) && auc > 0.75, "Sí", "No")
    
    cm_table <- table(dt_model_cv$pred$pred, dt_model_cv$pred$obs)
    
    list(model = dt_model_cv, 
         data = balanced_data, 
         accuracy = accuracy, 
         sensitivity = sensitivity, 
         specificity = specificity, 
         auc = auc, 
         is_good_model = is_good_model, 
         cm_table = cm_table, 
         optimal_cp = optimal_cp,
         predictors = predictors)
  })
  
  output$dt_summary <- renderPrint({
    dt <- dt_model_data()
    group_name <- ifelse(input$group == "All", "Todos", input$group)
    issue_name <- ifelse(input$issue == "All", "Todos", input$issue)
    
    if (is.null(dt)) {
      cat("No hay datos suficientes para entrenar el modelo Árbol de Decisión para los filtros seleccionados\n")
      cat("Modelo Árbol de Decisión (CV=5)\n")
      cat("CSAT Rated Group:", group_name, "\n")
      cat("Issue Classification:", issue_name, "\n", "\n")
    } else {
      importance <- dt$model$finalModel$variable.importance
      importance_scaled <- round(100 * importance / max(importance), 2)
      
      cat("Modelo Árbol de Decisión\n")      
      cat("CSAT Rated Group:", group_name, "\n")
      cat("Issue Classification:", issue_name, "\n", "\n")
      
      cat("Resultados del mejor modelo (CV=5):\n")
      cat("Sensibilidad (TPR para PROMOTER):", sprintf("%.2f%%", dt$sensitivity * 100), "\n")
      cat("Especificidad (TNR para DETRACTOR):", sprintf("%.2f%%", dt$specificity * 100), "\n")
      cat("AUC:", round(dt$auc, 3), "\n")
      cat("Optimal cp:", sprintf("%.4f", dt$optimal_cp), "\n")
      cat("¿Es un buen modelo?:", dt$is_good_model, "(basado en AUC > 0.75)\n")
      cat("\nMatriz de confusión (CV=5):\n")
      print(dt$cm_table)
      
      cat("\nImportancia de las variables (escalada, referencia del modelo):\n")
      print(importance_scaled)
    }
  })
  
  output$dt_prediction <- renderPrint({
    dt_prediction()
  })
  
  dt_prediction <- eventReactive(input$predict_btn_dt, {
    dt <- dt_model_data()
    if (is.null(dt)) return("No hay modelo disponible para predecir. Presione 'Ejecutar' con filtros válidos.")
    
    new_data <- data.frame(
      total_groups = as.numeric(input$pred_groups_dt),
      resolution_time_day = as.double(input$pred_time_dt),
      resolved_in_sla = factor(input$pred_sla_dt, levels = c("0", "1"))
    )
    
    if ("channel" %in% dt$predictors) {
      if (input$channel == "All") {
        most_common_channel <- names(sort(table(dt$data$channel), decreasing = TRUE))[1]
        new_data$channel <- factor(most_common_channel, levels = levels(dt$data$channel))
      } else {
        new_data$channel <- factor(input$channel, levels = levels(dt$data$channel))
      }
    }
    
    if ("issue_classification" %in% dt$predictors) {
      if (input$issue == "All") {
        most_common_issue <- names(sort(table(dt$data$issue_classification), decreasing = TRUE))[1]
        new_data$issue_classification <- factor(most_common_issue, levels = levels(dt$data$issue_classification))
      } else {
        new_data$issue_classification <- factor(input$issue, levels = levels(dt$data$issue_classification))
      }
    }
    
    pred <- predict(dt$model, new_data, type = "raw")
    prob <- predict(dt$model, new_data, type = "prob")
    
    cat(
      " Predicción:", as.character(pred), "\n",
      "Probabilidad PROMOTER:", sprintf("%.2f%%", prob[,"PROMOTER"] * 100), "\n",
      "Probabilidad DETRACTOR:", sprintf("%.2f%%", prob[,"DETRACTOR"] * 100)
    )
  })
  
  
  output$download_dt_plot <- downloadHandler(
    filename = function() {
      # Formatear la fecha y hora en el formato deseado
      timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
      paste("decision_tree_", timestamp, ".png", sep = "")
    },
    content = function(file) {
      dt <- dt_model_data()
      if (is.null(dt)) {
        stop("No hay modelo disponible para descargar.")
      }
      
      # Obtener los valores de los filtros
      group_name <- ifelse(input$group == "All", "Todos", input$group)
      issue_name <- ifelse(input$issue == "All", "Todos", input$issue)
      channel_name <- ifelse(input$channel == "All", "Todos", input$channel)
      date_range <- paste(format(input$date_range[1], "%Y-%m-%d"), "a", format(input$date_range[2], "%Y-%m-%d"))
      
      # Crear el título
      title <- paste("\n","\n","ÁRBOL DE DECISIÓN\n","\n",
                     "CSAT Rated Group:", group_name, " | ",
                     "Issue Classification:", issue_name, " | ",
                     "Channel:", channel_name, "\n",
                     "Rango de Fechas:", date_range)
      
      png(file, width = 1200, height = 720)
      rpart.plot(dt$model$finalModel, 
                 box.palette = "RdYlGn", 
                 shadow.col = "gray", 
                 nn = TRUE, 
                 main = title)
      dev.off()
    }
  )
  
  
  
  
  
  
  # Código que se ejecuta cuando la aplicación se detiene
  onStop(function() {
    message("La aplicación se ha detenido. Realizando tareas de limpieza...")
    # Aquí puedes agregar código adicional, como guardar logs o notificaciones.
  })
  
  # Detener la aplicación cuando el usuario hace clic en el botón
  observeEvent(input$stop, {
    stopApp()
  })
  
  # Detener la aplicación cuando el usuario cierra la ventana del navegador
  session$onSessionEnded(function() {
    stopApp()
  })  
  
  
  
  ###################### DATAFRAME ###################################
  
  # Datos tabulados mensuales
  monthly_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(data.frame())
    
    df %>%
      group_by(month_year, channel, csat_rated_group_name, issue_classification) %>%
      summarise(
        CSAT_Score = ifelse(n() > 0 && sum(!is.na(classification)) > 0,
                            round(sum(classification == "PROMOTER", na.rm = TRUE) / n() * 100, 1), NA),
        SLA_Percent = ifelse(n() > 0 && sum(!is.na(resolved_in_sla)) > 0,
                             round(sum(resolved_in_sla == 1, na.rm = TRUE) / n() * 100, 1), NA),
        Avg_Resolution_Time_Days = round(mean(resolution_time_day, na.rm = TRUE), 3),
        Avg_Total_Groups = round(mean(total_groups, na.rm = TRUE), 1),
        Avg_CSAT_Rating_Received = round(mean(csat_rating_received, na.rm = TRUE), 1),
        Total_Surveys = n(),
        Promoters = sum(classification == "PROMOTER", na.rm = TRUE),
        Detractors = sum(classification == "DETRACTOR", na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(month_year, channel, csat_rated_group_name, issue_classification)
  })
  
  # Renderizar la tabla
  output$monthly_table <- DT::renderDT({
    DT::datatable(monthly_data(), 
                  options = list(pageLength = 10, autoWidth = TRUE),
                  rownames = FALSE)
  })
  
  # Botón de descarga CSV
  output$download_csv <- downloadHandler(
    filename = function() {
      paste("datos_mensuales_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      write.csv(monthly_data(), file, row.names = FALSE)
    }
  )
  
  
  
  
}
  
  
  
# Al final de app.R
args <- commandArgs(trailingOnly = TRUE)
port <- if (length(args) > 0 && grepl("port=", args[1])) {
  as.integer(sub("port=", "", args[1]))
} else {
  8080
}

cat("Argumentos recibidos:", paste(args, collapse = ", "), "\n")
cat("Iniciando Shiny en puerto:", port, "\n")

options(shiny.host = "127.0.0.1")
options(shiny.port = port)
cat("Aplicación Shiny configurada, iniciando servidor en http://127.0.0.1:", port, "\n")

# Iniciar Shiny y capturar errores
tryCatch({
  shinyApp(ui, server)
}, error = function(e) {
  cat("Error al iniciar Shiny:", e$message, "\n")
  stop(e)
})