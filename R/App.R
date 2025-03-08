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
library(forecast)      # Para Holt-Winters
library(randomForest)  # Para Random Forest
library(caret)         # Para métricas y validación cruzada
library(pROC)          # Para ROC y AUC
library(rpart)         # Para árbol de decisión
library(rpart.plot)    # Para graficar el árbol

# Default URL
DEFAULT_URL <- "https://raw.githubusercontent.com/ringoquimico/Portfolio/refs/heads/main/Data%20Sources/bank_call_center_feedback.csv"

# UI definition
ui <- fluidPage(
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  tags$head(
    tags$style(HTML("
      .dark-mode .nav-tabs > li > a { color: green !important; }
      .dark-mode .nav-tabs > li.active > a { color: white !important; }
    "))
  ),
  titlePanel("NetworkApp 1.0.0"),
  sidebarLayout(
    sidebarPanel(
      textInput("data_url", "URL de la Fuente de Datos:", 
                value = DEFAULT_URL,
                placeholder = "Ingrese la URL del archivo CSV"),
      dateRangeInput("date_range", "Rango de Fechas:",
                     start = NULL, end = NULL, min = NULL, max = NULL,
                     format = "yyyy-mm-dd"),
      selectInput("group", "CSAT Rated Group Name:",
                  choices = c("Todos" = "All")),
      selectInput("issue", "Issue Classification:",
                  choices = c("Todos" = "All")),
      switchInput("toggle_mode", "Modo Oscuro", value = FALSE),
      actionButton("run_btn", "Ejecutar", class = "btn-primary"),
      verbatimTextOutput("debug_info")
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Red de Interacciones", visNetworkOutput("network", height = "600px")),
        tabPanel("Top 10 Interacciones", plotlyOutput("top_interactions")),
        tabPanel("Estadísticas",
                 fluidRow(
                   column(6, h3("CSAT Score"), verbatimTextOutput("csat_score")),
                   column(6, h3("SLA"), verbatimTextOutput("sla_stats")),
                   column(6, h3("Resolution Time"), verbatimTextOutput("resolution_time")),
                   column(6, h3("Resolution Groups"), verbatimTextOutput("resolution_groups"))
                 )),
        tabPanel("Tendencias",
                 plotlyOutput("csat_trend", height = "300px"),
                 plotlyOutput("resolution_time_trend", height = "300px"),
                 plotlyOutput("resolution_groups_trend", height = "300px"),
                 plotlyOutput("sla_trend", height = "300px")),
        tabPanel("Correlograma",
                 plotOutput("corr_plot", height = "400px"),
                 plotlyOutput("impact_bar", height = "400px")),
        tabPanel("Boxplots",
                 plotlyOutput("csat_boxplot", height = "400px"),
                 plotlyOutput("rt_boxplot", height = "400px"),
                 plotlyOutput("rg_boxplot", height = "400px")),
        tabPanel("Random Forest",
                 fluidRow(
                   column(12, h3("Resumen"), 
                          verbatimTextOutput("rf_summary")
                   )
                 ),
                 fluidRow(
                   column(12, h3("Predicción"), 
                          numericInput("pred_groups_rf", "Número de Grupos:", value = 1, min = 1),
                          numericInput("pred_time_rf", "Tiempo de Resolución (días):", value = 1, min = 0, step = 0.1),
                          selectInput("pred_sla_rf", "Resuelto en SLA:", choices = c("Sí" = 1, "No" = 0)),
                          actionButton("predict_btn_rf", "Predecir"),
                          verbatimTextOutput("rf_prediction"))
                 )
        ),
        tabPanel("Árbol de Decisión",
                 fluidRow(
                   column(12, style = "width: 1200px;", h3("Resumen"), 
                          verbatimTextOutput("dt_summary")
                   )
                 ),
                 fluidRow(
                   column(12, h3("Predicción"), 
                          numericInput("pred_groups_dt", "Número de Grupos:", value = 1, min = 1),
                          numericInput("pred_time_dt", "Tiempo de Resolución (días):", value = 1, min = 0, step = 0.1),
                          selectInput("pred_sla_dt", "Resuelto en SLA:", choices = c("Sí" = 1, "No" = 0)),
                          actionButton("predict_btn_dt", "Predecir"),
                          downloadButton("download_dt_plot", "Descargar Árbol (PNG)"),
                          verbatimTextOutput("dt_prediction"))
                 )
        )
      )
    )
  )
)

# Server definition
server <- function(input, output, session) {
  # Define themes
  light_theme <- bs_theme(version = 5, bootswatch = "flatly")
  dark_theme <- bs_theme(version = 5, bootswatch = "darkly")
  
  # Reactive data loading
  data <- reactive({
    req(input$data_url)
    tryCatch({
      df <- read.csv(input$data_url, sep = ";", quote = '"', encoding = "UTF-8") %>%
        rename(
          Start.of.Month = `Start of Month`,
          Resolution.Time.Days = `RESOLUTION TIME (min)`,
          Resolved.in.SLA = `RESOLVED IN SLA`,
          CSAT.Rated.Group.Name = `CSAT Rated Group Name`,
          Issue.Classification = `Issue Classification`,
          Group.Name.History = `Group Name History`,
          Resolution.Groups = `Total Groups`,
          CSAT.Rating.Received = `CSAT Rating Received`
        ) %>%
        mutate(
          Start.of.Month = as.Date(Start.of.Month, format = "%d/%m/%Y"),
          Resolution.Time.Days = as.double(gsub(",", ".", Resolution.Time.Days)) / 1440,  # Convertir minutos a días
          Month_Year = format(Start.of.Month, "%Y-%m"),
          Resolved.in.SLA = case_when(
            is.na(Resolved.in.SLA) & Resolution.Time.Days < 1 ~ 1,
            is.na(Resolved.in.SLA) ~ 0,
            TRUE ~ as.numeric(Resolved.in.SLA)
          ),
          Resolution.Time.Days = case_when(
            is.na(Resolution.Time.Days) & (Resolved.in.SLA == 1 | CLASSIFICATION == "PROMOTER") ~ 0.5,
            is.na(Resolution.Time.Days) ~ 1,
            TRUE ~ Resolution.Time.Days
          )
        )
      df
    }, error = function(e) {
      showNotification(paste("Error al cargar los datos:", e$message), type = "error")
      NULL
    })
  })
  
  # Update UI inputs when data changes
  observeEvent(data(), {
    df <- data()
    if (!is.null(df) && nrow(df) > 0) {
      updateDateRangeInput(session, "date_range",
                           start = min(df$Start.of.Month, na.rm = TRUE),
                           end = max(df$Start.of.Month, na.rm = TRUE),
                           min = min(df$Start.of.Month, na.rm = TRUE),
                           max = max(df$Start.of.Month, na.rm = TRUE))
      updateSelectInput(session, "group",
                        choices = c("Todos" = "All", unique(df$CSAT.Rated.Group.Name)))
      updateSelectInput(session, "issue",
                        choices = c("Todos" = "All", sort(unique(df$Issue.Classification))))
    }
  })
  
  # Toggle theme
  observeEvent(input$toggle_mode, {
    if (input$toggle_mode) {
      session$setCurrentTheme(dark_theme)
      session$sendCustomMessage(type = "toggleDarkMode", message = TRUE)
    } else {
      session$setCurrentTheme(light_theme)
      session$sendCustomMessage(type = "toggleDarkMode", message = FALSE)
    }
  })
  
  # Reactive filtered data
  filtered_data <- reactive({
    req(data(), input$date_range[1], input$date_range[2])
    df <- data()
    
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    df <- df %>% 
      filter(Start.of.Month >= input$date_range[1],
             Start.of.Month <= input$date_range[2])
    
    if (input$group != "All") {
      df <- df %>% filter(CSAT.Rated.Group.Name == input$group)
    }
    
    if (input$issue != "All") {
      df <- df %>% filter(Issue.Classification == input$issue)
    }
    
    df %>% filter(!is.na(Group.Name.History) & Group.Name.History != "")
  })
  
  # Datos procesados solo cuando se presiona "Ejecutar"
  processed_data <- eventReactive(input$run_btn, {
    filtered_data()
  })
  
  # Debug info
  output$debug_info <- renderPrint({
    df <- processed_data()
    if (is.null(df)) {
      cat("No hay datos disponibles o no se ha ejecutado\n")
    } else {
      cat("Encuestas analizadas:", nrow(df), "\n")
    }
  })
  
  # Network data preparation
  network_data <- eventReactive(input$run_btn, {
    data <- processed_data()
    
    if (is.null(data) || nrow(data) == 0) return(list(nodes = data.frame(), edges = data.frame()))
    
    data$Group.Name.History <- gsub(", ", ",", data$Group.Name.History)
    data$Group.Name.History <- lapply(strsplit(data$Group.Name.History, ","), as.vector)
    
    edges <- data.frame(from = character(), to = character(), weight = numeric())
    for (groups in data$Group.Name.History) {
      if (length(groups) == 1) {
        edges <- rbind(edges, data.frame(from = groups[1], to = groups[1], weight = 1))
      } else if (length(groups) > 1) {
        for (i in 1:(length(groups) - 1)) {
          edges <- rbind(edges, data.frame(from = groups[i], to = groups[i + 1], weight = 1))
        }
      }
    }
    edges <- edges %>% group_by(from, to) %>% summarise(weight = sum(weight), .groups = "drop")
    
    node_occurrences <- table(unlist(data$Group.Name.History))
    if (length(node_occurrences) == 0) return(list(nodes = data.frame(), edges = edges))
    
    occurrences <- as.numeric(node_occurrences)
    normalized_sizes <- (occurrences - min(occurrences)) / (max(occurrences) - min(occurrences))
    node_sizes <- 20 + (100 - 20) * normalized_sizes
    
    nodes <- data.frame(
      id = names(node_occurrences),
      label = names(node_occurrences),
      value = node_sizes,
      color = viridis(length(node_occurrences), alpha = 0.8)[rank(normalized_sizes)]
    )
    
    list(nodes = nodes, edges = edges %>% rename(value = weight))
  })
  
  # Render network
  output$network <- renderVisNetwork({
    data <- network_data()
    if (nrow(data$nodes) == 0) {
      return(visNetwork(data.frame(id = 1, label = "Sin datos"), data.frame(), width = "100%", height = "600px") %>%
               visNodes(shape = "dot", font = list(size = 15, color = ifelse(input$toggle_mode, "white", "black"))))
    }
    visNetwork(data$nodes, data$edges, width = "100%", height = "600px") %>%
      visEdges(arrows = "to", color = list(color = "gray", opacity = 0.5)) %>%
      visNodes(shape = "dot", font = list(size = 15, color = ifelse(input$toggle_mode, "white", "black"))) %>%
      visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
      visPhysics(solver = "forceAtlas2Based", forceAtlas2Based = list(gravitationalConstant = -50, centralGravity = 0.01))
  })
  
  # Top 10 interactions
  top_interactions_data <- eventReactive(input$run_btn, {
    edges <- network_data()$edges
    if (nrow(edges) == 0) return(data.frame(group_pairs = character(), weights = numeric()))
    top_10 <- edges %>% arrange(desc(value)) %>% head(10)
    group_pairs <- paste(top_10$from, "-", top_10$to)
    data.frame(group_pairs, weights = top_10$value) %>% arrange(weights)
  })
  
  # Render top interactions
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
      textposition = "auto",
      textfont = list(size = 12, color = text_color)
    ) %>%
      layout(
        title = list(text = "Top 10 Interacciones entre Grupos", font = list(size = 20, color = text_color), x = 0),
        xaxis = list(title = "", showticklabels = FALSE, showgrid = FALSE, color = axis_color),
        yaxis = list(title = "", categoryorder = "array", categoryarray = df_top$group_pairs, showgrid = FALSE, color = axis_color),
        margin = list(l = 150, r = 50, t = 50, b = 50),
        bargap = 0.2,
        showlegend = FALSE,
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  # Statistics calculations
  stats_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(list(csat_stats = list(avg = NA), sla_stats = list(avg = NA), resolution_time = list(), resolution_groups = list()))
    surveys_df <- nrow(df)
    
    csat_by_month <- df %>%
      group_by(Month_Year) %>%
      summarise(CSAT_Score = ifelse(n() > 0 && sum(!is.na(CLASSIFICATION)) > 0, 
                                    round(sum(CLASSIFICATION == "PROMOTER", na.rm = TRUE) / n() * 100, 1), NA),
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
      promoters = sum(df$CLASSIFICATION == "PROMOTER", na.rm = TRUE),
      detractors = sum(df$CLASSIFICATION == "DETRACTOR", na.rm = TRUE)
    )
    
    sla_by_month <- df %>%
      group_by(Month_Year) %>%
      summarise(SLA_Percent = ifelse(n() > 0 && sum(!is.na(Resolved.in.SLA)) > 0, 
                                     round(sum(Resolved.in.SLA == 1, na.rm = TRUE) / n() * 100, 1), NA),
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
    
    rt <- df$Resolution.Time.Days
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
      avg = round(rt_mean, 1),
      std = round(sd(rt, na.rm = TRUE), 1),
      q1 = round(rt_quartiles[1], 1),
      q2 = round(rt_quartiles[2], 1),
      q3 = round(rt_quartiles[3], 1),
      outliers_lower = rt_outliers_lower,
      outliers_upper = rt_outliers_upper,
      skew = rt_skew
    )
    
    rg <- df$Resolution.Groups
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
  
  # Render statistics
  output$csat_score <- renderPrint({
    csat <- stats_data()$csat_stats
    if (is.na(csat$avg)) {
      cat("No data")
    } else {
      cat(sprintf("Min: %.1f\nQ1: %.1f\nMedian: %.1f\nQ3: %.1f\nMax: %.1f\nAvg: %.1f\nStd: %.1f\nOutliers Inferiores: %d\nOutliers Superiores: %d\nDistribución: %s\nSurveys: %d\nPromoters: %d\nDetractors: %d",
                  csat$min, csat$q1, csat$q2, csat$q3, csat$max, csat$avg, csat$std, csat$outliers_lower, csat$outliers_upper, csat$skew,
                  csat$surveys, csat$promoters, csat$detractors))
    }
  })
  
  output$sla_stats <- renderPrint({
    sla <- stats_data()$sla_stats
    if (is.na(sla$avg)) {
      cat("No data")
    } else {
      cat(sprintf("Min: %.1f\nQ1: %.1f\nMedian: %.1f\nQ3: %.1f\nMax: %.1f\nAvg: %.1f\nStd: %.1f\nOutliers Inferiores: %d\nOutliers Superiores: %d\nDistribución: %s",
                  sla$min, sla$q1, sla$q2, sla$q3, sla$max, sla$avg, sla$std, sla$outliers_lower, sla$outliers_upper, sla$skew))
    }
  })
  
  output$resolution_time <- renderPrint({
    rt <- stats_data()$resolution_time
    if (length(rt) == 0) {
      cat("No data")
    } else {
      cat(sprintf("Min: %.1f\nQ1: %.1f\nMedian: %.1f\nQ3: %.1f\nMax: %.1f\nAvg: %.1f\nStd: %.1f\nOutliers Inferiores: %d\nOutliers Superiores: %d\nDistribución: %s",
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
  
  # Trends data preparation
  trends_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    if (is.null(df) || nrow(df) == 0) return(data.frame())
    
    trends <- df %>%
      group_by(Month_Year) %>%
      summarise(
        CSAT_Score = ifelse(n() > 0 && sum(!is.na(CLASSIFICATION)) > 0, 
                            round(sum(CLASSIFICATION == "PROMOTER", na.rm = TRUE) / n() * 100, 1), NA),
        Median_Resolution_Time = ifelse(sum(!is.na(Resolution.Time.Days)) > 0, 
                                        median(Resolution.Time.Days, na.rm = TRUE), NA),
        Mean_Resolution_Groups = ifelse(sum(!is.na(Resolution.Groups)) > 0, 
                                        mean(Resolution.Groups, na.rm = TRUE), NA),
        SLA_Percent = ifelse(n() > 0 && sum(!is.na(Resolved.in.SLA)) > 0, 
                             round(sum(Resolved.in.SLA == 1, na.rm = TRUE) / n() * 100, 1), NA),
        .groups = "drop"
      ) %>%
      arrange(as.Date(paste0(Month_Year, "-01")))
    
    last_date <- as.Date(paste0(max(trends$Month_Year), "-01"))
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
        Month_Year = future_months,
        CSAT_Score = as.numeric(pred_csat$mean),
        CI_Lower_CSAT = as.numeric(pred_csat$lower),
        CI_Upper_CSAT = as.numeric(pred_csat$upper),
        Is_Projection_CSAT = TRUE
      )
      
      future_df_sla <- data.frame(
        Month_Year = future_months,
        SLA_Percent = as.numeric(pred_sla$mean),
        CI_Lower_SLA = as.numeric(pred_sla$lower),
        CI_Upper_SLA = as.numeric(pred_sla$upper),
        Is_Projection_SLA = TRUE
      )
      
      trends$Is_Projection_CSAT <- FALSE
      trends$Is_Projection_SLA <- FALSE
      
      future_df <- full_join(future_df_csat, future_df_sla, by = "Month_Year")
      return(bind_rows(trends, future_df))
    } else if (nrow(trends) >= 2) {
      ts_csat <- ts(trends$CSAT_Score)
      hw_csat <- HoltWinters(ts_csat, gamma = FALSE)
      pred_csat <- forecast(hw_csat, h = 5, level = 95)
      
      ts_sla <- ts(trends$SLA_Percent)
      hw_sla <- HoltWinters(ts_sla, gamma = FALSE)
      pred_sla <- forecast(hw_sla, h = 5, level = 95)
      
      future_df_csat <- data.frame(
        Month_Year = future_months,
        CSAT_Score = as.numeric(pred_csat$mean),
        CI_Lower_CSAT = as.numeric(pred_csat$lower),
        CI_Upper_CSAT = as.numeric(pred_csat$upper),
        Is_Projection_CSAT = TRUE
      )
      
      future_df_sla <- data.frame(
        Month_Year = future_months,
        SLA_Percent = as.numeric(pred_sla$mean),
        CI_Lower_SLA = as.numeric(pred_sla$lower),
        CI_Upper_SLA = as.numeric(pred_sla$upper),
        Is_Projection_SLA = TRUE
      )
      
      trends$Is_Projection_CSAT <- FALSE
      trends$Is_Projection_SLA <- FALSE
      
      future_df <- full_join(future_df_csat, future_df_sla, by = "Month_Year")
      return(bind_rows(trends, future_df))
    } else {
      trends$Is_Projection_CSAT <- FALSE
      trends$Is_Projection_SLA <- FALSE
      return(trends)
    }
  })
  
  # Define colors based on mode
  trend_color <- reactive({
    if (input$toggle_mode) "#11EFE3" else "#635BFF"
  })
  
  # Render CSAT trend
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
      plot_ly(actual_data, x = ~Month_Year, y = ~CSAT_Score, type = "scatter", mode = "markers",
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
        add_trace(data = actual_data, x = ~Month_Year, y = ~CSAT_Score, type = "scatter", mode = "lines+markers",
                  name = "Actual", marker = list(color = trend_color(), size = 8), line = list(color = trend_color())) %>%
        add_trace(data = proj_data, x = ~Month_Year, y = ~CSAT_Score, type = "scatter", mode = "lines",
                  name = "Proyección", line = list(color = trend_color(), dash = "dash")) %>%
        add_ribbons(data = proj_data, x = ~Month_Year, ymin = ~CI_Lower_CSAT, ymax = ~CI_Upper_CSAT,
                    name = "IC 95%", fillcolor = "rgba(128, 128, 128, 0.2)", line = list(color = "transparent")) %>%
        layout(
          title = list(text = title_text, font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "CSAT Score (%)", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)",
          legend = list(x = 0.1, y = 0.9, font = list(color = legend_color))
        )
    }
  })
  
  # Render Resolution Time trend
  output$resolution_time_trend <- renderPlotly({
    df <- trends_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (nrow(df) == 0 || all(is.na(df$Median_Resolution_Time))) {
      return(plot_ly() %>% layout(title = "No hay datos disponibles para Tiempo de Resolución"))
    }
    
    if (nrow(df[!is.na(df$Median_Resolution_Time), ]) < 2) {
      plot_ly(df, x = ~Month_Year, y = ~Median_Resolution_Time, type = "scatter", mode = "markers",
              marker = list(color = trend_color(), size = 10)) %>%
        layout(
          title = list(text = "Tiempo de Resolución (Sin Tendencia)", font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "Días", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)"
        )
    } else {
      plot_ly(df, x = ~Month_Year, y = ~Median_Resolution_Time, type = "scatter", mode = "lines",
              line = list(color = trend_color())) %>%
        layout(
          title = list(text = "Tendencia Mediana Tiempo de Resolución", font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "Días", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)"
        )
    }
  })
  
  # Render Resolution Groups trend
  output$resolution_groups_trend <- renderPlotly({
    df <- trends_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (nrow(df) == 0 || all(is.na(df$Mean_Resolution_Groups))) {
      return(plot_ly() %>% layout(title = "No hay datos disponibles para Grupos de Resolución"))
    }
    
    if (nrow(df[!is.na(df$Mean_Resolution_Groups), ]) < 2) {
      plot_ly(df, x = ~Month_Year, y = ~Mean_Resolution_Groups, type = "scatter", mode = "markers",
              marker = list(color = trend_color(), size = 10)) %>%
        layout(
          title = list(text = "Grupos de Resolución (Sin Tendencia)", font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "Número Promedio de Grupos", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)"
        )
    } else {
      plot_ly(df, x = ~Month_Year, y = ~Mean_Resolution_Groups, type = "scatter", mode = "lines",
              line = list(color = trend_color())) %>%
        layout(
          title = list(text = "Tendencia Promedio Grupos de Resolución", font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "Número Promedio de Grupos", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)"
        )
    }
  })
  
  # Render SLA trend
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
      plot_ly(actual_data, x = ~Month_Year, y = ~SLA_Percent, type = "scatter", mode = "markers",
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
        add_trace(data = actual_data, x = ~Month_Year, y = ~SLA_Percent, type = "scatter", mode = "lines+markers",
                  name = "Actual", marker = list(color = trend_color(), size = 8), line = list(color = trend_color())) %>%
        add_trace(data = proj_data, x = ~Month_Year, y = ~SLA_Percent, type = "scatter", mode = "lines",
                  name = "Proyección", line = list(color = trend_color(), dash = "dash")) %>%
        add_ribbons(data = proj_data, x = ~Month_Year, ymin = ~CI_Lower_SLA, ymax = ~CI_Upper_SLA,
                    name = "IC 95%", fillcolor = "rgba(128, 128, 128, 0.2)", line = list(color = "transparent")) %>%
        layout(
          title = list(text = title_text, font = list(size = 16, color = text_color), x = 0),
          xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
          yaxis = list(title = "% SLA", color = axis_color, showgrid = FALSE),
          plot_bgcolor = "rgba(0,0,0,0)",
          paper_bgcolor = "rgba(0,0,0,0)",
          legend = list(x = 0.1, y = 0.9, font = list(color = legend_color))
        )
    }
  })
  
  # Correlation data preparation
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
  
  # Render correlogram
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
  
  # Render bar chart for CSAT impact
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
  
  # Render boxplots
  output$csat_boxplot <- renderPlotly({
    df <- processed_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(df) || nrow(df) == 0 || !"CSAT.Rating.Received" %in% names(df)) {
      return(plot_ly() %>% layout(title = "No hay datos o columna CSAT.Rating.Received no encontrada"))
    }
    
    plot_ly(df, x = ~Month_Year, y = ~CSAT.Rating.Received, type = "box", name = "CSAT", marker = list(color = trend_color())) %>%
      layout(
        title = list(text = "Boxplot CSAT Rating por Mes", font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
        yaxis = list(title = "CSAT Rating", color = axis_color, showgrid = FALSE),
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  output$rt_boxplot <- renderPlotly({
    df <- processed_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(df) || nrow(df) == 0 || all(is.na(df$Resolution.Time.Days))) {
      return(plot_ly() %>% layout(title = "No hay datos para RT"))
    }
    
    plot_ly(df, x = ~Month_Year, y = ~Resolution.Time.Days, type = "box", name = "RT", marker = list(color = trend_color())) %>%
      layout(
        title = list(text = "Boxplot Resolution Time por Mes", font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
        yaxis = list(title = "Días", color = axis_color, showgrid = FALSE),
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  output$rg_boxplot <- renderPlotly({
    df <- processed_data()
    text_color <- ifelse(input$toggle_mode, "white", "black")
    axis_color <- ifelse(input$toggle_mode, "white", "black")
    
    if (is.null(df) || nrow(df) == 0 || all(is.na(df$Resolution.Groups))) {
      return(plot_ly() %>% layout(title = "No hay datos para RG"))
    }
    
    plot_ly(df, x = ~Month_Year, y = ~Resolution.Groups, type = "box", name = "RG", marker = list(color = trend_color())) %>%
      layout(
        title = list(text = "Boxplot Resolution Groups por Mes", font = list(size = 16, color = text_color), x = 0),
        xaxis = list(title = "Mes", color = axis_color, showgrid = FALSE),
        yaxis = list(title = "Número de Grupos", color = axis_color, showgrid = FALSE),
        plot_bgcolor = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)"
      )
  })
  
  # Random Forest
  rf_model_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    rf_data <- df %>%
      filter(CLASSIFICATION %in% c("PROMOTER", "DETRACTOR")) %>%
      select(CLASSIFICATION, Resolution.Groups, Resolution.Time.Days, Resolved.in.SLA) %>%
      na.omit()
    
    if (nrow(rf_data) < 10) return(NULL)
    
    rf_data$CLASSIFICATION <- as.factor(rf_data$CLASSIFICATION)
    rf_data$Resolution.Groups <- as.numeric(rf_data$Resolution.Groups)
    rf_data$Resolution.Time.Days <- as.numeric(rf_data$Resolution.Time.Days)
    rf_data$Resolved.in.SLA <- as.numeric(rf_data$Resolved.in.SLA)
    
    set.seed(123)
    balanced_data <- upSample(rf_data[, -which(names(rf_data) == "CLASSIFICATION")], rf_data$CLASSIFICATION, 
                              list = FALSE, yname = "CLASSIFICATION")
    
    train_control <- trainControl(method = "cv", number = 5, 
                                  classProbs = TRUE, 
                                  summaryFunction = twoClassSummary, 
                                  savePredictions = TRUE)
    
    tune_grid <- expand.grid(mtry = c(1:3))
    
    rf_model_cv <- tryCatch({
      train(CLASSIFICATION ~ Resolution.Groups + Resolution.Time.Days + Resolved.in.SLA,
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
        imp <- rep(0, length(c("Resolution.Groups", "Resolution.Time.Days", "Resolved.in.SLA")))
        names(imp) <- c("Resolution.Groups", "Resolution.Time.Days", "Resolved.in.SLA")
      }
      imp
    }, error = function(e) {
      imp <- rep(0, length(c("Resolution.Groups", "Resolution.Time.Days", "Resolved.in.SLA")))
      names(imp) <- c("Resolution.Groups", "Resolution.Time.Days", "Resolved.in.SLA")
      imp
    })
    
    max_imp <- max(importance, na.rm = TRUE)
    importance_scaled <- if (is.na(max_imp) || max_imp == 0) {
      importance
    } else {
      round(100 * importance / max_imp, 2)
    }
    
    promoter_data <- balanced_data[balanced_data$CLASSIFICATION == "PROMOTER", ]
    detractor_data <- balanced_data[balanced_data$CLASSIFICATION == "DETRACTOR", ]
    
    ranges <- list(
      Resolution.Groups = quantile(promoter_data$Resolution.Groups, probs = c(0.25, 0.75), na.rm = TRUE),
      Resolution.Time.Days = quantile(promoter_data$Resolution.Time.Days, probs = c(0.25, 0.75), na.rm = TRUE)
    )
    
    sla_counts_promoter <- table(promoter_data$Resolved.in.SLA)
    sla_total_promoter <- sum(sla_counts_promoter)
    sla_prop_1_promoter <- ifelse(is.na(sla_counts_promoter["1"]), 0, sla_counts_promoter["1"] / sla_total_promoter)
    sla_prop_0_promoter <- ifelse(is.na(sla_counts_promoter["0"]), 0, sla_counts_promoter["0"] / sla_total_promoter)
    recommended_sla <- ifelse(sla_prop_1_promoter > 0.5, 1, 0)
    
    sla_counts_detractor <- table(detractor_data$Resolved.in.SLA)
    sla_total_detractor <- sum(sla_counts_detractor)
    sla_prop_1_detractor <- ifelse(is.na(sla_counts_detractor["1"]), 0, sla_counts_detractor["1"] / sla_total_detractor)
    sla_prop_0_detractor <- ifelse(is.na(sla_counts_detractor["0"]), 0, sla_counts_detractor["0"] / sla_total_detractor)
    
    sla_is_relevant <- if (sla_prop_1_promoter > 0.5 && sla_prop_0_detractor > 0.5) {
      "Sí (PROMOTER tiende a 1, DETRACTOR tiende a 0)"
    } else {
      "No (sin tendencia clara entre clases)"
    }
    
    ranges$Resolved.in.SLA <- list(
      value = recommended_sla,
      prop_1_promoter = sla_prop_1_promoter,
      prop_0_promoter = sla_prop_0_promoter,
      prop_1_detractor = sla_prop_1_detractor,
      prop_0_detractor = sla_prop_0_detractor,
      is_relevant = sla_is_relevant
    )
    
    midpoints <- data.frame(
      Resolution.Groups = mean(ranges$Resolution.Groups),
      Resolution.Time.Days = mean(ranges$Resolution.Time.Days),
      Resolved.in.SLA = recommended_sla
    )
    
    prob <- predict(rf_model_cv, newdata = midpoints, type = "prob")
    promoter_prob <- prob[1, "PROMOTER"]
    
    list(model = rf_model_cv, data = balanced_data, accuracy = accuracy, sensitivity = sensitivity, 
         specificity = specificity, auc = auc, is_good_model = is_good_model, cm_table = cm_table, 
         importance = importance_scaled, promoter_ranges = ranges, promoter_prob = promoter_prob)
  })
  
  # Render Random Forest summary
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
      cat("GRUPOS DE RESOLUCIÓN (STEPS):", sprintf("%.2f - %.2f", rf$promoter_ranges$Resolution.Groups[1], rf$promoter_ranges$Resolution.Groups[2]), "\n")
      cat("TIEMPO DE RESOLUCIÓN (DÍAS):", sprintf("%.2f - %.2f", rf$promoter_ranges$Resolution.Time.Days[1], rf$promoter_ranges$Resolution.Time.Days[2]), "\n")
      cat("RESOLUCIÓN EN SLA: Recomendado =", rf$promoter_ranges$Resolved.in.SLA$value, "\n")
      cat("  PROMOTER - SLA=1:", sprintf("%.2f%%", rf$promoter_ranges$Resolved.in.SLA$prop_1_promoter * 100), 
          ", SLA=0:", sprintf("%.2f%%", rf$promoter_ranges$Resolved.in.SLA$prop_0_promoter * 100), "\n")
      cat("  DETRACTOR - SLA=1:", sprintf("%.2f%%", rf$promoter_ranges$Resolved.in.SLA$prop_1_detractor * 100), 
          ", SLA=0:", sprintf("%.2f%%", rf$promoter_ranges$Resolved.in.SLA$prop_0_detractor * 100), "\n")
      cat("  ¿Es relevante?:", rf$promoter_ranges$Resolved.in.SLA$is_relevant, "\n")
      cat("\nImportancia de variables (escalada, referencia del modelo):\n")
      print(round(rf$importance, 2))
    }
  })
  
  # Prediction with user input for Random Forest
  rf_prediction <- eventReactive(input$predict_btn_rf, {
    rf <- rf_model_data()
    if (is.null(rf)) return("No hay modelo disponible para predecir. Presione 'Ejecutar' con filtros válidos.")
    
    new_data <- data.frame(
      Resolution.Groups = as.numeric(input$pred_groups_rf),
      Resolution.Time.Days = as.double(input$pred_time_rf),
      Resolved.in.SLA = as.numeric(input$pred_sla_rf)
    )
    
    pred <- predict(rf$model, new_data, type = "raw")
    prob <- predict(rf$model, new_data, type = "prob")
    
    cat(
      " Predicción:", as.character(pred), "\n",
      "Probabilidad PROMOTER:", sprintf("%.2f%%", prob[,"PROMOTER"] * 100), "\n",
      "Probabilidad DETRACTOR:", sprintf("%.2f%%", prob[,"DETRACTOR"] * 100)
    )
  })
  
  # Render prediction output for Random Forest
  output$rf_prediction <- renderPrint({
    rf_prediction()
  })
  
  # Decision Tree
  dt_model_data <- eventReactive(input$run_btn, {
    df <- processed_data()
    
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    dt_data <- df %>%
      filter(CLASSIFICATION %in% c("PROMOTER", "DETRACTOR")) %>%
      select(CLASSIFICATION, Resolution.Groups, Resolution.Time.Days, Resolved.in.SLA) %>%
      na.omit()
    
    if (nrow(dt_data) < 10) return(NULL)
    
    unique_sla <- unique(dt_data$Resolved.in.SLA)
    if (length(unique_sla) != 2) {
      message("Error: Resolved.in.SLA no es una variable binaria. Tiene ", length(unique_sla), " valores únicos.")
      return(NULL)
    }
    
    dt_data$CLASSIFICATION <- as.factor(dt_data$CLASSIFICATION)
    dt_data$Resolution.Groups <- as.numeric(dt_data$Resolution.Groups)
    dt_data$Resolution.Time.Days <- as.numeric(dt_data$Resolution.Time.Days)
    dt_data$Resolved.in.SLA <- factor(dt_data$Resolved.in.SLA, levels = c(0, 1))
    
    set.seed(123)
    balanced_data <- upSample(dt_data[, -which(names(dt_data) == "CLASSIFICATION")], dt_data$CLASSIFICATION, 
                              list = FALSE, yname = "CLASSIFICATION")
    
    train_control <- trainControl(method = "cv", number = 5, 
                                  classProbs = TRUE, 
                                  summaryFunction = twoClassSummary, 
                                  savePredictions = TRUE)
    
    tune_grid <- expand.grid(cp = seq(0.01, 0.5, by = 0.05))
    
    dt_model_cv <- tryCatch({
      train(CLASSIFICATION ~ Resolution.Groups + Resolution.Time.Days + Resolved.in.SLA,
            data = balanced_data,
            method = "rpart",
            trControl = train_control,
            tuneGrid = tune_grid,
            metric = "ROC")
    }, error = function(e) {
      message("Error en el entrenamiento del modelo DT: ", e$message)
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
    
    list(model = dt_model_cv, data = balanced_data, accuracy = accuracy, 
         sensitivity = sensitivity, specificity = specificity, auc = auc, 
         is_good_model = is_good_model, cm_table = cm_table, optimal_cp = optimal_cp)
  })
  
  # Render Decision Tree summary
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
  
  # Prediction with user input for Decision Tree
  dt_prediction <- eventReactive(input$predict_btn_dt, {
    dt <- dt_model_data()
    if (is.null(dt)) return("No hay modelo disponible para predecir. Presione 'Ejecutar' con filtros válidos.")
    
    new_data <- data.frame(
      Resolution.Groups = as.numeric(input$pred_groups_dt),
      Resolution.Time.Days = as.double(input$pred_time_dt),
      Resolved.in.SLA = factor(input$pred_sla_dt, levels = c(0, 1))
    )
    
    pred <- predict(dt$model, new_data, type = "raw")
    prob <- predict(dt$model, new_data, type = "prob")
    
    cat(
      " Predicción:", as.character(pred), "\n",
      "Probabilidad PROMOTER:", sprintf("%.2f%%", prob[,"PROMOTER"] * 100), "\n",
      "Probabilidad DETRACTOR:", sprintf("%.2f%%", prob[,"DETRACTOR"] * 100)
    )
  })
  
  # Render prediction output for Decision Tree
  output$dt_prediction <- renderPrint({
    dt_prediction()
  })
  
  # Download handler for Decision Tree plot
  output$download_dt_plot <- downloadHandler(
    filename = function() {
      paste("decision_tree_cv5_", Sys.time(), ".png", sep = "")
    },
    content = function(file) {
      dt <- dt_model_data()
      group_name <- ifelse(input$group == "All", "Todos", input$group)
      issue_name <- ifelse(input$issue == "All", "Todos", input$issue)
      
      if (is.null(dt)) {
        png(file, width = 1200, height = 900, res = 100)
        plot.new()
        text(0.5, 0.5, "No hay datos suficientes para graficar el árbol", cex = 1.5)
        dev.off()
      } else {
        png(file, width = 1200, height = 900, res = 100)
        title_text <- paste("Árbol de Decisión (CV=5, Optimal cp)\n",
                            "CSAT Rated Group:", group_name, "\n",
                            "Issue Classification:", issue_name)
        rpart.plot(dt$model$finalModel, 
                   main = title_text,
                   box.palette = "RdYlGn", 
                   shadow.col = "gray", 
                   nn = TRUE,
                   extra = 104,
                   fallen.leaves = TRUE,
                   type = 4,
                   tweak = 1.2)
        dev.off()
      }
    }
  )
}

# Run the app
shinyApp(ui, server)