####################################################################################################
# ECOTRACKER - GESTOR DE RESIDUOS v. 1.0.0
# By JDR Analytics
####################################################################################################

#====================================================================================================
# Al inicio de tu app.R, después de las librerías
onStop(function() {
  message("Cerrando la aplicación Shiny y liberando recursos...")
  dbDisconnect(con)
  
  # Forzar el cierre de cualquier conexión pendiente
  if (exists("con") && dbIsValid(con)) {
    dbDisconnect(con)
  }
  
  # Forzar el cierre de la aplicación
  stopApp()
})
#====================================================================================================


library(shiny)
library(shinydashboard)
library(DBI)
library(RSQLite)
library(DT)
library(plotly)
library(viridis)
library(shinyjs) 

#install.packages(c("shiny", "shinydashboard", "DBI", "RSQLite", "DT", "plotly", "viridis", "shinyjs"), dependencies = TRUE)

# Definición de operador %||% para valores por defecto
`%||%` <- function(a, b) if (!is.null(a)) a else b

# Establecer conexión con la base de datos
con <- dbConnect(RSQLite::SQLite(), dbname = "ecogestor.db")

# Creación de tablas en SQLite si no existen
dbExecute(con, "CREATE TABLE IF NOT EXISTS Ciudades (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Nombre TEXT NOT NULL UNIQUE,
  CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);")
dbExecute(con, "CREATE TABLE IF NOT EXISTS Establecimientos (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Nombre TEXT NOT NULL UNIQUE,
  CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);")
dbExecute(con, "CREATE TABLE IF NOT EXISTS ProcesosGeneradores (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Nombre TEXT NOT NULL UNIQUE,
  CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);")
dbExecute(con, "CREATE TABLE IF NOT EXISTS Residuos (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Residuo TEXT NOT NULL UNIQUE,
  Descripcion TEXT,
  TipoResiduo TEXT,
  GradoPeligrosidad TEXT,
  EstadoMateria TEXT,
  CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);")
dbExecute(con, "CREATE TABLE IF NOT EXISTS Presentaciones (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Nombre TEXT NOT NULL UNIQUE,
  CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);")
dbExecute(con, "CREATE TABLE IF NOT EXISTS Gestores (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Nombre TEXT NOT NULL UNIQUE,
  CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);")
dbExecute(con, "CREATE TABLE IF NOT EXISTS MecanismosEntrega (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Nombre TEXT NOT NULL UNIQUE,
  CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);")
dbExecute(con, "CREATE TABLE IF NOT EXISTS Registros (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Codigo TEXT NOT NULL UNIQUE,
  FechaRegistro DATE,
  CiudadID INTEGER,
  EstablecimientoID INTEGER,
  ProcesoGeneradorID INTEGER,
  ResiduoID INTEGER,
  Descripcion TEXT,
  Cantidad REAL,
  PresentacionID INTEGER,
  GestorID INTEGER,
  MecanismoID INTEGER,
  Certificado TEXT DEFAULT 'Pendiente',
  CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (CiudadID) REFERENCES Ciudades(ID),
  FOREIGN KEY (EstablecimientoID) REFERENCES Establecimientos(ID),
  FOREIGN KEY (ProcesoGeneradorID) REFERENCES ProcesosGeneradores(ID),
  FOREIGN KEY (ResiduoID) REFERENCES Residuos(ID),
  FOREIGN KEY (PresentacionID) REFERENCES Presentaciones(ID),
  FOREIGN KEY (GestorID) REFERENCES Gestores(ID),
  FOREIGN KEY (MecanismoID) REFERENCES MecanismosEntrega(ID)
);")
dbExecute(con, "CREATE TABLE IF NOT EXISTS RespaldoRegistros (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Codigo TEXT NOT NULL,
  FechaRegistro DATE,
  CiudadID INTEGER,
  EstablecimientoID INTEGER,
  ProcesoGeneradorID INTEGER,
  ResiduoID INTEGER,
  Descripcion TEXT,
  Cantidad REAL,
  PresentacionID INTEGER,
  GestorID INTEGER,
  MecanismoID INTEGER,
  Certificado TEXT,
  CreatedAt DATETIME,
  UpdatedAt DATETIME,
  DeletedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);")

# Inserción de datos de ejemplo
dbExecute(con, "INSERT OR IGNORE INTO Ciudades (Nombre, CreatedAt, UpdatedAt) VALUES 
  ('Bogotá', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), 
  ('Cali', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), 
  ('Medellín', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), 
  ('Pereira', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);")

dbExecute(con, "INSERT OR IGNORE INTO Establecimientos (Nombre, CreatedAt, UpdatedAt) VALUES 
  ('Planta', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), 
  ('Oficinas', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);")

dbExecute(con, "INSERT OR IGNORE INTO Residuos (Residuo, Descripcion, TipoResiduo, GradoPeligrosidad, EstadoMateria, CreatedAt, UpdatedAt) VALUES 
  ('Cartón', 'Material de empaque', 'Residuo No Peligroso Aprovechable', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Papel', 'Material de empaques y productos', 'Residuo No Peligroso Aprovechable', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Madera', 'Estibas', 'Residuo No Peligroso Aprovechable', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Aserrín', 'Material de contigencia', 'Residuo No Peligroso Aprovechable', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Chatarra', 'Piezas y equipos obsoletos', 'Residuo No Peligroso Aprovechable', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Aparatos eléctricos y electrónicos', 'Equipos obsoletos', 'Residuo Peligroso', 'Ecotóxico', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Aceites y grasas', 'Mantenimiento', 'Residuo Peligroso', 'Ecotóxico', 'Líquido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Escombros', 'Mantenimiento y construcción', 'Residuo Especial', 'No aplica', 'Líquido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Llantas usadas', 'de vehículos de uso interno', 'Residuo Especial', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Cortes y poda de materiales vegetales', 'Mantenimiento General', 'Residuo No Peligroso Orgánico degradable', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Plástico', 'Bolsas, garrafas, envases y tapas', 'Residuo No Peligroso No aprovechable', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Empaques', 'Material de empaques contaminados', 'Residuo Peligroso', 'Toxicidad', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);")

dbExecute(con, "INSERT OR IGNORE INTO ProcesosGeneradores (Nombre, CreatedAt, UpdatedAt) VALUES 
  ('Almacenamiento de insumo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Almacenamiento de producto', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Control de calidad', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Descarga del producto', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Emergencia', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Mantenimiento', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Otros', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Proceso productivo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Servicios auxiliares', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Subproducto', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Transporte de insumo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Transporte del producto', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Producción de producto', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);")

dbExecute(con, "INSERT OR IGNORE INTO Presentaciones (Nombre, CreatedAt, UpdatedAt) VALUES 
  ('Tanque de 55 gal', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Caneca', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Saco o costal', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('A granel bajo techo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('A granel a la intemperie', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('En tolva', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Contenedor metálico', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Contenedor plástico', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Bolsa plástica', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Embalaje cartón', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Otro', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);")

dbExecute(con, "INSERT OR IGNORE INTO Gestores (Nombre, CreatedAt, UpdatedAt) VALUES 
  ('Recicladores', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Programa postconsumo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Empresa Especializada', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Aseo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);")

dbExecute(con, "INSERT OR IGNORE INTO MecanismosEntrega (Nombre, CreatedAt, UpdatedAt) VALUES 
  ('Punto de recolección', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Centro de acopio', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Campañas de recolección', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Recolección directa en instalaciones del generador', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Entrega a centro de acopio', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Ruta selectiva programada', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Entrega directa en planta del gestor', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Devolución vía correo postal o mensajería certificada', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Otro', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);")

# Agregar columna Codigo si no existe
if (!"Codigo" %in% dbGetQuery(con, "PRAGMA table_info(Registros)")$name) {
  dbExecute(con, "ALTER TABLE Registros ADD COLUMN Codigo TEXT NOT NULL DEFAULT ''")
}

# Función para generar código GR-0000000X
generate_codigo <- function() {
  last_id <- dbGetQuery(con, "SELECT MAX(ID) as max_id FROM Registros")$max_id
  if (is.na(last_id)) last_id <- 0
  sprintf("GR-%08d", last_id + 1)
}

# Interfaz de usuario con barra lateral
ui <- dashboardPage(

  title = "Ecotracker - Gestor de Residuos",  
  dashboardHeader(

    title = tags$div(
      style = "font-size: 14px; font-weight: 600; color: #E6F0FA;",
      "Ecotracker - Gestor de Residuos ",
      tags$span(style = "font-size: 12px;", "v1.0.0"),
      tags$br(),
      tags$span(style = "font-size: 8px; font-weight: 400;", "By JDR Analytics")
    ),
    titleWidth = 300
  ),
  dashboardSidebar(
    width = 300,
    sidebarMenu(
      id = "sidebar_menu",
      menuItem("Nuevo Registro", tabName = "new_record", icon = icon("plus-circle")),
      menuItem("Visualizar/Editar", tabName = "view_edit", icon = icon("edit")),
      menuItem("Dimensiones Únicas", tabName = "unique_dims", icon = icon("list")),
      menuItem("Dimensiones de Residuos", tabName = "waste_dims", icon = icon("trash")),
      menuItem("Tablero", tabName = "dashboard", icon = icon("chart-bar"), selected = TRUE)
    ),
    tags$style(HTML("
      .sidebar-menu li a { color: #E6F0FA; font-size: 16px; transition: all 0.3s ease; }
      .sidebar-menu li a:hover { background-color: #2E6A9A; color: #1E425A; }
      .sidebar-menu .treeview-menu { background-color: #2A4F6A; }
      .skin-blue .sidebar-menu > li.active > a { background-color: #6BA8D1; color: #1E425A; }
      .dateRangeInput input { background-color: #345C7D !important; color: #E6F0FA !important; border: 1px solid #2E6A9A !important; border-radius: 5px !important; }
    "))
  ),
  dashboardBody(
    useShinyjs(),
    tags$head(
      tags$style(HTML("
        body, .content-wrapper { background-color: #1E425A; color: #E6F0FA; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .skin-blue .main-header .navbar { background-color: #2A4F6A; }
        .skin-blue .main-header .logo { background-color: #2A4F6A; font-weight: 600; font-size: 18px; }
        .box { background-color: #2A4F6A; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-top: 3px solid #2E6A9A; }
        .box-header { color: #E6F0FA; font-weight: 500; }
        .box-body { padding: 20px; }
        .btn { background-color: #2E6A9A; color: #FFFFFF; border: none; padding: 8px 16px; border-radius: 5px; transition: all 0.3s ease; }
        .btn:hover { background-color: #225A8A; }
        .form-control, select, textarea { background-color: #345C7D; color: #E6F0FA; border: 1px solid #2E6A9A; border-radius: 5px; }
        .form-control:focus { border-color: #6BA8D1; box-shadow: 0 0 0 0.2rem rgba(107, 168, 209, 0.25); }
        .dataTable { background-color: #2A4F6A; color: #E6F0FA; border: 1px solid #225A8A; border-radius: 5px; }
        .dataTables_wrapper .dataTables_length, .dataTables_wrapper .dataFilters_filter, .dataTables_wrapper .dataTables_info, .dataTables_wrapper .dataTables_paginate { color: #E6F0FA; }
        .dataTables_wrapper .dataTables_filter input { background-color: #345C7D; color: #E6F0FA; border: 1px solid #2E6A9A; }
        .dataTables_wrapper .dataTables_paginate .paginate_button { background-color: #2A4F6A; color: #E6F0FA; border: 1px solid #225A8A; }
        .dataTables_wrapper .dataTables_paginate .paginate_button:hover { background-color: #2E6A9A; }
        .dataTables_wrapper .dataTables_paginate .paginate_button.current { background-color: #6BA8D1; }
        .dataTable td {font-size: 12px !important;}
        .value-box { background-color: #2A4F6A; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .value-box .value { font-size: 24px; font-weight: 600; }
        .kpi-box { padding: 15px; border-radius: 8px; text-align: center; color: #FFFFFF; margin-bottom: 20px; }
        .kpi-box h4 { margin-top: 0; font-size: 1.1em; opacity: 0.8; }
        .kpi-box h2 { margin-bottom: 0; font-size: 2.2em; font-weight: bold; }
        .datepicker table { background-color: #2A4F6A; }
        .datepicker table tr td.day { color: #E6F0FA; }
        .datepicker table tr td.day:hover { background-color: #2E6A9A; }
        .datepicker table tr td.active { background-color: #6BA8D1 !important; }
        .modal-dialog { width: 80%; max-width: 1200px; }
        .modal-content { background-color: #345C7D; color: #E6F0FA; }
        .modal-title { color: #E6F0FA !important; }
        .form-row-spaced { margin-bottom: 25px; }
        .dt-selected-row { background-color: #6BA8D1 !important; color: #1E425A !important; }
        .modal-content label, .modal-content .control-label { color: #E6F0FA !important; }
        #guardar_progress .progress { height: 20px; background: #1F3F5A; }
        #guardar_progress .progress-bar { background: #2E6A9A; }
        #edit_progress .progress { height: 20px; background: #1F3F5A; }
        #edit_progress .progress-bar { background: #2E6A9A; }
        #descripcion, #tipo_residuo, #grado_peligro, #estado_materia,
        #edit_descripcion, #edit_tipo_residuo, #edit_grado_peligro, #edit_estado_materia {
          background-color: #345C7D;
          color: #E6F0FA !important;
          border: 1px solid #2E6A9A;
          border-radius: 5px;
        }
        #guardar, #editar, #refresh_registros, #agregar, #borrar_dimension, #refresh_dimensiones,
        #agregar_residuo, #editar_residuo, #refresh_residuos, #guardar_cambios,
        #borrar_registro_modal, #cerrar_modal, #guardar_cambios_residuo,
        #borrar_residuo_modal, #cerrar_modal_residuo {
          background-color: #2E6A9A;
          color: #FFFFFF;
          cursor: pointer;
        }
        #editar.enabled, #refresh_registros.enabled, #agregar.enabled, #borrar_dimension.enabled,
        #refresh_dimensiones.enabled, #agregar_residuo.enabled, #editar_residuo.enabled,
        #refresh_residuos.enabled, #guardar_cambios.enabled, #borrar_registro_modal.enabled,
        #cerrar_modal.enabled, #guardar_cambios_residuo.enabled, #borrar_residuo_modal.enabled,
        #cerrar_modal_residuo.enabled {
          background-color: #2E6A9A !important;
          cursor: pointer !important;
        }
      "))
    ),
    tabItems(
      tabItem(
        tabName = "new_record",
        fluidRow(
          column(12, htmlOutput("contador_registros"))
        ),
        fluidRow(class = "form-row-spaced",
          column(4, dateInput("fecha", "Fecha del Registro", value = Sys.Date(), language = "es")),
          column(4, selectInput("ciudad", "Ciudad", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]))),
          column(4, selectInput("establecimiento", "Establecimiento", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]])))
        ),
        fluidRow(class = "form-row-spaced",
          column(4, selectInput("proceso", "Proceso Generador", choices = c("", dbGetQuery(con, "SELECT Nombre FROM ProcesosGeneradores")[[1]]))),
          column(4, selectInput("residuo", "Residuo", choices = c("", dbGetQuery(con, "SELECT Residuo FROM Residuos")[[1]]))),
          column(4, textInput("descripcion_registro", "Descripción"))
        ),
        fluidRow(class = "form-row-spaced",
          column(4,
            tags$label("Descripción del Residuo", style="color:#E6F0FA;"),
            uiOutput("descripcion_ui")
          ),
          column(4,
            tags$label("Tipo de Residuo", style="color:#E6F0FA;"),
            uiOutput("tipo_residuo_ui")
          ),
          column(4,
            tags$label("Nivel de Peligro", style="color:#E6F0FA;"),
            uiOutput("grado_peligro_ui")
          )
        ),
        fluidRow(class = "form-row-spaced",
          column(4,
            tags$label("Estado de la Materia", style="color:#E6F0FA;"),
            uiOutput("estado_materia_ui")
          ),
          column(4, numericInput("cantidad", "Cantidad (Kg)", value = 0, min = 0, step = 0.1)),
          column(4, selectInput("presentacion", "Presentación", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Presentaciones")[[1]])))
        ),
        fluidRow(class = "form-row-spaced",
          column(4, selectInput("gestor", "Gestor", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Gestores")[[1]]))),
          column(4, selectInput("mecanismo", "Mecanismo de Entrega", choices = c("", dbGetQuery(con, "SELECT Nombre FROM MecanismosEntrega")[[1]]))),
          column(4, selectInput("certificado", "Certificado", choices = c("Pendiente", "Recibido", "No Aplicable"), selected = "Pendiente"))
        ),
        fluidRow(
          column(12,
            div(style = "display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px;",
              actionButton("guardar", "Guardar"),
              actionButton("limpiar", "Limpiar"),
              br(), br(),
              uiOutput("guardar_progress"),
              uiOutput("guardar_msg")
            )
          )
        )
      ),
      tabItem(
        tabName = "view_edit",
        fluidRow(
          column(12, DTOutput("registros"))
        ),
        fluidRow(
          column(12,
            div(style = "display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px;",
              actionButton("editar", "Editar Seleccionado"),
              actionButton("refresh_registros", "Refrescar")
            )
          )
        )
      ),
      tabItem(
        tabName = "unique_dims",
        fluidRow(
          column(12, DTOutput("dimensiones"))
        ),
        fluidRow(
          column(4, selectInput("dimension", "Dimensión", choices = c("Ciudades", "Establecimientos", "ProcesosGeneradores", "Presentaciones", "Gestores", "MecanismosEntrega"))),
          column(4, textInput("nuevo_valor", "Nuevo Valor"))
        ),
        fluidRow(
          column(12,
            div(style = "display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px;",
              actionButton("agregar", "Agregar"),
              actionButton("borrar_dimension", "Eliminar Seleccionado"),
              actionButton("refresh_dimensiones", "Refrescar")
            )
          )
        )
      ),
      tabItem(
        tabName = "waste_dims",
        fluidRow(
          column(12, DTOutput("dimensiones_residuos"))
        ),
        fluidRow(
          column(3, textInput("nuevo_residuo", "Residuo")),
          column(3, textInput("nueva_descripcion", "Descripción")),
          column(3, selectInput("nuevo_tipo_residuo", "Tipo de Residuo", choices = c("", "Residuo No Peligroso Aprovechable", "Residuo No Peligroso No aprovechable", "Residuo No Peligroso Orgánico degradable", "Residuo Peligroso", "Residuo Especial"))),
          column(3, selectInput("nuevo_grado_peligrosidad", "Nivel de Peligro", choices = c("", "No aplica", "Ecotóxico", "Toxicidad")))
        ),
        fluidRow(
          column(3, selectInput("nuevo_estado_materia", "Estado de la Materia", choices = c("", "Sólido o semisólido", "Líquido"))),
          column(9,
            div(style = "display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px;",
              actionButton("agregar_residuo", "Agregar"),
              actionButton("editar_residuo", "Editar Seleccionado"),
              actionButton("refresh_residuos", "Refrescar")
            )
          )
        )
      ),
      tabItem(
        tabName = "dashboard",
        fluidRow(
          box(
            width = 12,
            title = "Filtros",
            status = "primary",
            solidHeader = TRUE,
            collapsible = FALSE,
            div(
              style = "display: flex; flex-wrap: wrap; gap: 15px; padding: 15px;",
              div(
                style = "flex: 1; min-width: 200px;",
                dateRangeInput("dashboard_fecha", "Rango de Fechas:", start = Sys.Date() - 30, end = Sys.Date(), language = "es")
              ),
              div(
                style = "flex: 1; min-width: 200px;",
                selectInput("dashboard_ciudad", "Ciudad:", choices = c("Todos", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]), selected = "Todos")
              ),
              div(
                style = "flex: 1; min-width: 200px;",
                selectInput("dashboard_establecimiento", "Establecimiento:", choices = c("Todos", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]]), selected = "Todos")
              ),
              div(
                style = "flex: 1; min-width: 200px;",
                selectInput("dashboard_tipo_residuo", "Tipo de Residuo:", choices = c("Todos", dbGetQuery(con, "SELECT DISTINCT TipoResiduo FROM Residuos")[[1]]), selected = "Todos")
              )
            )
          )
        ),
        fluidRow(
          column(4, uiOutput("kpi_registros"), class = "kpi-box"),
          column(4, uiOutput("kpi_certificados_recibidos"), class = "kpi-box"),
          column(4, uiOutput("kpi_certificados_pendientes"), class = "kpi-box")
        ),
        fluidRow(
          column(4, uiOutput("kpi_toneladas"), class = "kpi-box"),
          column(4, uiOutput("kpi_peligrosos"), class = "kpi-box"),
          column(4, uiOutput("kpi_no_peligrosos"), class = "kpi-box")
        ),
        fluidRow(
          column(12, 
            uiOutput("kpi_especiales"), class = "kpi-box"
          )
        ),
        br(),
        fluidRow(
          column(12, 
            plotlyOutput("dashboard_tendencia", height = "40vh"),
            uiOutput("tendencia_empty")
          )
        ),
        br(),
        fluidRow(
          column(12, 
            plotlyOutput("dashboard_doughnut_ciudad", height = "40vh"),
            uiOutput("doughnut_ciudad_empty")
          )),
        br(),
        fluidRow(
          column(12, 
            plotlyOutput("dashboard_stacked_establecimiento", height = "40vh"),
            uiOutput("stacked_establecimiento_empty")
          )
        ),
        br(),
        fluidRow(
          column(12, 
            plotlyOutput("dashboard_pareto", height = "60vh"),
            uiOutput("pareto_empty")
          )),
        br(),
        fluidRow(
          column(12, 
            plotlyOutput("dashboard_bar_tipo", height = "40vh"),
            uiOutput("bar_tipo_empty")
          )
        )
      )
    )
  )
)

server <- function(input, output, session) {
  # Actualización de Dropdowns al iniciar
  observe({
    updateSelectInput(session, "ciudad", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]))
    updateSelectInput(session, "establecimiento", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]]))
    updateSelectInput(session, "proceso", choices = c("", dbGetQuery(con, "SELECT Nombre FROM ProcesosGeneradores")[[1]]))
    updateSelectInput(session, "residuo", choices = c("", dbGetQuery(con, "SELECT Residuo FROM Residuos")[[1]]))
    updateSelectInput(session, "presentacion", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Presentaciones")[[1]]))
    updateSelectInput(session, "gestor", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Gestores")[[1]]))
    updateSelectInput(session, "mecanismo", choices = c("", dbGetQuery(con, "SELECT Nombre FROM MecanismosEntrega")[[1]]))
    updateSelectInput(session, "dashboard_ciudad", choices = c("Todos", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]))
    updateSelectInput(session, "dashboard_establecimiento", choices = c("Todos", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]]))
    updateSelectInput(session, "dashboard_tipo_residuo", choices = c("Todos", dbGetQuery(con, "SELECT DISTINCT TipoResiduo FROM Residuos")[[1]]))
  })

  # Campos dinámicos para nuevo registro
  output$descripcion_ui <- renderUI({
    if (input$residuo != "") {
      residuo_data <- dbGetQuery(con, sprintf("SELECT Descripcion FROM Residuos WHERE Residuo = '%s'", input$residuo))
      if (nrow(residuo_data) > 0) {
        tags$input(type = "text", id = "descripcion", value = residuo_data$Descripcion, disabled = "disabled", class = "form-control")
      } else {
        tags$input(type = "text", id = "descripcion", value = "", class = "form-control")
      }
    } else {
      tags$input(type = "text", id = "descripcion", value = "", class = "form-control")
    }
  })

  output$tipo_residuo_ui <- renderUI({
    if (input$residuo != "") {
      residuo_data <- dbGetQuery(con, sprintf("SELECT TipoResiduo FROM Residuos WHERE Residuo = '%s'", input$residuo))
      if (nrow(residuo_data) > 0) {
        tags$input(type = "text", id = "tipo_residuo", value = residuo_data$TipoResiduo, disabled = "disabled", class = "form-control")
      } else {
        tags$input(type = "text", id = "tipo_residuo", value = "", class = "form-control")
      }
    } else {
      tags$input(type = "text", id = "tipo_residuo", value = "", class = "form-control")
    }
  })

  output$grado_peligro_ui <- renderUI({
    if (input$residuo != "") {
      residuo_data <- dbGetQuery(con, sprintf("SELECT GradoPeligrosidad FROM Residuos WHERE Residuo = '%s'", input$residuo))
      if (nrow(residuo_data) > 0) {
        tags$input(type = "text", id = "grado_peligro", value = residuo_data$GradoPeligrosidad, disabled = "disabled", class = "form-control")
      } else {
        tags$input(type = "text", id = "grado_peligro", value = "", class = "form-control")
      }
    } else {
      tags$input(type = "text", id = "grado_peligro", value = "", class = "form-control")
    }
  })

  output$estado_materia_ui <- renderUI({
    if (input$residuo != "") {
      residuo_data <- dbGetQuery(con, sprintf("SELECT EstadoMateria FROM Residuos WHERE Residuo = '%s'", input$residuo))
      if (nrow(residuo_data) > 0) {
        tags$input(type = "text", id = "estado_materia", value = residuo_data$EstadoMateria, disabled = "disabled", class = "form-control")
      } else {
        tags$input(type = "text", id = "estado_materia", value = "", class = "form-control")
      }
    } else {
      tags$input(type = "text", id = "estado_materia", value = "", class = "form-control")
    }
  })

  # Validación y habilitación del botón Guardar
observe({
  # Asegurar que los inputs se evalúen solo cuando estén disponibles
  req(input$fecha, input$ciudad, input$establecimiento, input$proceso, input$residuo,
      input$descripcion_registro, input$cantidad, input$presentacion, input$gestor,
      input$mecanismo, input$certificado)
  
  required_fields <- list(
    fecha = as.character(input$fecha),  # Convertir fecha a string para validar
    ciudad = input$ciudad,
    establecimiento = input$establecimiento,
    proceso = input$proceso,
    residuo = input$residuo,
    descripcion_registro = input$descripcion_registro,
    cantidad = input$cantidad,
    presentacion = input$presentacion,
    gestor = input$gestor,
    mecanismo = input$mecanismo,
    certificado = input$certificado
  )
  
  all_filled <- all(sapply(required_fields, function(x) {
    !is.null(x) && 
    (is.character(x) && x != "") || 
    (is.numeric(x) && x > 0)
  }))
  
  shinyjs::toggleState("guardar", condition = all_filled)
  shinyjs::toggleClass("guardar", "enabled", condition = all_filled)
})

  # Validación y habilitación de botones en modales
  observe({
    required_edit_fields <- list(
      ciudad = input$edit_ciudad,
      establecimiento = input$edit_establecimiento,
      proceso = input$edit_proceso,
      residuo = input$edit_residuo,
      descripcion = input$edit_descripcion_registro,
      cantidad = input$edit_cantidad,
      presentacion = input$edit_presentacion,
      gestor = input$edit_gestor,
      mecanismo = input$edit_mecanismo,
      certificado = input$edit_certificado
    )
    all_edit_filled <- all(sapply(required_edit_fields, function(x) {
      !is.null(x) && x != "" && (!is.numeric(x) || (is.numeric(x) && x > 0))
    }))
    shinyjs::toggleState("guardar_cambios", condition = all_edit_filled)
    shinyjs::toggleClass("guardar_cambios", "enabled", condition = all_edit_filled)
    shinyjs::toggleState("borrar_registro_modal", condition = TRUE)
    shinyjs::toggleClass("borrar_registro_modal", "enabled", condition = TRUE)
    shinyjs::toggleState("cerrar_modal", condition = TRUE)
    shinyjs::toggleClass("cerrar_modal", "enabled", condition = TRUE)
  })

  observe({
    required_residuo_fields <- list(
      residuo = input$edit_residuo_nombre,
      tipo = input$edit_residuo_tipo,
      grado = input$edit_residuo_grado,
      estado = input$edit_residuo_estado
    )
    all_residuo_filled <- all(sapply(required_residuo_fields, function(x) {
      !is.null(x) && x != ""
    }))
    shinyjs::toggleState("guardar_cambios_residuo", condition = all_residuo_filled)
    shinyjs::toggleClass("guardar_cambios_residuo", "enabled", condition = all_residuo_filled)
    shinyjs::toggleState("borrar_residuo_modal", condition = TRUE)
    shinyjs::toggleClass("borrar_residuo_modal", "enabled", condition = TRUE)
    shinyjs::toggleState("cerrar_modal_residuo", condition = TRUE)
    shinyjs::toggleClass("cerrar_modal_residuo", "enabled", condition = TRUE)
  })

  observe({
    shinyjs::toggleState("editar", condition = length(input$registros_rows_selected) > 0)
    shinyjs::toggleClass("editar", "enabled", condition = length(input$registros_rows_selected) > 0)
    shinyjs::toggleState("refresh_registros", condition = TRUE)
    shinyjs::toggleClass("refresh_registros", "enabled", condition = TRUE)
    shinyjs::toggleState("agregar", condition = input$nuevo_valor != "")
    shinyjs::toggleClass("agregar", "enabled", condition = input$nuevo_valor != "")
    shinyjs::toggleState("borrar_dimension", condition = length(input$dimensiones_rows_selected) > 0)
    shinyjs::toggleClass("borrar_dimension", "enabled", condition = length(input$dimensiones_rows_selected) > 0)
    shinyjs::toggleState("refresh_dimensiones", condition = TRUE)
    shinyjs::toggleClass("refresh_dimensiones", "enabled", condition = TRUE)
    shinyjs::toggleState("agregar_residuo", condition = input$nuevo_residuo != "" && input$nuevo_tipo_residuo != "" && input$nuevo_grado_peligrosidad != "" && input$nuevo_estado_materia != "")
    shinyjs::toggleClass("agregar_residuo", "enabled", condition = input$nuevo_residuo != "" && input$nuevo_tipo_residuo != "" && input$nuevo_grado_peligrosidad != "" && input$nuevo_estado_materia != "")
    shinyjs::toggleState("editar_residuo", condition = length(input$dimensiones_residuos_rows_selected) > 0)
    shinyjs::toggleClass("editar_residuo", "enabled", condition = length(input$dimensiones_residuos_rows_selected) > 0)
    shinyjs::toggleState("refresh_residuos", condition = TRUE)
    shinyjs::toggleClass("refresh_residuos", "enabled", condition = TRUE)
  })

  # Guardar nuevo registro
  observeEvent(input$guardar, {
    required_fields <- list(
      fecha = input$fecha,
      ciudad = input$ciudad,
      establecimiento = input$establecimiento,
      proceso = input$proceso,
      residuo = input$residuo,
      descripcion_registro = input$descripcion_registro,
      cantidad = input$cantidad,
      presentacion = input$presentacion,
      gestor = input$gestor,
      mecanismo = input$mecanismo,
      certificado = input$certificado
    )
    empty_fields <- names(required_fields)[sapply(required_fields, function(x) {
      is.null(x) || (is.character(x) && x == "") || (is.numeric(x) && x <= 0)
    })]
    if (length(empty_fields) > 0) {
      showNotification(paste("Por favor complete todos los campos requeridos. Campos vacíos:", paste(empty_fields, collapse = ", ")), type = "error", duration = 5)
      return()
    }

    output$guardar_progress <- renderUI({
      tags$div(style="margin-top:10px;", 
        tags$div(class="progress", style="height: 20px; background: #1F3F5A;",
          tags$div(class="progress-bar progress-bar-striped active", 
                  role="progressbar", style="width:100%; background:#2E6A9A;", "Guardando...")
        )
      )
    })
    
    tryCatch({
      ciudad_id <- dbGetQuery(con, sprintf("SELECT ID FROM Ciudades WHERE Nombre = '%s'", input$ciudad))[[1, 1]]
      if (is.null(ciudad_id)) stop("Ciudad no válida")
      establecimiento_id <- dbGetQuery(con, sprintf("SELECT ID FROM Establecimientos WHERE Nombre = '%s'", input$establecimiento))[[1, 1]]
      if (is.null(establecimiento_id)) stop("Establecimiento no válido")
      proceso_id <- dbGetQuery(con, sprintf("SELECT ID FROM ProcesosGeneradores WHERE Nombre = '%s'", input$proceso))[[1, 1]]
      if (is.null(proceso_id)) stop("Proceso no válido")
      residuo_id <- dbGetQuery(con, sprintf("SELECT ID FROM Residuos WHERE Residuo = '%s'", input$residuo))[[1, 1]]
      if (is.null(residuo_id)) stop("Residuo no válido")
      presentacion_id <- dbGetQuery(con, sprintf("SELECT ID FROM Presentaciones WHERE Nombre = '%s'", input$presentacion))[[1, 1]]
      if (is.null(presentacion_id)) stop("Presentación no válida")
      gestor_id <- dbGetQuery(con, sprintf("SELECT ID FROM Gestores WHERE Nombre = '%s'", input$gestor))[[1, 1]]
      if (is.null(gestor_id)) stop("Gestor no válido")
      mecanismo_id <- dbGetQuery(con, sprintf("SELECT ID FROM MecanismosEntrega WHERE Nombre = '%s'", input$mecanismo))[[1, 1]]
      if (is.null(mecanismo_id)) stop("Mecanismo no válido")

      codigo <- generate_codigo()

      query <- sprintf("INSERT INTO Registros (Codigo, FechaRegistro, CiudadID, EstablecimientoID, ProcesoGeneradorID, ResiduoID, Descripcion, Cantidad, PresentacionID, GestorID, MecanismoID, Certificado, CreatedAt, UpdatedAt) VALUES ('%s', '%s', %d, %d, %d, %d, '%s', %f, %d, %d, %d, '%s', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                      codigo, input$fecha, ciudad_id, establecimiento_id, proceso_id, residuo_id, input$descripcion_registro,
                      input$cantidad, presentacion_id, gestor_id, mecanismo_id, input$certificado)
      dbExecute(con, query)
      Sys.sleep(0.7)
      output$guardar_progress <- renderUI({ NULL })
      showNotification("Datos guardados con éxito!", type = "message", duration = 3)
      output$contador_registros <- renderUI({
        n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM Registros")$n
        HTML(sprintf("<h4>Registros Almacenados: <b>%d</b></h4>", n))
      })
      later::later(function() { output$guardar_msg <- renderUI({ NULL }) }, 2)
      registros_trigger(registros_trigger() + 1)

      updateDateInput(session, "fecha", value = Sys.Date())
      updateSelectInput(session, "ciudad", selected = "")
      updateSelectInput(session, "establecimiento", selected = "")
      updateSelectInput(session, "proceso", selected = "")
      updateSelectInput(session, "residuo", selected = "")
      updateTextInput(session, "descripcion_registro", value = "")
      updateNumericInput(session, "cantidad", value = 0)
      updateSelectInput(session, "presentacion", selected = "")
      updateSelectInput(session, "gestor", selected = "")
      updateSelectInput(session, "mecanismo", selected = "")
      updateSelectInput(session, "certificado", selected = "Pendiente")
    }, error = function(e) {
      output$guardar_progress <- renderUI({ NULL })
      showNotification(sprintf("Error al guardar el registro: %s", e$message), type = "error", duration = 5)
    })
  }, ignoreInit = TRUE)

  # Limpiar campos de registro
  observeEvent(input$limpiar, {
    updateDateInput(session, "fecha", value = Sys.Date())
    updateSelectInput(session, "ciudad", selected = "")
    updateSelectInput(session, "establecimiento", selected = "")
    updateSelectInput(session, "proceso", selected = "")
    updateSelectInput(session, "residuo", selected = "")
    updateTextInput(session, "descripcion_registro", value = "")
    updateNumericInput(session, "cantidad", value = 0)
    updateSelectInput(session, "presentacion", selected = "")
    updateSelectInput(session, "gestor", selected = "")
    updateSelectInput(session, "mecanismo", selected = "")
    updateSelectInput(session, "certificado", selected = "Pendiente")
  })

  # Contador de registros
  output$contador_registros <- renderUI({
    n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM Registros")$n
    HTML(sprintf("<h4>Registros Almacenados: <b>%d</b></h4>", n))
  })

  # Trigger reactivo para refrescar registros
  registros_trigger <- reactiveVal(0)

  observeEvent(input$refresh_registros, {
    registros_trigger(registros_trigger() + 1)
  })
  observeEvent(input$sidebar_menu, {
    if (input$sidebar_menu == "view_edit") registros_trigger(registros_trigger() + 1)
    if (input$sidebar_menu == "waste_dims") residuos_trigger(residuos_trigger() + 1)
  })
  observeEvent(input$guardar_cambios, {
    removeModal()
    registros_trigger(registros_trigger() + 1)
  })
  observeEvent(input$borrar_registro_modal, {
    removeModal()
    registros_trigger(registros_trigger() + 1)
  })
  observeEvent(input$guardar_cambios_residuo, {
    removeModal()
    residuos_trigger(residuos_trigger() + 1)
  })
  observeEvent(input$borrar_residuo_modal, {
    removeModal()
    residuos_trigger(residuos_trigger() + 1)
  })

  # Consulta reactiva de registros
  registros_data <- reactive({
    registros_trigger() 
    dbGetQuery(con, "SELECT r.ID, r.Codigo, r.FechaRegistro, c.Nombre AS Ciudad, e.Nombre AS Establecimiento, pg.Nombre AS Proceso, rs.Residuo, r.Descripcion, r.Cantidad, p.Nombre AS Presentacion, g.Nombre AS Gestor, me.Nombre AS Mecanismo, r.Certificado FROM Registros r LEFT JOIN Ciudades c ON r.CiudadID = c.ID LEFT JOIN Establecimientos e ON r.EstablecimientoID = e.ID LEFT JOIN ProcesosGeneradores pg ON r.ProcesoGeneradorID = pg.ID LEFT JOIN Residuos rs ON r.ResiduoID = rs.ID LEFT JOIN Presentaciones p ON r.PresentacionID = p.ID LEFT JOIN Gestores g ON r.GestorID = g.ID LEFT JOIN MecanismosEntrega me ON r.MecanismoID = me.ID")
  })

  output$registros <- renderDT({
    registros <- registros_data()
    if (nrow(registros) == 0) {
      datatable(data.frame(Mensaje="No hay registros disponibles"), options = list(dom = 't'), rownames = FALSE)
    } else {
      datatable(
        registros,
        options = list(
          pageLength = 10,
          lengthMenu = c(5, 10, 25, 50),
          scrollX = TRUE,
          rowCallback = JS(
            "function(row, data, index) {",
            "  if ($.inArray(index, this.api().rows({selected:true}).indexes().toArray()) !== -1) {",
            "    $(row).addClass('dt-selected-row');",
            "  }",
            "}"
          )
        ),
        class = "display",
        style = "bootstrap",
        selection = "single"
      )
    }
  })

  # Trigger reactivo para dimensiones únicas
  dimensiones_trigger <- reactiveVal(0)
  observeEvent(input$refresh_dimensiones, {
    dimensiones_trigger(dimensiones_trigger() + 1)
  })
  observeEvent(input$sidebar_menu, {
    if (input$sidebar_menu == "unique_dims") dimensiones_trigger(dimensiones_trigger() + 1)
  })
  observeEvent(input$agregar, { dimensiones_trigger(dimensiones_trigger() + 1) })
  observeEvent(input$borrar_dimension, { dimensiones_trigger(dimensiones_trigger() + 1) })

  output$dimensiones <- renderDT({
    dimensiones_trigger() 
    query <- switch(input$dimension,
      "Ciudades" = "SELECT ID, Nombre FROM Ciudades",
      "Establecimientos" = "SELECT ID, Nombre FROM Establecimientos",
      "ProcesosGeneradores" = "SELECT ID, Nombre FROM ProcesosGeneradores",
      "Presentaciones" = "SELECT ID, Nombre FROM Presentaciones",
      "Gestores" = "SELECT ID, Nombre FROM Gestores",
      "MecanismosEntrega" = "SELECT ID, Nombre FROM MecanismosEntrega"
    )
    datos <- dbGetQuery(con, query)
    if (nrow(datos) == 0) {
      datatable(data.frame(Mensaje="No hay dimensiones disponibles"), options = list(dom = 't'), rownames = FALSE)
    } else {
      datatable(
        datos,
        options = list(
          pageLength = 10,
          rowCallback = JS(
            "function(row, data, index) {",
            "  if ($.inArray(index, this.api().rows({selected:true}).indexes().toArray()) !== -1) {",
            "    $(row).addClass('dt-selected-row');",
            "  }",
            "}"
          )
        ),
        class = "display",
        style = "bootstrap",
        selection = "single"
      )
    }
  })

  # Trigger reactivo para residuos
  residuos_trigger <- reactiveVal(0)
  observeEvent(input$refresh_residuos, {
    residuos_trigger(residuos_trigger() + 1)
  })
  observeEvent(input$agregar_residuo, {
    residuos_trigger(residuos_trigger() + 1)
  })

  output$dimensiones_residuos <- renderDT({
    residuos_trigger()
    datos <- dbGetQuery(con, "SELECT ID, Residuo, Descripcion, TipoResiduo, GradoPeligrosidad, EstadoMateria FROM Residuos")
    if (nrow(datos) == 0) {
      datatable(data.frame(Mensaje="No hay residuos disponibles"), options = list(dom = 't'), rownames = FALSE)
    } else {
      datatable(
        datos,
        options = list(
          pageLength = 10,
          scrollX = TRUE,
          rowCallback = JS(
            "function(row, data, index) {",
            "  if ($.inArray(index, this.api().rows({selected:true}).indexes().toArray()) !== -1) {",
            "    $(row).addClass('dt-selected-row');",
            "  }",
            "}"
          )
        ),
        class = "display",
        style = "bootstrap",
        selection = "single"
      )
    }
  })

  # Modal para edición de registros
  edit_residuo_data <- reactive({
    req(input$edit_residuo)
    dbGetQuery(con, sprintf("SELECT * FROM Residuos WHERE Residuo = '%s'", input$edit_residuo))
  })

  output$edit_descripcion_ui <- renderUI({
    data <- edit_residuo_data()
    value <- if (nrow(data) > 0) data$Descripcion else ""
    tags$input(type = "text", id = "edit_descripcion", value = value, disabled = "disabled", class = "form-control")
  })
  output$edit_tipo_residuo_ui <- renderUI({
    data <- edit_residuo_data()
    value <- if (nrow(data) > 0) data$TipoResiduo else ""
    tags$input(type = "text", id = "edit_tipo_residuo", value = value, disabled = "disabled", class = "form-control")
  })
  output$edit_grado_peligro_ui <- renderUI({
    data <- edit_residuo_data()
    value <- if (nrow(data) > 0) data$GradoPeligrosidad else ""
    tags$input(type = "text", id = "edit_grado_peligro", value = value, disabled = "disabled", class = "form-control")
  })
  output$edit_estado_materia_ui <- renderUI({
    data <- edit_residuo_data()
    value <- if (nrow(data) > 0) data$EstadoMateria else ""
    tags$input(type = "text", id = "edit_estado_materia", value = value, disabled = "disabled", class = "form-control")
  })

  observeEvent(input$editar, {
    selected_row <- input$registros_rows_selected
    if (length(selected_row)) {
      registro <- dbGetQuery(con, sprintf("SELECT * FROM Registros WHERE ID = %d", dbGetQuery(con, sprintf("SELECT ID FROM Registros LIMIT 1 OFFSET %d", selected_row - 1))[[1]]))
      showModal(modalDialog(
        title = "Editar Registro",
        fluidRow(class = "form-row-spaced",
          column(4,
            tags$label("Código", style="color:#E6F0FA;"),
            tags$label(registro$Codigo, style="color:#E6F0FA;")
          ),
          column(4,
            tags$label("Fecha del Registro", style="color:#E6F0FA;"),
            tags$label(as.character(as.Date(registro$FechaRegistro)), style="color:#E6F0FA;")
          ),
          column(4, selectInput("edit_ciudad", "Ciudad", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM Ciudades WHERE ID = %d", registro$CiudadID))[[1]]))
        ),
        fluidRow(class = "form-row-spaced",
          column(4, selectInput("edit_establecimiento", "Establecimiento", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM Establecimientos WHERE ID = %d", registro$EstablecimientoID))[[1]])),
          column(4, selectInput("edit_proceso", "Proceso Generador", choices = c("", dbGetQuery(con, "SELECT Nombre FROM ProcesosGeneradores")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM ProcesosGeneradores WHERE ID = %d", registro$ProcesoGeneradorID))[[1]])),
          column(4, selectInput("edit_residuo", "Residuo", choices = c("", dbGetQuery(con, "SELECT Residuo FROM Residuos")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Residuo FROM Residuos WHERE ID = %d", registro$ResiduoID))[[1]]))
        ),
        fluidRow(class = "form-row-spaced",
          column(4, textInput("edit_descripcion_registro", "Descripción", value = registro$Descripcion)),
          column(4,
            tags$label("Descripción del Residuo", style="color:#E6F0FA;"),
            uiOutput("edit_descripcion_ui")
          ),
          column(4,
            tags$label("Tipo de Residuo", style="color:#E6F0FA;"),
            uiOutput("edit_tipo_residuo_ui")
          )
        ),
        fluidRow(class = "form-row-spaced",
          column(4,
            tags$label("Nivel de Peligro", style="color:#E6F0FA;"),
            uiOutput("edit_grado_peligro_ui")
          ),
          column(4,
            tags$label("Estado de la Materia", style="color:#E6F0FA;"),
            uiOutput("edit_estado_materia_ui")
          ),
          column(4, numericInput("edit_cantidad", "Cantidad (Kg)", value = registro$Cantidad, min = 0, step = 0.1))
        ),
        fluidRow(class = "form-row-spaced",
          column(4, selectInput("edit_presentacion", "Presentación", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Presentaciones")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM Presentaciones WHERE ID = %d", registro$PresentacionID))[[1]])),
          column(4, selectInput("edit_gestor", "Gestor", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Gestores")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM Gestores WHERE ID = %d", registro$GestorID))[[1]])),
          column(4, selectInput("edit_mecanismo", "Mecanismo de Entrega", choices = c("", dbGetQuery(con, "SELECT Nombre FROM MecanismosEntrega")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM MecanismosEntrega WHERE ID = %d", registro$MecanismoID))[[1]]))
        ),
        fluidRow(class = "form-row-spaced",
          column(4, selectInput("edit_certificado", "Certificado", choices = c("Pendiente", "Recibido", "No Aplicable"), selected = registro$Certificado)),
          column(8)
        ),
        fluidRow(
          column(12,
            div(style = "display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px;",
              actionButton("guardar_cambios", "Guardar Cambios"),
              actionButton("borrar_registro_modal", "Eliminar Registro"),
              actionButton("cerrar_modal", "Cerrar")
            )
          ),
          uiOutput("edit_progress"),
          uiOutput("edit_msg")
        ),
        footer = NULL,
        size = "l"
      ))
    }
  })

  observeEvent(input$cerrar_modal, {
    shinyjs::runjs("$('#shiny-modal').modal('hide');")
    removeModal()
  })

  # Guardar cambios en registro
  observeEvent(input$guardar_cambios, {
    if (is.null(input$edit_ciudad) || input$edit_ciudad == "" ||
        is.null(input$edit_establecimiento) || input$edit_establecimiento == "" ||
        is.null(input$edit_proceso) || input$edit_proceso == "" ||
        is.null(input$edit_residuo) || input$edit_residuo == "" ||
        is.null(input$edit_presentacion) || input$edit_presentacion == "" ||
        is.null(input$edit_gestor) || input$edit_gestor == "" ||
        is.null(input$edit_mecanismo) || input$edit_mecanismo == "") {
      showNotification("Por favor complete todos los campos requeridos.", type = "error", duration = 5)
      return()
    }

    ciudad_id <- dbGetQuery(con, sprintf("SELECT ID FROM Ciudades WHERE Nombre = '%s'", input$edit_ciudad))[[1]]
    establecimiento_id <- dbGetQuery(con, sprintf("SELECT ID FROM Establecimientos WHERE Nombre = '%s'", input$edit_establecimiento))[[1]]
    proceso_id <- dbGetQuery(con, sprintf("SELECT ID FROM ProcesosGeneradores WHERE Nombre = '%s'", input$edit_proceso))[[1]]
    residuo_id <- dbGetQuery(con, sprintf("SELECT ID FROM Residuos WHERE Residuo = '%s'", input$edit_residuo))[[1]]
    presentacion_id <- dbGetQuery(con, sprintf("SELECT ID FROM Presentaciones WHERE Nombre = '%s'", input$edit_presentacion))[[1]]
    gestor_id <- dbGetQuery(con, sprintf("SELECT ID FROM Gestores WHERE Nombre = '%s'", input$edit_gestor))[[1]]
    mecanismo_id <- dbGetQuery(con, sprintf("SELECT ID FROM MecanismosEntrega WHERE Nombre = '%s'", input$edit_mecanismo))[[1]]

    selected_row <- input$registros_rows_selected
    registro_id <- dbGetQuery(con, sprintf("SELECT ID FROM Registros LIMIT 1 OFFSET %d", selected_row - 1))[[1]]

    query <- sprintf(
      "UPDATE Registros SET CiudadID = %d, EstablecimientoID = %d, ProcesoGeneradorID = %d, ResiduoID = %d, Descripcion = '%s', Cantidad = %f, PresentacionID = %d, GestorID = %d, MecanismoID = %d, Certificado = '%s', UpdatedAt = CURRENT_TIMESTAMP WHERE ID = %d",
      ciudad_id, establecimiento_id, proceso_id, residuo_id, input$edit_descripcion_registro,
      input$edit_cantidad, presentacion_id, gestor_id, mecanismo_id, input$edit_certificado,
      registro_id
    )
    tryCatch({
      dbExecute(con, query)
      output$edit_progress <- renderUI({ NULL })
      output$edit_msg <- renderUI({ NULL })
      showNotification("Registro editado con éxito", type = "message", duration = 3)
      later::later(function() { output$edit_msg <- renderUI({ NULL }) }, 2)
      removeModal()
      registros_trigger(registros_trigger() + 1)
    }, error = function(e) {
      showNotification(sprintf("Error al guardar cambios: %s", e$message), type = "error", duration = 5)
    })
  })

  # Borrar registro con respaldo
  observeEvent(input$borrar_registro_modal, {
    selected_row <- input$registros_rows_selected
    if (length(selected_row)) {
      registro <- dbGetQuery(con, sprintf("SELECT * FROM Registros WHERE ID = %d", dbGetQuery(con, sprintf("SELECT ID FROM Registros LIMIT 1 OFFSET %d", selected_row - 1))[[1]]))
      
      # Insertar en RespaldoRegistros
      backup_query <- sprintf(
        "INSERT INTO RespaldoRegistros (Codigo, FechaRegistro, CiudadID, EstablecimientoID, ProcesoGeneradorID, ResiduoID, Descripcion, Cantidad, PresentacionID, GestorID, MecanismoID, Certificado, CreatedAt, UpdatedAt, DeletedAt) VALUES ('%s', '%s', %d, %d, %d, %d, '%s', %f, %d, %d, %d, '%s', '%s', '%s', CURRENT_TIMESTAMP)",
        registro$Codigo, registro$FechaRegistro, registro$CiudadID, registro$EstablecimientoID, registro$ProcesoGeneradorID,
        registro$ResiduoID, registro$Descripcion, registro$Cantidad, registro$PresentacionID, registro$GestorID,
        registro$MecanismoID, registro$Certificado, registro$CreatedAt, registro$UpdatedAt
      )
      
      tryCatch({
        dbExecute(con, backup_query)
        dbExecute(con, sprintf("DELETE FROM Registros WHERE ID = %d", registro$ID))
        output$edit_msg <- renderUI({ NULL })
        showNotification("Registro eliminado con éxito y respaldado", type = "warning", duration = 3)
        later::later(function() { output$edit_msg <- renderUI({ NULL }) }, 2)
        removeModal()
        registros_trigger(registros_trigger() + 1)
      }, error = function(e) {
        showNotification(sprintf("Error al eliminar el registro: %s", e$message), type = "error", duration = 5)
      })
    }
  })

  # Agregar dimensión única
  observeEvent(input$agregar, {
    if (input$nuevo_valor != "") {
      table <- switch(input$dimension,
                      "Ciudades" = "Ciudades",
                      "Establecimientos" = "Establecimientos",
                      "ProcesosGeneradores" = "ProcesosGeneradores",
                      "Presentaciones" = "Presentaciones",
                      "Gestores" = "Gestores",
                      "MecanismosEntrega" = "MecanismosEntrega")
      column <- "Nombre"
      dbExecute(con, sprintf("INSERT INTO %s (%s, CreatedAt, UpdatedAt) VALUES ('%s', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", table, column, input$nuevo_valor))
      updateTextInput(session, "nuevo_valor", value = "")
      dimensiones_trigger(dimensiones_trigger() + 1)
    }
  })

  # Borrar dimensión única
  observeEvent(input$borrar_dimension, {
    selected_row <- input$dimensiones_rows_selected
    if (length(selected_row)) {
      table <- switch(input$dimension,
                      "Ciudades" = "Ciudades",
                      "Establecimientos" = "Establecimientos",
                      "ProcesosGeneradores" = "ProcesosGeneradores",
                      "Presentaciones" = "Presentaciones",
                      "Gestores" = "Gestores",
                      "MecanismosEntrega" = "MecanismosEntrega")
      id <- dbGetQuery(con, sprintf("SELECT ID FROM %s LIMIT 1 OFFSET %d", table, selected_row - 1))[[1]]
      dbExecute(con, sprintf("DELETE FROM %s WHERE ID = %d", table, id))
      dimensiones_trigger(dimensiones_trigger() + 1)
    }
  })

  # Agregar residuo
  observeEvent(input$agregar_residuo, {
    if (input$nuevo_residuo != "" && input$nuevo_tipo_residuo != "" && input$nuevo_grado_peligrosidad != "" && input$nuevo_estado_materia != "") {
      dbExecute(con, sprintf("INSERT INTO Residuos (Residuo, Descripcion, TipoResiduo, GradoPeligrosidad, EstadoMateria, CreatedAt, UpdatedAt) VALUES ('%s', '%s', '%s', '%s', '%s', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                            input$nuevo_residuo, input$nueva_descripcion, input$nuevo_tipo_residuo, input$nuevo_grado_peligrosidad, input$nuevo_estado_materia))
      updateTextInput(session, "nuevo_residuo", value = "")
      updateTextInput(session, "nueva_descripcion", value = "")
      updateSelectInput(session, "nuevo_tipo_residuo", selected = "")
      updateSelectInput(session, "nuevo_grado_peligrosidad", selected = "")
      updateSelectInput(session, "nuevo_estado_materia", selected = "")
      residuos_trigger(residuos_trigger() + 1)
    } else {
      showNotification("Por favor complete todos los campos requeridos.", type = "error", duration = 5)
    }
  })

  # Modal para edición de residuos
  observeEvent(input$editar_residuo, {
    selected_row <- input$dimensiones_residuos_rows_selected
    if (length(selected_row)) {
      residuo <- dbGetQuery(con, sprintf("SELECT * FROM Residuos LIMIT 1 OFFSET %d", selected_row - 1))
      showModal(modalDialog(
        title = "Editar Residuo",
        fluidRow(class = "form-row-spaced",
          column(4, textInput("edit_residuo_nombre", "Residuo", value = residuo$Residuo)),
          column(4, textInput("edit_residuo_descripcion", "Descripción", value = residuo$Descripcion)),
          column(4, selectInput("edit_residuo_tipo", "Tipo de Residuo", choices = c("", "Residuo No Peligroso Aprovechable", "Residuo No Peligroso No aprovechable", "Residuo No Peligroso Orgánico degradable", "Residuo Peligroso", "Residuo Especial"), selected = residuo$TipoResiduo))
        ),
        fluidRow(class = "form-row-spaced",
          column(4, selectInput("edit_residuo_grado", "Nivel de Peligro", choices = c("", "No aplica", "Ecotóxico", "Toxicidad"), selected = residuo$GradoPeligrosidad)),
          column(4, selectInput("edit_residuo_estado", "Estado de la Materia", choices = c("", "Sólido o semisólido", "Líquido"), selected = residuo$EstadoMateria)),
          column(4)
        ),
        fluidRow(
          column(12,
            div(style = "display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px;",
              actionButton("guardar_cambios_residuo", "Guardar Cambios"),
              actionButton("borrar_residuo_modal", "Eliminar Residuo"),
              actionButton("cerrar_modal_residuo", "Cerrar")
            )
          )
        ),
        footer = NULL,
        size = "l"
      ))
    }
  })

  observeEvent(input$cerrar_modal_residuo, {
    shinyjs::runjs("$('#shiny-modal').modal('hide');")
    removeModal()
  })

  # Guardar cambios en residuo
  observeEvent(input$guardar_cambios_residuo, {
    if (input$edit_residuo_nombre == "" || input$edit_residuo_tipo == "" || input$edit_residuo_grado == "" || input$edit_residuo_estado == "") {
      showNotification("Por favor complete todos los campos requeridos.", type = "error", duration = 5)
      return()
    }
    selected_row <- input$dimensiones_residuos_rows_selected
    residuo_id <- dbGetQuery(con, sprintf("SELECT ID FROM Residuos LIMIT 1 OFFSET %d", selected_row - 1))[[1]]
    query <- sprintf(
      "UPDATE Residuos SET Residuo = '%s', Descripcion = '%s', TipoResiduo = '%s', GradoPeligrosidad = '%s', EstadoMateria = '%s', UpdatedAt = CURRENT_TIMESTAMP WHERE ID = %d",
      input$edit_residuo_nombre, input$edit_residuo_descripcion, input$edit_residuo_tipo, input$edit_residuo_grado, input$edit_residuo_estado, residuo_id
    )
    tryCatch({
      dbExecute(con, query)
      showNotification("Residuo editado con éxito", type = "message", duration = 3)
      removeModal()
      residuos_trigger(residuos_trigger() + 1)
    }, error = function(e) {
      showNotification(sprintf("Error al guardar cambios: %s", e$message), type = "error", duration = 5)
    })
  })

  # Borrar residuo
  observeEvent(input$borrar_residuo_modal, {
    selected_row <- input$dimensiones_residuos_rows_selected
    id <- dbGetQuery(con, sprintf("SELECT ID FROM Residuos LIMIT 1 OFFSET %d", selected_row - 1))[[1]]
    dbExecute(con, sprintf("DELETE FROM Residuos WHERE ID = %d", id))
    showNotification("Residuo eliminado con éxito", type = "warning", duration = 3)
    removeModal()
    residuos_trigger(residuos_trigger() + 1)
  })

  # Tablero
  dashboard_data <- reactive({
    query <- "
      SELECT r.FechaRegistro, r.Cantidad, rs.TipoResiduo, rs.GradoPeligrosidad, r.Certificado, 
            rs.Residuo, c.Nombre AS Ciudad, e.Nombre AS Establecimiento
      FROM Registros r
      LEFT JOIN Ciudades c ON r.CiudadID = c.ID
      LEFT JOIN Establecimientos e ON r.EstablecimientoID = e.ID
      LEFT JOIN Residuos rs ON r.ResiduoID = rs.ID
    "
    datos <- dbGetQuery(con, query)
    if (!is.null(input$dashboard_fecha)) {
      datos <- datos[as.Date(datos$FechaRegistro) >= input$dashboard_fecha[1] & 
                    as.Date(datos$FechaRegistro) <= input$dashboard_fecha[2], ]
    }
    if (!is.null(input$dashboard_ciudad) && input$dashboard_ciudad != "Todos") {
      datos <- datos[datos$Ciudad == input$dashboard_ciudad, ]
    }
    if (!is.null(input$dashboard_establecimiento) && input$dashboard_establecimiento != "Todos") {
      datos <- datos[datos$Establecimiento == input$dashboard_establecimiento, ]
    }
    if (!is.null(input$dashboard_tipo_residuo) && input$dashboard_tipo_residuo != "Todos") {
      datos <- datos[datos$TipoResiduo == input$dashboard_tipo_residuo, ]
    }
    datos
  })

  observe({
    if (input$sidebar_menu == "dashboard" || is.null(input$sidebar_menu)) {
      query <- "SELECT MIN(FechaRegistro) AS min_date, MAX(FechaRegistro) AS max_date FROM Registros"
      dates <- dbGetQuery(con, query)
      if (nrow(dates) > 0 && !is.na(dates$min_date) && !is.na(dates$max_date)) {
        min_date <- as.Date(dates$min_date)
        max_date <- as.Date(dates$max_date)
        updateDateRangeInput(session, "dashboard_fecha", start = min_date, end = max_date)
      } else {
        default_start <- Sys.Date() - 30
        default_end <- Sys.Date()
        updateDateRangeInput(session, "dashboard_fecha", start = default_start, end = default_end)
      }
    }
  })

  dashboard_agg <- reactive({
    datos <- dashboard_data()
    if (nrow(datos) == 0) return(NULL)
    
    trend_data <- datos
    trend_data$Mes <- as.Date(cut(as.Date(datos$FechaRegistro), "month"))
    trend_agg <- aggregate(Cantidad ~ Mes, data = trend_data, sum, na.rm = TRUE)
    trend_agg$Toneladas <- trend_agg$Cantidad / 1000
    
    pareto_agg <- aggregate(Cantidad ~ Residuo, data = datos, sum, na.rm = TRUE)
    pareto_agg <- pareto_agg[order(-pareto_agg$Cantidad), ]
    pareto_agg$Toneladas <- pareto_agg$Cantidad / 1000
    pareto_agg$Pct <- round(cumsum(pareto_agg$Toneladas) / sum(pareto_agg$Toneladas) * 100, 1)

    bar_agg <- aggregate(Cantidad ~ TipoResiduo, data = datos, sum, na.rm = TRUE)
    bar_agg$Toneladas <- bar_agg$Cantidad / 1000
    bar_agg <- bar_agg[order(bar_agg$Toneladas), ]
    
    # Aggregation for doughnut chart (tons by city)
    ciudad_agg <- aggregate(Cantidad ~ Ciudad, data = datos, sum, na.rm = TRUE)
    ciudad_agg$Toneladas <- ciudad_agg$Cantidad / 1000
    ciudad_agg <- ciudad_agg[order(ciudad_agg$Toneladas, decreasing = TRUE), ]
    
    # Corrected aggregation for stacked bar chart (tons by establishment and waste type)
    stacked_agg <- aggregate(Cantidad ~ Establecimiento + TipoResiduo, data = datos, FUN = sum, na.rm = TRUE)
    stacked_agg$Toneladas <- stacked_agg$Cantidad / 1000  # Matches SQL (SUM(R.Cantidad)/1000)
    stacked_agg <- stacked_agg[!is.na(stacked_agg$Establecimiento) & !is.na(stacked_agg$TipoResiduo), ] # Remove NA values
    
    # Debugging: Print the aggregated data to verify
    print("Stacked Aggregation:")
    print(stacked_agg)
    
    list(trend = trend_agg, pareto = pareto_agg, bar = bar_agg, ciudad = ciudad_agg, stacked = stacked_agg)
  })

  # KPIs
  output$kpi_registros <- renderUI({
    datos <- dashboard_data()
    n <- nrow(datos)
    tags$div(class = "kpi-box", style = "background:#9F3632;",
            tags$h4("Registros"),
            tags$h2(n)
    )
  })

  output$kpi_certificados_recibidos <- renderUI({
    datos <- dashboard_data()
    n <- sum(datos$Certificado == "Recibido", na.rm = TRUE)
    tags$div(class = "kpi-box", style = "background:#2ECC71;",
            tags$h4("Certificados Recibidos"),
            tags$h2(n)
    )
  })

  output$kpi_certificados_pendientes <- renderUI({
    datos <- dashboard_data()
    n <- sum(datos$Certificado == "Pendiente", na.rm = TRUE)
    tags$div(class = "kpi-box", style = "background:#E67E22;",
            tags$h4("Certificados Pendientes"),
            tags$h2(n)
    )
  })

  output$kpi_toneladas <- renderUI({
    datos <- dashboard_data()
    t <- sum(datos$Cantidad, na.rm = TRUE) / 1000
    tags$div(class = "kpi-box", style = "background:#3498DB;",
            tags$h4("Toneladas Generadas"),
            tags$h2(sprintf("%.2f", t))
    )
  })

  output$kpi_peligrosos <- renderUI({
    datos <- dashboard_data()
    total <- sum(datos$Cantidad, na.rm = TRUE)
    peligrosos <- sum(datos$Cantidad[grepl("Residuo Peligroso", datos$TipoResiduo, ignore.case = TRUE)], na.rm = TRUE)
    
    pct <- if (total > 0) round(100 * peligrosos / total, 1) else 0
    
    tags$div(class = "kpi-box", style = "background:#E74C3C;",
             tags$h4("% Residuos Peligrosos"),
             tags$h2(paste0(pct, "%"))
  )})
    
    output$kpi_no_peligrosos <- renderUI({
      datos <- dashboard_data()
      total <- sum(datos$Cantidad, na.rm = TRUE)
      no_peligrosos <- sum(
        datos$Cantidad[
          grepl("Residuo No Peligroso", datos$TipoResiduo, ignore.case = TRUE)
        ], 
        na.rm = TRUE
      )
      
      pct <- if (total > 0) round(100 * no_peligrosos / total, 1) else 0
      
      tags$div(
        class = "kpi-box", 
        style = "background:#27AE60;",
        tags$h4("% Residuos No Peligrosos"),
        tags$h2(paste0(pct, "%"))
      )
    })
    
    output$kpi_especiales <- renderUI({
      datos <- dashboard_data()
      total <- sum(datos$Cantidad, na.rm = TRUE)
      especiales <- sum(datos$Cantidad[grepl("Residuo Especial", datos$TipoResiduo, ignore.case = TRUE)], na.rm = TRUE)
      
      pct <- if (total > 0) round(100 * especiales / total, 1) else 0
      
      tags$div(
        class = "kpi-box", 
        style = "background:#9B59B6;",
        tags$h4("% Residuos Especiales"),
        tags$h2(paste0(pct, "%"))
      )
    })
    
  # Gráficos del Tablero


  output$dashboard_doughnut_ciudad <- renderPlotly({
    agg <- dashboard_agg()$ciudad
    if (is.null(agg) || nrow(agg) == 0 || all(is.na(agg$Ciudad)) || all(is.na(agg$Toneladas))) {
      output$doughnut_ciudad_empty <- renderUI({
        tags$div(class = "alert alert-info", "No hay datos disponibles para el gráfico de proporción por ciudad.")
      })
      return(NULL)
    }
    output$doughnut_ciudad_empty <- renderUI({ NULL })
    
    n <- length(unique(agg$Ciudad))
    colors <- viridisLite::viridis(n)
    
    # Prepare text colors based on city
    text_colors <- ifelse(agg$Ciudad == "Medellín", "#000000", "#E6F0FA")
    hover_colors <- ifelse(agg$Ciudad == "Medellín", "black", "white")
    
    # Calculate total tons for percentage
    total_tons <- sum(agg$Toneladas)
    
    p <- plot_ly(data = agg, labels = ~Ciudad, values = ~Toneladas, type = "pie", hole = 0.4,
                textinfo = "label+percent+value", texttemplate = "%{label}<br>%{percent:.1%}<br>%{value:.2f} Ton",
                textposition = "inside",
                textfont = list(color = ~text_colors),  # Dynamic text color per segment
                hoverinfo = "text",
                hovertext = ~paste0("<span style='color:", hover_colors, "'>",
                                    "Ciudad: ", Ciudad, "<br>",
                                    "Porcentaje: ", round(Toneladas / total_tons * 100, 1), "%<br>",
                                    "Toneladas: ", round(Toneladas, 2), " Ton</span>"),
                marker = list(colors = colors, line = list(color = "#162C40", width = 1)),
                showlegend = FALSE)
    
    p <- p %>% layout(
      title = list(text = "Proporción de Toneladas por Ciudad", font = list(color = "#E6F0FA", size = 12), xanchor = "left", x = 0.02),
      plot_bgcolor = "#162C40",
      paper_bgcolor = "#162C40",
      margin = list(l = 50, r = 50, t = 50, b = 50),
      hovermode = "closest",
      hoverlabel = list(font = list(color = "#FFFFFF"))  # Default, overridden by hovertext
    )
    p
  })


  output$dashboard_stacked_establecimiento <- renderPlotly({
    agg <- dashboard_agg()$stacked
    if (is.null(agg) || nrow(agg) == 0 || all(is.na(agg$Establecimiento)) || all(is.na(agg$Toneladas))) {
      output$stacked_establecimiento_empty <- renderUI({
        tags$div(class = "alert alert-info", "No hay datos disponibles para el gráfico de residuos por establecimiento.")
      })
      return(NULL)
    }
    output$stacked_establecimiento_empty <- renderUI({ NULL })
    
    # Calculate total tons per establishment for sorting
    total_tons_per_estab <- aggregate(Toneladas ~ Establecimiento, data = agg, sum, na.rm = TRUE)
    total_tons_per_estab <- total_tons_per_estab[order(-total_tons_per_estab$Toneladas), ]
    ordered_estabs <- total_tons_per_estab$Establecimiento
    
    # Order the agg data based on the sorted establishments
    agg$Establecimiento <- factor(agg$Establecimiento, levels = ordered_estabs)
    
    unique_tipos <- unique(agg$TipoResiduo)
    n <- length(unique_tipos)
    colors <- viridisLite::viridis(n)
    
    p <- plot_ly()
    for (i in seq_along(unique_tipos)) {
      tipo <- unique_tipos[i]
      data_tipo <- agg[agg$TipoResiduo == tipo, ]
      p <- p %>% add_trace(data = data_tipo, x = ~Establecimiento, y = ~Toneladas, type = "bar",
                          name = tipo, marker = list(color = colors[i]),
                          text = ~ifelse(Toneladas > 0, paste(round(Toneladas, 2), "Ton"), ""),
                          textposition = "outside",  # Labels outside
                          textangle = 0,  # Rotate text 180 degrees
                          textfont = list(size = 8, color = "#E6F0FA"),  # White labels, size 8
                          hoverinfo = "text",
                          hovertext = ~paste("Tipo de Residuo:", TipoResiduo, "<br>Toneladas:", round(Toneladas, 2), "Ton"))
    }
    p <- p %>% layout(
      title = list(text = "Toneladas de Residuos por Establecimiento y Tipo", font = list(color = "#E6F0FA", size = 12), xanchor = "left", x = 0.02),
      xaxis = list(title = "", tickfont = list(color = "#E6F0FA", size = 8), showgrid = FALSE, zeroline = FALSE, categoryorder = "array", categoryarray = ordered_estabs),
      yaxis = list(title = "Toneladas", titlefont = list(color = "#E6F0FA", size = 10), tickfont = list(color = "#E6F0FA", size = 10), showgrid = FALSE, zeroline = FALSE),
      barmode = "group",  # Grouped bars
      plot_bgcolor = "#162C40",
      paper_bgcolor = "#162C40",
      margin = list(l = 60, r = 60, t = 50, b = 70),  # Increased margins for labels
      hovermode = "x unified",
      hoverlabel = list(font = list(color = "#FFFFFF")),
      legend = list(orientation = "h", x = 0.5, xanchor = "center", y = -0.2, font = list(color = "#E6F0FA", size = 8))
    )
    p
  })

  output$dashboard_tendencia <- renderPlotly({
    agg <- dashboard_agg()$trend
    if (is.null(agg) || nrow(agg) == 0 || all(is.na(agg$Mes)) || all(is.na(agg$Toneladas))) {
      output$tendencia_empty <- renderUI({
        tags$div(class = "alert alert-info", "No hay datos disponibles para la tendencia.")
      })
      return(NULL)
    }
    output$tendencia_empty <- renderUI({ NULL })
    
    p <- plot_ly(data = agg, x = ~format(Mes, "%Y-%m"), y = ~Toneladas, type = "scatter", mode = "lines+markers",
                line = list(color = "white", width = 2),
                marker = list(color = "#2E6A9A", size = 6)) %>%
      layout(
        title = list(text = "Tendencia de Toneladas de Residuos Generadas Mes a Mes", font = list(color = "#E6F0FA", size = 12), xanchor = "left", x = 0.01),
        xaxis = list(title = "", titlefont = list(color = "#E6F0FA"), tickfont = list(color = "#E6F0FA"),
                    gridcolor = "#225A8A", zerolinecolor = "#225A8A", tickformat = "%Y-%m"),
        yaxis = list(title = "Toneladas", titlefont = list(color = "#E6F0FA"), tickfont = list(color = "#E6F0FA"),
                    gridcolor = "#225A8A", zerolinecolor = "#225A8A"),
        plot_bgcolor = "#162C40",
        paper_bgcolor = "#162C40",
        margin = list(l = 50, r = 50, t = 50, b = 50),
        hovermode = "x unified",
        hoverlabel = list(font = list(color = "#FFFFFF"))
      )
    p
  })

  output$dashboard_pareto <- renderPlotly({
    agg <- dashboard_agg()$pareto
    if (is.null(agg) || nrow(agg) == 0 || all(is.na(agg$Residuo)) || all(is.na(agg$Toneladas))) {
      output$pareto_empty <- renderUI({
        tags$div(class = "alert alert-info", "No hay datos disponibles para el diagrama de Pareto.")
      })
      return(NULL)
    }
    output$pareto_empty <- renderUI({ NULL })
    
    n <- length(unique(agg$Residuo))
    colors <- viridisLite::viridis(n) 
    
    p <- plot_ly() %>%
      add_bars(data = agg, x = ~reorder(Residuo, -Toneladas), y = ~Toneladas, name = "Toneladas",
              marker = list(color = colors)) %>%
      add_lines(x = ~reorder(Residuo, -Toneladas), y = ~Pct, name = "% Acumulados", yaxis = "y2",
                line = list(color = "#EF4444", width = 2, dash = "dash"),
                marker = list(color = "#EF4444", size = 6)) %>%
      layout(
        title = list(text = "Diagrama de Pareto por Residuos Generados", font = list(color = "#E6F0FA", size = 12), xanchor = "left", x = 0.02),
        xaxis = list(title = "", showgrid = FALSE, zeroline = FALSE, tickfont = list(color = "#E6F0FA", size = 8)),
        yaxis = list(title = "Toneladas", titlefont = list(color = "#E6F0FA", size = 10), tickfont = list(color = "#E6F0FA", size = 10), showgrid = FALSE, zeroline = FALSE),
        yaxis2 = list(title = "% Acumulados", overlaying = "y", side = "right", titlefont = list(color = "#E6F0FA", size = 10), tickfont = list(color = "#E6F0FA", size = 10), showgrid = FALSE, zeroline = FALSE),
        plot_bgcolor = "#162C40",
        paper_bgcolor = "#162C40",
        margin = list(l = 50, r = 50, t = 50, b = 50),
        hovermode = "x unified",
        hoverlabel = list(font = list(color = "#FFFFFF")),
        showlegend = FALSE
      )
    p
  })

  output$dashboard_bar_tipo <- renderPlotly({
      agg <- dashboard_agg()$bar
      if (is.null(agg) || nrow(agg) == 0 || all(is.na(agg$TipoResiduo)) || all(is.na(agg$Toneladas))) {
        output$bar_tipo_empty <- renderUI({
          tags$div(class = "alert alert-info", "No hay datos disponibles para el gráfico de barras.")
        })
        return(NULL)
      }
      output$bar_tipo_empty <- renderUI({ NULL })
      
      agg$TipoResiduo <- factor(agg$TipoResiduo, levels = agg$TipoResiduo[order(agg$Toneladas, decreasing = FALSE)])
      
      n <- length(unique(agg$TipoResiduo))
      colors <- viridisLite::viridis(n)
      
      p <- plot_ly(data = agg, y = ~TipoResiduo, x = ~Toneladas, type = "bar",
                  marker = list(color = colors)) %>%
        add_trace(x = ~Toneladas, y = ~TipoResiduo, type = 'bar', 
                  text = ~paste(round(Toneladas, 2), "Ton"), 
                  textposition = 'outside', 
                  textfont = list(color = '#E6F0FA', size = 10),
                  cliponaxis = FALSE, 
                  hoverinfo = 'none') %>% 
        layout(
          title = list(text = "Toneladas de Residuos Generadas por Tipo de Residuo", font = list(color = "#E6F0FA", size = 12), xanchor = "left", x = 0.02),
          xaxis = list(
            title = "", 
            showgrid = FALSE, 
            zeroline = FALSE, 
            showticklabels = FALSE,  
            ticks = "",             
            tickfont = list(color = "#E6F0FA", size = 10)
          ),
          yaxis = list(title = "", showgrid = FALSE, zeroline = FALSE, tickfont = list(color = "#E6F0FA", size = 10)),
          plot_bgcolor = "#162C40",
          paper_bgcolor = "#162C40",
          margin = list(l = 50, r = 50, t = 50, b = 50),
          hovermode = "y unified",
          hoverlabel = list(font = list(color = "#FFFFFF")),
          showlegend = FALSE
        )
      p
    })


}

# Desconectar de la base de datos al cerrar la aplicación

onStop(function() {
  dbDisconnect(con)
})

# Ejecutar la aplicación Shiny

args <- commandArgs(trailingOnly = TRUE)
port <- if (length(args) > 0 && grepl("port=", args[1])) {
  as.integer(sub("port=", "", args[1]))
} else {
  8888
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
