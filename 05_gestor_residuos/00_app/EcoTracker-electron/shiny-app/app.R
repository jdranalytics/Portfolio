library(shiny)
library(DBI)
library(RSQLite)
library(DT)
library(plotly)
library(viridis)

# Definición de operador %||% para valores por defecto
`%||%` <- function(a, b) if (!is.null(a)) a else b

# Establish database connection
con <- dbConnect(RSQLite::SQLite(), dbname = "ecogestor.db")

# Creación de Tablas en SQLite si no existen

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
  FechaRegistro DATE,
  CiudadID INTEGER,
  EstablecimientoID INTEGER,
  ProcesoGeneradorID INTEGER,
  ResiduoID INTEGER,
  Descripcion TEXT,
  TipoResiduo TEXT,
  GradoPeligrosidad TEXT,
  EstadoMateria TEXT,
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

# EJEMPLOS


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
  ('Aparatos eléctricos y electrónicos', 'Equipos obsoletso', 'Residuo Peligroso', 'Ecotóxico', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Aceites y grasas', 'Mantenimiento', 'Residuo Peligroso', 'Ecotóxico', 'Líquido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Escombros', 'Mantenimiento y construcción', 'Residuo Especial', 'No aplica', 'Líquido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('Llantas usadas', 'de vehiculos de uso interno', 'Residuo Especial', 'No aplica', 'Sólido o semisólido', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
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
  ('Progrma postconsumo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
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



if (!"Certificado" %in% dbGetQuery(con, "PRAGMA table_info(Registros)")$name) {
  dbExecute(con, "ALTER TABLE Registros ADD COLUMN Certificado TEXT DEFAULT 'Pendiente';")
}


custom_css <- "
  body {
    background-color: #2C3E50; /* Fondo oscuro */
    color: #ECF0F1; /* Texto claro */
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  }
  .container-fluid {
    padding: 20px;
  }
  .form-control:disabled {
  color: #222 !important; /* Texto negro para campos deshabilitados */
  opacity: 1; /* Evitar que el campo deshabilitado se vea desvaído */
  }
  .modal-title {
  color: #222 !important; /* Título del modal en negro */
  }
  .titlePanel {
    color: #2ECC71; /* Verde esmeralda para el título */
    text-align: center;
    margin-bottom: 30px;
  }
  .well {
    background-color: #34495E; /* Fondo para paneles/cajas */
    border: none;
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
  }
  .nav-tabs > li > a {
    color: #ECF0F1; /* Color de texto para pestañas inactivas */
    background-color: #34495E; /* Fondo de pestañas inactivas */
    border-color: #34495E;
  }
  .nav-tabs > li.active > a,
  .nav-tabs > li.active > a:hover,
  .nav-tabs > li.active > a:focus {
    color: #2C3E50; /* Texto oscuro para pestaña activa */
    background-color: #2ECC71; /* Verde para pestaña activa */
    border-color: #2ECC71;
  }
  .nav-tabs > li > a:hover {
    background-color: #5D6D7E; /* Oscuro al pasar el ratón */
    border-color: #5D6D7E;
  }
  .tab-content {
    background-color: #34495E;
    padding: 20px;
    border-radius: 0 0 8px 8px;
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
  }
  label {
    color: #ECF0F1; /* Color de texto para etiquetas de input */
  }
  .form-control {
    background-color: #4A6076; /* Fondo de inputs */
    color: #ECF0F1; /* Texto de inputs */
    border: 1px solid #5D6D7E;
  }
  .form-control:focus {
    border-color: #2ECC71; /* Borde al enfocar */
    box-shadow: 0 0 0 0.2rem rgba(46, 204, 113, 0.25);
  }
  .btn-default {
    background-color: #3498DB; /* Azul para botones por defecto */
    color: #FFFFFF;
    border: none;
    transition: background-color 0.3s ease;
  }
  .btn-default:hover {
    background-color: #217DBB; /* Azul más oscuro al pasar el ratón */
    color: #FFFFFF;
  }
  .dataTables_wrapper .dataTables_length,
  .dataTables_wrapper .dataTables_filter,
  .dataTables_wrapper .dataTables_info,
  .dataTables_wrapper .dataTables_paginate {
    color: #ECF0F1; /* Color de texto para elementos de la tabla (paginación, búsqueda) */
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button.current,
  .dataTables_wrapper .dataTables_paginate .paginate_button.current:hover {
    background-color: #2ECC71 !important;
    color: #FFFFFF !important;
    border-color: #2ECC71 !important;
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button {
    background-color: #3498DB;
    color: #FFFFFF !important;
    border-color: #3498DB;
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button:hover {
    background-color: #217DBB;
    color: #FFFFFF !important;
    border-color: #217DBB;
  }
  table.dataTable thead th {
    background-color: #2C3E50; /* Fondo de encabezado de tabla */
    color: #ECF0F1; /* Texto de encabezado de tabla */
    border-bottom: 1px solid #5D6D7E;
  }
  table.dataTable tbody td {
    background-color: #34495E; /* Fondo de celdas de tabla */
    color: #ECF0F1; /* Texto de celdas de tabla */
    border-top: 1px solid #5D6D7E;
  }
  table.dataTable.hover tbody tr:hover {
    background-color: #5D6D7E !important; /* Fondo al pasar el ratón por la fila */
  }
  /* Estilo para los KPI boxes */
  .kpi-box {
    padding: 15px;
    border-radius: 8px;
    text-align: center;
    color: #FFFFFF;
    margin-bottom: 20px; /* Espacio entre KPIs */
  }
  .kpi-box h4 {
    margin-top: 0;
    font-size: 1.1em;
    opacity: 0.8;
  }
  .kpi-box h2 {
    margin-bottom: 0;
    font-size: 2.2em;
    font-weight: bold;
  }
  .modal-dialog {
    width: 80%;
    max-width: 1200px;
  }
"


ui <- fluidPage(
  titlePanel("Gestor de Residuos - EcoTracker Manager v. 1.0.0"),
  tags$head(
    tags$style(HTML(custom_css)),
    tags$style(HTML("
      .datepicker table {
        background-color: #2C3E50;
        color: #FFFFFF;
      }
      .datepicker table tr td.day {
        color: #FFFFFF;
      }
      .datepicker table tr td.day:hover {
        background-color: #3498DB;
        color: #FFFFFF;
      }
      .datepicker table tr td.active {
        background-color: #2ECC71 !important;
        color: #FFFFFF !important;
      }
      .form-row-spaced {
        margin-bottom: 25px;
      }
      .dt-selected-row {
        background-color: #2ecc71 !important;
        color: #2C3E50 !important;
      }
      /* Modal edit labels in black */
      .modal-content label, .modal-content .control-label {
        color: #222 !important;
      }
    "))
  ),
  tabsetPanel(
    id = "main_tabs",
    tabPanel("Nuevo Registro",
             fluidRow(
               column(12, htmlOutput("contador_registros"))
             ),
             fluidRow(class = "form-row-spaced",
               column(4, dateInput("fecha", "Fecha de Registro", value = Sys.Date())),
               column(4, selectInput("ciudad", "Ciudad", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]))),
               column(4, selectInput("establecimiento", "Establecimiento", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]])))
             ),
             fluidRow(class = "form-row-spaced",
               column(4, selectInput("proceso", "Proceso Generador", choices = c("", dbGetQuery(con, "SELECT Nombre FROM ProcesosGeneradores")[[1]]))),
               column(4, selectInput("residuo", "Residuo", choices = c("", dbGetQuery(con, "SELECT Residuo FROM Residuos")[[1]]))),
               column(4,
                 tags$label("Descripción", style="color:#ECF0F1;"),
                 uiOutput("descripcion_ui")
               )
             ),
             fluidRow(class = "form-row-spaced",
               column(4,
                 tags$label("Tipo de Residuo", style="color:#ECF0F1;"),
                 uiOutput("tipo_residuo_ui"
                 )
               ),
               column(4,
                 tags$label("Grado de Peligrosidad", style="color:#ECF0F1;"),
                 uiOutput("grado_peligro_ui")
               ),
               column(4,
                 tags$label("Estado del Material", style="color:#ECF0F1;"),
                 uiOutput("estado_materia_ui")
               )
             ),
             fluidRow(class = "form-row-spaced",
               column(4, numericInput("cantidad", "Cantidad (Kg)", value = 0, min = 0, step = 0.1)),
               column(4, selectInput("presentacion", "Presentación", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Presentaciones")[[1]]))),
               column(4, selectInput("gestor", "Gestor", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Gestores")[[1]])))
             ),
             fluidRow(class = "form-row-spaced",
               column(4, selectInput("mecanismo", "Mecanismo de Entrega", choices = c("", dbGetQuery(con, "SELECT Nombre FROM MecanismosEntrega")[[1]]))),
               column(4, selectInput("certificado", "Certificado", choices = c("Pendiente", "Recibido", "No Aplica"), selected = "Pendiente")),
               column(4,
                 actionButton("guardar", "Guardar"),
                 actionButton("limpiar", "Limpiar"),
                 br(), br(),
                 uiOutput("guardar_progress"),
                 uiOutput("guardar_msg")
               )
             )
    ),
    tabPanel("Visualizar/Editar",
             fluidRow(
               column(12, DTOutput("registros"))
             ),
             fluidRow(
               column(6, actionButton("editar", "Editar Seleccionado")),
               column(6, actionButton("refresh_registros", "Refrescar"))
             )
    ),
    tabPanel("Gestionar Dimensiones",
      fluidRow(
        column(12, DTOutput("dimensiones"))
      ),
      fluidRow(
        column(4, selectInput("dimension", "Dimensión", choices = c("Ciudades", "Establecimientos", "ProcesosGeneradores", "Residuos", "Presentaciones", "Gestores", "MecanismosEntrega"))),
        column(4, textInput("nuevo_valor", "Nuevo Valor"))
      ),
      fluidRow(
        column(2, actionButton("agregar", "Agregar")),
        column(2, actionButton("borrar_dimension", "Borrar Seleccionado")),
        column(2, actionButton("refresh_dimensiones", "Refrescar"))
      )
    ),
    tabPanel(
      "Dashboard",
      fluidRow(
        column(4, dateRangeInput("dashboard_fecha", "Rango de Fecha", start = Sys.Date()-30, end = Sys.Date())),
        column(4, selectInput("dashboard_ciudad", "Ciudad", choices = c("Todas", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]))),
        column(4, selectInput("dashboard_establecimiento", "Establecimiento", choices = c("Todas", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]])))
      ),
      br(),
      # KPI Grid with responsive layout
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
        column(12, uiOutput("kpi_especiales"), class = "kpi-box")
      ),
      br(),
      # Plots with responsive height
      fluidRow(
        column(12, 
              plotlyOutput("dashboard_tendencia", height = "40vh"),
              uiOutput("tendencia_empty"),
        )
      ),
      br(), 
      br(),
      fluidRow(
        column(6, 
              plotlyOutput("dashboard_pareto", height = "40vh"),
              uiOutput("pareto_empty")
        ),
        column(6, 
              plotlyOutput("dashboard_bar_tipo", height = "40vh"),
              uiOutput("bar_tipo_empty")
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
    updateSelectInput(session, "dashboard_ciudad", choices = c("Todas", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]))
    updateSelectInput(session, "dashboard_establecimiento", choices = c("Todas", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]]))
  })
  
  # Campos dinámicos
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
  
  # Guardar nuevo registro
  observeEvent(input$guardar, {
    output$guardar_progress <- renderUI({
      tags$div(style="margin-top:10px;", 
        tags$div(class="progress", style="height: 20px; background: #34495E;",
          tags$div(class="progress-bar progress-bar-striped active", 
                   role="progressbar", style="width:100%; background:#3498DB;", "Guardando...")
        )
      )
    })
    ciudad_id <- dbGetQuery(con, sprintf("SELECT ID FROM Ciudades WHERE Nombre = '%s'", input$ciudad))[[1]]
    establecimiento_id <- dbGetQuery(con, sprintf("SELECT ID FROM Establecimientos WHERE Nombre = '%s'", input$establecimiento))[[1]]
    proceso_id <- dbGetQuery(con, sprintf("SELECT ID FROM ProcesosGeneradores WHERE Nombre = '%s'", input$proceso))[[1]]
    residuo_id <- dbGetQuery(con, sprintf("SELECT ID FROM Residuos WHERE Residuo = '%s'", input$residuo))[[1]]
    presentacion_id <- dbGetQuery(con, sprintf("SELECT ID FROM Presentaciones WHERE Nombre = '%s'", input$presentacion))[[1]]
    gestor_id <- dbGetQuery(con, sprintf("SELECT ID FROM Gestores WHERE Nombre = '%s'", input$gestor))[[1]]
    mecanismo_id <- dbGetQuery(con, sprintf("SELECT ID FROM MecanismosEntrega WHERE Nombre = '%s'", input$mecanismo))[[1]]

    descripcion <- input$descripcion %||% ""
    tipo_residuo <- input$tipo_residuo %||% ""
    grado_peligro <- input$grado_peligro %||% ""
    estado_materia <- input$estado_materia %||% ""

    query <- sprintf("INSERT INTO Registros (FechaRegistro, CiudadID, EstablecimientoID, ProcesoGeneradorID, ResiduoID, Descripcion, TipoResiduo, GradoPeligrosidad, EstadoMateria, Cantidad, PresentacionID, GestorID, MecanismoID, Certificado, CreatedAt, UpdatedAt) VALUES ('%s', %d, %d, %d, %d, '%s', '%s', '%s', '%s', %f, %d, %d, %d, '%s', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                     input$fecha, ciudad_id, establecimiento_id, proceso_id, residuo_id, descripcion, tipo_residuo,
                     grado_peligro, estado_materia, input$cantidad, presentacion_id, gestor_id, mecanismo_id, input$certificado)
    dbExecute(con, query)
    Sys.sleep(0.7)
    output$guardar_progress <- renderUI({ NULL })
    showNotification("¡Datos almacenados con éxito!", type = "message", duration = 3)
    output$contador_registros <- renderUI({
      n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM Registros")$n
      HTML(sprintf("<h4>Registros almacenados: <b>%d</b></h4>", n))
    })
    later::later(function() { output$guardar_msg <- renderUI({ NULL }) }, 2)
    registros_trigger(registros_trigger() + 1)

    # Limpiar campos tras guardar registro
    updateDateInput(session, "fecha", value = Sys.Date())
    updateSelectInput(session, "ciudad", selected = "")
    updateSelectInput(session, "establecimiento", selected = "")
    updateSelectInput(session, "proceso", selected = "")
    updateSelectInput(session, "residuo", selected = "")
    updateNumericInput(session, "cantidad", value = 0)
    updateSelectInput(session, "presentacion", selected = "")
    updateSelectInput(session, "gestor", selected = "")
    updateSelectInput(session, "mecanismo", selected = "")
    updateSelectInput(session, "certificado", selected = "Pendiente")
  }, ignoreInit = TRUE)

  # Limpiar campos de registro
  observeEvent(input$limpiar, {
    updateDateInput(session, "fecha", value = Sys.Date())
    updateSelectInput(session, "ciudad", selected = "")
    updateSelectInput(session, "establecimiento", selected = "")
    updateSelectInput(session, "proceso", selected = "")
    updateSelectInput(session, "residuo", selected = "")
    updateNumericInput(session, "cantidad", value = 0)
    updateSelectInput(session, "presentacion", selected = "")
    updateSelectInput(session, "gestor", selected = "")
    updateSelectInput(session, "mecanismo", selected = "")
    updateSelectInput(session, "certificado", selected = "Pendiente")
  })

  # Contador de registros
  output$contador_registros <- renderUI({
    n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM Registros")$n
    HTML(sprintf("<h4>Registros almacenados: <b>%d</b></h4>", n))
  })

  # Trigger reactivo para refrescar registros
  registros_trigger <- reactiveVal(0)

  # Dispara el refresco en los eventos relevantes
  observeEvent(input$refresh_registros, {
    registros_trigger(registros_trigger() + 1)
  })
  observeEvent(input$main_tabs, {
    if (input$main_tabs == "Visualizar/Editar") registros_trigger(registros_trigger() + 1)
  })
  observeEvent(input$guardar_cambios, {
    removeModal()
    registros_trigger(registros_trigger() + 1)
  })
  observeEvent(input$borrar_registro_modal, {
    removeModal()
    registros_trigger(registros_trigger() + 1)
  })
  observeEvent(input$borrar, {
    registros_trigger(registros_trigger() + 1)
  })

  # Consulta reactiva de registros
  registros_data <- reactive({
    registros_trigger() 
    dbGetQuery(con, "SELECT r.ID, r.FechaRegistro, c.Nombre AS Ciudad, e.Nombre AS Establecimiento, pg.Nombre AS Proceso, rs.Residuo, rs.Descripcion, rs.TipoResiduo, rs.GradoPeligrosidad, rs.EstadoMateria, r.Cantidad, p.Nombre AS Presentacion, g.Nombre AS Gestor, me.Nombre AS Mecanismo, r.Certificado FROM Registros r LEFT JOIN Ciudades c ON r.CiudadID = c.ID LEFT JOIN Establecimientos e ON r.EstablecimientoID = e.ID LEFT JOIN ProcesosGeneradores pg ON r.ProcesoGeneradorID = pg.ID LEFT JOIN Residuos rs ON r.ResiduoID = rs.ID LEFT JOIN Presentaciones p ON r.PresentacionID = p.ID LEFT JOIN Gestores g ON r.GestorID = g.ID LEFT JOIN MecanismosEntrega me ON r.MecanismoID = me.ID")
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

  dimensiones_trigger <- reactiveVal(0)
  observeEvent(input$refresh_dimensiones, {
    dimensiones_trigger(dimensiones_trigger() + 1)
  })
  observeEvent(input$main_tabs, {
    if (input$main_tabs == "Gestionar Dimensiones") dimensiones_trigger(dimensiones_trigger() + 1)
  })
  observeEvent(input$agregar, { dimensiones_trigger(dimensiones_trigger() + 1) })
  observeEvent(input$borrar_dimension, { dimensiones_trigger(dimensiones_trigger() + 1) })

  output$dimensiones <- renderDT({
    dimensiones_trigger() 
    query <- switch(input$dimension,
      "Ciudades" = "SELECT ID, Nombre FROM Ciudades",
      "Establecimientos" = "SELECT ID, Nombre FROM Establecimientos",
      "ProcesosGeneradores" = "SELECT ID, Nombre FROM ProcesosGeneradores",
      "Residuos" = "SELECT ID, Residuo FROM Residuos",
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

  # --- EDICIÓN MODAL: CAMPOS DINÁMICOS ---
  
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


  # --- MODAL DE EDICIÓN ---
  
  observeEvent(input$editar, {
    selected_row <- input$registros_rows_selected
    if (length(selected_row)) {
      registro <- dbGetQuery(con, sprintf("SELECT * FROM Registros WHERE ID = %d", dbGetQuery(con, sprintf("SELECT ID FROM Registros LIMIT 1 OFFSET %d", selected_row - 1))[[1]]))
      showModal(modalDialog(
        title = "Editar Registro",
        fluidRow(class = "form-row-spaced",
                 column(4,
                        tags$label("Fecha de Registro", style="color:#222;"),
                        tags$label(as.character(as.Date(registro$FechaRegistro)), style="color:#222;")
                 ),
                 column(4, selectInput("edit_ciudad", "Ciudad", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Ciudades")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM Ciudades WHERE ID = %d", registro$CiudadID))[[1]])),
                 column(4, selectInput("edit_establecimiento", "Establecimiento", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Establecimientos")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM Establecimientos WHERE ID = %d", registro$EstablecimientoID))[[1]]))
        ),
        fluidRow(class = "form-row-spaced",
                 column(4, selectInput("edit_proceso", "Proceso Generador", choices = c("", dbGetQuery(con, "SELECT Nombre FROM ProcesosGeneradores")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM ProcesosGeneradores WHERE ID = %d", registro$ProcesoGeneradorID))[[1]])),
                 column(4, selectInput("edit_residuo", "Residuo", choices = c("", dbGetQuery(con, "SELECT Residuo FROM Residuos")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Residuo FROM Residuos WHERE ID = %d", registro$ResiduoID))[[1]])),
                 column(4,
                        tags$label("Descripción", style="color:#222;"),
                        uiOutput("edit_descripcion_ui")
                 )
        ),
        fluidRow(class = "form-row-spaced",
                 column(4,
                        tags$label("Tipo de Residuo", style="color:#222;"),
                        uiOutput("edit_tipo_residuo_ui")
                 ),
                 column(4,
                        tags$label("Grado de Peligrosidad", style="color:#222;"),
                        uiOutput("edit_grado_peligro_ui")
                 ),
                 column(4,
                        tags$label("Estado del Material", style="color:#222;"),
                        uiOutput("edit_estado_materia_ui")
                 )
        ),
        fluidRow(class = "form-row-spaced",
                 column(4, numericInput("edit_cantidad", "Cantidad (Kg)", value = registro$Cantidad, min = 0, step = 0.1)),
                 column(4, selectInput("edit_presentacion", "Presentación", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Presentaciones")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM Presentaciones WHERE ID = %d", registro$PresentacionID))[[1]])),
                 column(4, selectInput("edit_gestor", "Gestor", choices = c("", dbGetQuery(con, "SELECT Nombre FROM Gestores")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM Gestores WHERE ID = %d", registro$GestorID))[[1]]))
        ),
        fluidRow(class = "form-row-spaced",
                 column(4, selectInput("edit_mecanismo", "Mecanismo de Entrega", choices = c("", dbGetQuery(con, "SELECT Nombre FROM MecanismosEntrega")[[1]]), selected = dbGetQuery(con, sprintf("SELECT Nombre FROM MecanismosEntrega WHERE ID = %d", registro$MecanismoID))[[1]])),
                 column(4, selectInput("edit_certificado", "Certificado", choices = c("Pendiente", "Recibido", "No Aplica"), selected = registro$Certificado)),
                 column(4)
        ),
        fluidRow(
          column(12, 
                 div(style = "display: flex; justify-content: flex-end; gap: 10px; margin-top: 20px;",
                     actionButton("guardar_cambios", "Guardar Cambios", style = "background:#3498DB; color:white;"),
                     actionButton("borrar_registro_modal", "Borrar Registro", style = "background:#e74c3c; color:white;"),
                     actionButton("cerrar_modal", "Cerrar", style = "background:#95a5a6; color:white;")
                 )
          )
        ),
        uiOutput("edit_progress"),
        uiOutput("edit_msg"),
        footer = NULL,
        size = "l"
      ))
    }
  })
  
  observeEvent(input$cerrar_modal, {
    shinyjs::runjs("$('#shiny-modal').modal('hide');")
    removeModal()
  })

    # --- NOTIFICACIONES EN EDICIÓN Y BORRADO ---
  
  
  observeEvent(input$guardar_cambios, {
    # Validar que todos los campos requeridos tengan valores válidos
    if (is.null(input$edit_ciudad) || input$edit_ciudad == "" ||
        is.null(input$edit_establecimiento) || input$edit_establecimiento == "" ||
        is.null(input$edit_proceso) || input$edit_proceso == "" ||
        is.null(input$edit_residuo) || input$edit_residuo == "" ||
        is.null(input$edit_presentacion) || input$edit_presentacion == "" ||
        is.null(input$edit_gestor) || input$edit_gestor == "" ||
        is.null(input$edit_mecanismo) || input$edit_mecanismo == "") {
      showNotification("Por favor, complete todos los campos requeridos.", type = "error", duration = 5)
      return()
    }
    
    # Obtener IDs con manejo de errores
    ciudad_id <- dbGetQuery(con, sprintf("SELECT ID FROM Ciudades WHERE Nombre = '%s'", input$edit_ciudad))
    if (nrow(ciudad_id) == 0) {
      showNotification("Ciudad no encontrada.", type = "error", duration = 5)
      return()
    }
    ciudad_id <- ciudad_id$ID[1]
    
    establecimiento_id <- dbGetQuery(con, sprintf("SELECT ID FROM Establecimientos WHERE Nombre = '%s'", input$edit_establecimiento))
    if (nrow(establecimiento_id) == 0) {
      showNotification("Establecimiento no encontrado.", type = "error", duration = 5)
      return()
    }
    establecimiento_id <- establecimiento_id$ID[1]
    
    proceso_id <- dbGetQuery(con, sprintf("SELECT ID FROM ProcesosGeneradores WHERE Nombre = '%s'", input$edit_proceso))
    if (nrow(proceso_id) == 0) {
      showNotification("Proceso Generador no encontrado.", type = "error", duration = 5)
      return()
    }
    proceso_id <- proceso_id$ID[1]
    
    residuo_id <- dbGetQuery(con, sprintf("SELECT ID FROM Residuos WHERE Residuo = '%s'", input$edit_residuo))
    if (nrow(residuo_id) == 0) {
      showNotification("Residuo no encontrado.", type = "error", duration = 5)
      return()
    }
    residuo_id <- residuo_id$ID[1]
    
    presentacion_id <- dbGetQuery(con, sprintf("SELECT ID FROM Presentaciones WHERE Nombre = '%s'", input$edit_presentacion))
    if (nrow(presentacion_id) == 0) {
      showNotification("Presentación no encontrada.", type = "error", duration = 5)
      return()
    }
    presentacion_id <- presentacion_id$ID[1]
    
    gestor_id <- dbGetQuery(con, sprintf("SELECT ID FROM Gestores WHERE Nombre = '%s'", input$edit_gestor))
    if (nrow(gestor_id) == 0) {
      showNotification("Gestor no encontrado.", type = "error", duration = 5)
      return()
    }
    gestor_id <- gestor_id$ID[1]
    
    mecanismo_id <- dbGetQuery(con, sprintf("SELECT ID FROM MecanismosEntrega WHERE Nombre = '%s'", input$edit_mecanismo))
    if (nrow(mecanismo_id) == 0) {
      showNotification("Mecanismo de Entrega no encontrado.", type = "error", duration = 5)
      return()
    }
    mecanismo_id <- mecanismo_id$ID[1]
    
    # Obtener el ID del registro a editar
    selected_row <- input$registros_rows_selected
    if (is.null(selected_row) || length(selected_row) == 0) {
      showNotification("No se ha seleccionado ningún registro.", type = "error", duration = 5)
      return()
    }
    registro_id <- dbGetQuery(con, sprintf("SELECT ID FROM Registros LIMIT 1 OFFSET %d", selected_row - 1))
    if (nrow(registro_id) == 0) {
      showNotification("Registro no encontrado.", type = "error", duration = 5)
      return()
    }
    registro_id <- registro_id$ID[1]
    
    # Obtener datos del residuo
    residuo_data <- edit_residuo_data()
    
    # Construir y ejecutar la consulta de actualización
    query <- sprintf(
      "UPDATE Registros SET CiudadID = %d, EstablecimientoID = %d, ProcesoGeneradorID = %d, ResiduoID = %d, Descripcion = '%s', TipoResiduo = '%s', GradoPeligrosidad = '%s', EstadoMateria = '%s', Cantidad = %f, PresentacionID = %d, GestorID = %d, MecanismoID = %d, Certificado = '%s', UpdatedAt = CURRENT_TIMESTAMP WHERE ID = %d",
      ciudad_id, establecimiento_id, proceso_id, residuo_id,
      residuo_data$Descripcion %||% "", residuo_data$TipoResiduo %||% "",
      residuo_data$GradoPeligrosidad %||% "", residuo_data$EstadoMateria %||% "",
      input$edit_cantidad, presentacion_id, gestor_id, mecanismo_id, input$edit_certificado,
      registro_id
    )
    tryCatch({
      dbExecute(con, query)
      output$edit_progress <- renderUI({ NULL })
      output$edit_msg <- renderUI({ NULL })
      showNotification("Registro editado correctamente", type = "message", duration = 3)
      later::later(function() { output$edit_msg <- renderUI({ NULL }) }, 2)
      removeModal()
      registros_trigger(registros_trigger() + 1)
    }, error = function(e) {
      showNotification(sprintf("Error al guardar cambios: %s", e$message), type = "error", duration = 5)
    })
  })

  observeEvent(input$borrar_registro_modal, {
    id <- dbGetQuery(con, sprintf("SELECT ID FROM Registros LIMIT 1 OFFSET %d", input$registros_rows_selected - 1))[[1]]
    dbExecute(con, sprintf("DELETE FROM Registros WHERE ID = %d", id))
    output$edit_msg <- renderUI({ NULL })
    showNotification("Registro borrado correctamente", type = "warning", duration = 3)
    later::later(function() { output$edit_msg <- renderUI({ NULL }) }, 2)
    removeModal()
    registros_trigger(registros_trigger() + 1)
  })
  
  # --- ACTUALIZACIÓN INMEDIATA DE DIMENSIONES ---
  
  observeEvent(input$agregar, {
    if (input$nuevo_valor != "") {
      table <- switch(input$dimension,
                      "Ciudades" = "Ciudades",
                      "Establecimientos" = "Establecimientos",
                      "ProcesosGeneradores" = "ProcesosGeneradores",
                      "Residuos" = "Residuos",
                      "Presentaciones" = "Presentaciones",
                      "Gestores" = "Gestores",
                      "MecanismosEntrega" = "MecanismosEntrega")
      column <- if (input$dimension == "Residuos") "Residuo" else "Nombre"
      dbExecute(con, sprintf("INSERT INTO %s (%s, CreatedAt, UpdatedAt) VALUES ('%s', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", table, column, input$nuevo_valor))
      updateTextInput(session, "nuevo_valor", value = "")
      dimensiones_trigger(dimensiones_trigger() + 1)
    }
  })

  observeEvent(input$borrar_dimension, {
    selected_row <- input$dimensiones_rows_selected
    if (length(selected_row)) {
      table <- switch(input$dimension,
                      "Ciudades" = "Ciudades",
                      "Establecimientos" = "Establecimientos",
                      "ProcesosGeneradores" = "ProcesosGeneradores",
                      "Residuos" = "Residuos",
                      "Presentaciones" = "SELECT ID, Nombre FROM Presentaciones",
                      "Gestores" = "Gestores",
                      "MecanismosEntrega" = "MecanismosEntrega")
      id_col <- "ID"
      id <- dbGetQuery(con, sprintf("SELECT ID FROM %s LIMIT 1 OFFSET %d", table, selected_row - 1))[[1]]
      dbExecute(con, sprintf("DELETE FROM %s WHERE %s = %d", table, id_col, id))
      dimensiones_trigger(dimensiones_trigger() + 1)
    }
  })
  
  # --- DASHBOARD ---

  # Dashboard (placeholder)
  output$dashboard <- renderUI({
    HTML("<h3>Dashboard en desarrollo</h3>")
  })

  dashboard_data <- reactive({
    query <- "
      SELECT r.FechaRegistro, r.Cantidad, r.TipoResiduo, r.GradoPeligrosidad, r.Certificado, 
            rs.Residuo, c.Nombre AS Ciudad, e.Nombre AS Establecimiento
      FROM Registros r
      LEFT JOIN Ciudades c ON r.CiudadID = c.ID
      LEFT JOIN Establecimientos e ON r.EstablecimientoID = e.ID
      LEFT JOIN Residuos rs ON r.ResiduoID = rs.ID
    "
    datos <- dbGetQuery(con, query)



  observe({
    if (input$main_tabs == "Dashboard" || is.null(input$main_tabs)) {
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
    
    # Aplicar Filtros
    if (!is.null(input$dashboard_fecha)) {
      datos <- datos[as.Date(datos$FechaRegistro) >= input$dashboard_fecha[1] & 
                    as.Date(datos$FechaRegistro) <= input$dashboard_fecha[2], ]
    }
    if (!is.null(input$dashboard_ciudad) && input$dashboard_ciudad != "Todas") {
      datos <- datos[datos$Ciudad == input$dashboard_ciudad, ]
    }
    if (!is.null(input$dashboard_establecimiento) && input$dashboard_establecimiento != "Todas") {
      datos <- datos[datos$Establecimiento == input$dashboard_establecimiento, ]
    }
    
    datos
  })

  # Pre-agregaciones para las gráficas
  dashboard_agg <- reactive({
    datos <- dashboard_data()
    if (nrow(datos) == 0) return(NULL)
    
    # Tendencia mensual de residuos generados
    
    trend_data <- datos
    trend_data$Mes <- as.Date(cut(as.Date(datos$FechaRegistro), "month"))
    trend_agg <- aggregate(Cantidad ~ Mes, data = trend_data, sum, na.rm = TRUE)
    trend_agg$Toneladas <- trend_agg$Cantidad / 1000
    
    # Pareto 
    
    pareto_agg <- aggregate(Cantidad ~ Residuo, data = datos, sum, na.rm = TRUE)
    pareto_agg <- pareto_agg[order(-pareto_agg$Cantidad), ]
    pareto_agg$Toneladas <- pareto_agg$Cantidad / 1000
    pareto_agg$Pct <- cumsum(pareto_agg$Toneladas) / sum(pareto_agg$Toneladas) * 100
    
    # Bar chart 
    bar_agg <- aggregate(Cantidad ~ TipoResiduo, data = datos, sum, na.rm = TRUE)
    bar_agg$Toneladas <- bar_agg$Cantidad / 1000 # Calcula las Toneladas
    bar_agg <- bar_agg[order(bar_agg$Toneladas), ] # Ordena por Toneladas de mayor a menor
    list(trend = trend_agg, pareto = pareto_agg, bar = bar_agg)

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
    total <- nrow(datos)
    pct <- if (total > 0) round(100 * sum(grepl("Residuo Peligroso", datos$TipoResiduo), na.rm = TRUE) / total, 1) else 0
    tags$div(class = "kpi-box", style = "background:#E74C3C;",
            tags$h4("% Residuos Peligrosos"),
            tags$h2(paste0(pct, "%"))
    )
  })

  output$kpi_no_peligrosos <- renderUI({
    datos <- dashboard_data()
    total <- nrow(datos)
    pct <- if (total > 0) round(100 * sum(grepl("Residuo No Peligroso", datos$TipoResiduo), na.rm = TRUE) / total, 1) else 0
    tags$div(class = "kpi-box", style = "background:#27AE60;",
            tags$h4("% Residuos No Peligrosos"),
            tags$h2(paste0(pct, "%"))
    )
  })

  output$kpi_especiales <- renderUI({
    datos <- dashboard_data()
    total <- nrow(datos)
    pct <- if (total > 0) round(100 * sum(grepl("Residuo Especial", datos$TipoResiduo), na.rm = TRUE) / total, 1) else 0
    tags$div(class = "kpi-box", style = "background:#9B59B6;",
            tags$h4("% Residuos Especiales"),
            tags$h2(paste0(pct, "%"))
    )
  })

  # Trend Plot
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
                line = list(color = "#3498DB", width = 2),
                marker = list(color = "#3498DB", size = 6)) %>%
      layout(
        title = list(text = "Tendencia de Toneladas Generadas de Resoduos Mes a Mes", font = list(color = "#ECF0F1")),
        xaxis = list(title = "Año-Mes", titlefont = list(color = "#ECF0F1"), tickfont = list(color = "#ECF0F1"),
                    gridcolor = "#5D6D7E", zerolinecolor = "#5D6D7E", tickformat = "%Y-%m"),
        yaxis = list(title = "Toneladas", titlefont = list(color = "#ECF0F1"), tickfont = list(color = "#ECF0F1"),
                    gridcolor = "#5D6D7E", zerolinecolor = "#5D6D7E"),
        plot_bgcolor = "#34495E",
        paper_bgcolor = "#2C3E50",
        margin = list(l = 50, r = 50, t = 50, b = 50),
        hovermode = "x unified",
        hoverlabel = list(font = list(color = "#FFFFFF"))
      )
    p
  })

  # Pareto Plot
  output$dashboard_pareto <- renderPlotly({
    agg <- dashboard_agg()$pareto
    if (is.null(agg) || nrow(agg) == 0 || all(is.na(agg$Residuo)) || all(is.na(agg$Toneladas))) {
      output$pareto_empty <- renderUI({
        tags$div(class = "alert alert-info", "No hay datos disponibles para el Pareto.")
      })
      return(NULL)
    }
    output$pareto_empty <- renderUI({ NULL })
    
    n <- length(unique(agg$Residuo))
    colors <- viridisLite::viridis(n) 
    
    p <- plot_ly() %>%
      add_bars(data = agg, x = ~reorder(Residuo, -Toneladas), y = ~Toneladas, name = "Toneladas",
              marker = list(color = colors)) %>%
      add_lines(x = ~reorder(Residuo, -Toneladas), y = ~Pct, name = "% Acumulado", yaxis = "y2",
                line = list(color = "#E74C3C", width = 2, dash = "dash"),
                marker = list(color = "#E74C3C", size = 6)) %>%
      layout(
        title = list(text = "Diagrama de Pareto por Residuos Generados", font = list(color = "#ECF0F1")),
        xaxis = list(title = "", titlefont = list(color = "#ECF0F1", size = 10), 
                    tickfont = list(color = "#ECF0F1", size = 8), 
                    gridcolor = "#5D6D7E", zerolinecolor = "#5D6D7E"),
        yaxis = list(title = "Toneladas", titlefont = list(color = "#ECF0F1", size = 10),
                    tickfont = list(color = "#ECF0F1", size = 10),
                    gridcolor = "#5D6D7E", zerolinecolor = "#5D6D7E"),
        yaxis2 = list(title = "% Acumulado", overlaying = "y", side = "right",
                      titlefont = list(color = "#ECF0F1", size = 10),
                      tickfont = list(color = "#ECF0F1", size = 10),
                      range = c(0, 100), gridcolor = "#5D6D7E", zerolinecolor = "#5D6D7E"),
        plot_bgcolor = "#34495E",
        paper_bgcolor = "#2C3E50",
        margin = list(l = 50, r = 50, t = 50, b = 50),
        hovermode = "x unified",
        hoverlabel = list(font = list(color = "#FFFFFF")),
        legend = list(orientation = "h", yanchor = "bottom", y = -0.95, xanchor = "center", x = 0.5,
                      font = list(color = "#FFFFFF"), size = 10)
      )
    p
  })

  # Bar Chart por Tipo de Residuo

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
    
    p <- plot_ly(data = agg, y = ~TipoResiduo, x = ~Toneladas, type = "bar", #
                 marker = list(color = colors)) %>%
      add_trace(x = ~Toneladas, y = ~TipoResiduo, type = 'bar', 
                text = ~paste(round(Toneladas, 2), "Ton"), 
                textposition = 'outside', 
                textfont = list(color = '#ECF0F1', size = 10),
                cliponaxis = FALSE, 
                hoverinfo = 'none') %>% 
      layout(
        title = list(text = "Toneladas de Residuo Generados por Tipo de Residuo", font = list(color = "#ECF0F1")),
        yaxis = list(title = "Tipo de Residuo", titlefont = list(color = "#ECF0F1", size = 10),
                     tickfont = list(color = "#ECF0F1", size = 8),
                     gridcolor = "#5D6D7E", zerolinecolor = "#5D6D7E"),
        xaxis = list(title = "Toneladas", titlefont = list(color = "#ECF0F1", size = 10),
                     tickfont = list(color = "#ECF0F1", size = 8),
                     gridcolor = "#5D6D7E", zerolinecolor = "#5D6D7E"),
        plot_bgcolor = "#34495E",
        paper_bgcolor = "#2C3E50",
        margin = list(l = 100, r = 80, t = 50, b = 50),
        hovermode = "y unified",
        hoverlabel = list(font = list(color = "#FFFFFF")),
        showlegend = FALSE
      )
    p
  })

  
}


# Disconectar de la base de datos al cerrar la aplicación

onStop(function() {
  dbDisconnect(con)
})

# Ejecutar la aplicación Shiny

shinyApp(ui, server)