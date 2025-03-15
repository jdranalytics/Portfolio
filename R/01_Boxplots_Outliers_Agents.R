#VISUALIZACIÓN PREPARADA PARA USAR EN POWER BI

# The following code to create a dataframe and remove duplicated rows is always executed and acts as a preamble for your script: 

# dataset <- data.frame(MARKET AGENT, CSAT_Q, Agent_Name)
# dataset <- unique(dataset)

# Paste or type your script code here:

set.seed(123)


library(tidyverse)
library(ggstatsplot)
library(ggrepel)
library(ggplot2)
library(dplyr)



dataset$CSAT_Q <- round(dataset$CSAT_Q*100,digits = 1)
dataset <- dataset %>% rename('MARKET' = `MARKET AGENT`)

head(dataset)

CSAT <- as.vector(dataset$CSAT_Q)
MARKET <- as.vector(dataset$MARKET)
AGENT <- as.vector(dataset$Agent_Name)

data <- c(CSAT,AGENT,MARKET)

print(data)

set.seed(123)


# Definir una función para etiquetar outliers
identify_outliers <- function(df, column) {
  Q1 <- quantile(df[[column]], 0.25, na.rm = TRUE)
  Q3 <- quantile(df[[column]], 0.75, na.rm = TRUE)
  IQR <- Q3 - Q1
  lower_bound <- Q1 - 1.5 * IQR
  upper_bound <- Q3 + 1.5 * IQR
  df <- df %>%
    mutate(outlier = ifelse((df[[column]] < lower_bound | df[[column]] > upper_bound), TRUE, FALSE))
  return(df)
}

dataset2 <- identify_outliers(dataset, "CSAT_Q")

# Ver los primeros registros del dataframe para comprobar la nueva columna 'outlier'


# Crear un gráfico de cajas con los outliers resaltados
ggplot(dataset2, aes(x = MARKET, y = CSAT_Q)) +
  geom_boxplot() + theme_grey()+
  geom_point(aes(color = outlier), size = 3) +
  stat_summary(fun = mean, geom = "point", shape = 5, size = 3, color = "blue", fill = "blue") +  # Añadir la media
  stat_summary(fun = median, geom = "point", shape = 5, size = 3, color = "red", fill = "red") +  # Añadir la mediana
  scale_color_manual(values = c("#11efe3", "red")) +
  labs(
    x = "MARKET",
    y = "CSAT SCORE (%)",
    color = "Outlier"
  ) +   geom_text_repel(aes(label = ifelse(outlier, Agent_Name,"")), 
                  hjust = -0.1, vjust = 0.1, size = 4, color = "red")  +

  theme(legend.position="bottom")





