#VISUAL CREADA PARA POWER BI


# The following code to create a dataframe and remove duplicated rows is always executed and acts as a preamble for your script: 

# dataset <- data.frame(MARKET AGENT, CSAT_Q, Agent_Name)
# dataset <- unique(dataset)

# Paste or type your script code here:
# dataset <- unique(dataset)

# Paste or type your script code here:

set.seed(123)


library(tidyverse)
library(ggstatsplot)
library(ggrepel)
library(ggplot2)
library(dplyr)



dataset$CSAT_Q <- round(dataset$CSAT_Q*100,digits = 1)



CSAT <- as.vector(dataset$CSAT_Q)
AGENT <- as.vector(dataset$Agent_Name)

data <- c(CSAT,AGENT)



set.seed(123)


mean_csat <- round(mean(dataset$CSAT_Q),digits=1)
sd_csat <- round(sd(dataset$CSAT_Q),digits=1)
median_csat <- round(median(dataset$CSAT_Q),digits=1)

                   
# Definir los segmentos
df <- dataset %>%
  mutate(segmento = cut(CSAT_Q, breaks = seq(0, 100, by = 10), right = FALSE, include.lowest = TRUE))

# Agrupar los datos por segmentos y contar el número de agentes en cada segmento
df_segmentos <- df %>%
  group_by(segmento) %>%
  summarise(conteo = n())

# Crear el histograma agrupado
ggplot(df_segmentos, aes(x = segmento, y = conteo)) +
  geom_bar(stat = "identity", fill = "#11efe3", color = "black", alpha = 0.7) +
  labs(
       x = "CSAT Score (%)",
       y = "DENSITY") +
  theme_minimal() +
  geom_text(aes(label = conteo), vjust = -0.5, color = "black") + 
  geom_text(aes(label = conteo), vjust = -0.5, color = "black") 