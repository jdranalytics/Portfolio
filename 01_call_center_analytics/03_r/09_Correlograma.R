### VISUALIZACIÓN CREADA PARA POWER BI

# The following code to create a dataframe and remove duplicated rows is always executed and acts as a preamble for your script: 

# dataset <- data.frame(WEEK, % CSAT (STRIPE), TIME TO RESOLVE (m), MERGES, REOPENS, ASSIGNEES, SOLVES, TOUCHES, REPLIED CASES, REJECTED MERCHANT, RESOLVED IN SLA, AHT OUT OF TARGET, RESOLUTION GROUPS, CASE TOUCHED BY T2)
# dataset <- unique(dataset)

# Paste or type your script code here:


library(ggplot2)
library(corrplot)

dataset <- dataset[-c(1)]

RAC.cor <- cor(dataset,method = "pearson")
round(RAC.cor, digits = 2)
corrplot(RAC.cor)

colx <- colorRampPalette(c("#BB4444","#EE9988","#FFFFFF","#4477AA","#203864"))


corrplot(RAC.cor,  shade.col = NA, tl.col = "black",
         tl.srt = 45, col=colx(400), addCoef.col = "black", number.cex = 1.5, order = "AOE", addgrid.col = NA) 

