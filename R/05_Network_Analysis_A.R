### VISUALIZACIÓN CREADA PARA POWER BI

# The following code to create a dataframe and remove duplicated rows is always executed and acts as a preamble for your script: 

# dataset <- data.frame(NODE, GROUP, OCCURRENCES)
# dataset <- unique(dataset)

# Paste or type your script code here:
# activate packages
library(flextable)
library(GGally)
library(ggraph)
library(gutenbergr)
library(igraph)
library(Matrix)
library(network)
library(quanteda)
library(sna)
library(tidygraph)
library(tidyverse)
library(tm)
library(tibble)
library(dplyr)
library(reshape2)
# activate klippy for copy-to-clipboard button

df <- dataset

#df <- filter(df,df$STEP <=10)

df <- df[1:2]

net_0 <- crossprod(table(df[1:2]))
diag(net_0) <- 0
net_f <- as.data.frame(net_0)

net_dfm <- quanteda::as.dfm(net_f)
# create feature co-occurrence matrix
net_fcm <- quanteda::fcm(net_dfm)
# inspect data
head(net_fcm)

quanteda.textplots::textplot_network(net_fcm, 
                                     min_freq = .5, 
                                     edge_alpha = 0.5, 
                                     edge_color = "#635bff",
                                     vertex_labelsize = log(rowSums(net_fcm))*0.8,
                                     edge_size = 1.5)