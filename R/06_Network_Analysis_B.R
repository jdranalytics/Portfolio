# VISUALIZACIÓN CREADA PARA POWER BI

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
#klippy::klippy()

df <- dataset

#df <- filter(df,df$STEP <=10)

df <- df[1:2]

net_0 <- crossprod(table(df[1:2]))
diag(net_0) <- 0
net_f <- as.data.frame(net_0)

va <- net_f %>%
  dplyr::mutate(GROUPS = rownames(.),
                Occurrences = rowSums(.)) %>%
  dplyr::select(GROUPS, Occurrences) %>%
  dplyr::filter(!str_detect(GROUPS, "CASE"))



top <- head(va,1)
bottom <- tail(va,1)

top <- c(top,2)
top <- as.character(top[1])

bottom <- c(bottom,2)
bottom <- as.character(bottom[1])

ed <- net_f %>%
  dplyr::mutate(from = rownames(.)) %>%
  tidyr::gather(to, Frequency, (top):(bottom)) %>%
  dplyr::mutate(Frequency = ifelse(Frequency == 0, NA, Frequency))

ig <- igraph::graph_from_data_frame(d=ed, vertices=va, directed = FALSE)

tg <- tidygraph::as_tbl_graph(ig) %>% 
  tidygraph::activate(nodes) %>% 
  dplyr::mutate(label=name)


v.size <- V(tg)$Occurrences
# inspect
v.size <- v.size/2



E(tg)$weight <- E(tg)$Frequency
# inspect weights
head(E(tg)$weight, 10)

# set seed
set.seed(12345)
# edge size shows frequency of co-occurrence
tg %>%
  ggraph(layout = "fr") +
  geom_edge_arc(colour= "#11efe3",
                lineend = "round",
                strength = .1,
                aes(edge_width = weight,
                    alpha = weight)) +
  geom_node_point(colour= "#635bff", size=log(v.size)*2) +
  geom_node_text(aes(label = name), 
                 repel = TRUE, 
                 point.padding = unit(0.05, "lines"), 
                 size=sqrt(v.size), 
                 colour="gray40") +
  scale_edge_width(range = c(0, 2.5)) +
  scale_edge_alpha(range = c(0, .3)) +
  theme_graph(background = "white") +
  theme(legend.position = "top") +
  guides(edge_width = FALSE,
         edge_alpha = FALSE)