### VISUAL CREADA PARA POWER BI

# The following code to create a dataframe and remove duplicated rows is always executed and acts as a preamble for your script: 

# dataset <- data.frame(COMMENTS TRANSLATED)
# dataset <- unique(dataset)


options(stringsAsFactors = F)         # no automatic data transformation
options("scipen" = 100, "digits" = 4) # suppress math annotation
# load packages
library(knitr) 
library(kableExtra) 
library(DT)
library(tm)
library(topicmodels)
library(reshape2)
library(ggplot2)
library(wordcloud)
library(pals)
library(SnowballC)
library(lda)
library(ldatuning)
library(flextable)
library(corpora)
library(vader)
library(dplyr)
library(stringr)
library(readr)
library(scales)
library(ggrepel)
# activate klippy for copy-to-clipboard button
#klippy::klippy()



getSentiment <- function(x)
{
  return(tryCatch(vader::get_vader(x, incl_nt = T, neu_set = T)[["compound"]], error=function(e) 0))
}

#&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
# Data pre-processing 
#&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
dataset$doc_id <- 1:nrow(dataset)
names(dataset) <- c("text" , "CASE ID", "WEEK", "START OF WEEK", "doc_id" )

#build corpus
corpus <- tm::Corpus(tm::DataframeSource(dataset))

#clean corpus
corpus <- tm::tm_map(corpus, tm::removePunctuation)
corpus <- tm::tm_map(corpus, tolower)
corpus <- tm::tm_map(corpus, tm::removeWords, tm::stopwords("english"))
corpus <- tm::tm_map(corpus, tm::removeWords, c("stripe", "$", "€", "will", "guy", "seem", "one", "tri", "say", "cant", "took", "yet", "didnt", "also", "put", "say", "didn't", "see", "never", "got", "gave", "doesn't", "talk", "told", "tell", "a", "about", "above", "across", "after", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "among", "an", "and", "another", "any", "anybody", "anyone", "anything", "anywhere", "are", "area", "areas", "around", "as", "ask", "asked", "asking", "asks", "at", "away", "b", "back", "backed", "backing", "backs", "be", "became", "because", "become", "becomes", "been", "before", "began", "behind", "being", "beings", "best", "better", "between", "big", "both", "but", "by", "c", "came", "can", "cannot", "case", "cases", "certain", "certainly", "clear", "clearly", "come", "could", "d", "did", "differ", "different", "differently", "do", "does", "done", "down", "downed", "downing", "downs", "during", "e", "each", "early", "either", "end", "ended", "ending", "ends", "enough", "even", "evenly", "ever", "every", "everybody", "everyone", "everything", "everywhere", "f", "face", "faces", "fact", "facts", "far", "felt", "few", "find", "finds", "first", "for", "four", "from", "full", "fully", "further", "furthered", "furthering", "furthers", "g", "gave", "general", "generally", "get", "gets", "give", "given", "gives", "go", "going", "good", "goods", "got", "great", "greater", "greatest", "group", "grouped", "grouping", "groups", "h", "had", "has", "have", "having", "he", "her", "here", "herself", "high", "higher", "highest", "him", "himself", "his", "how", "however", "i", "if", "important", "in", "interest", "interested", "interesting", "interests", "into", "is", "it", "its", "itself", "j", "just", "k", "keep", "keeps", "kind", "knew", "know", "known", "knows", "l", "large", "largely", "last", "later", "latest", "least", "less", "let", "lets", "like", "likely", "long", "longer", "longest", "m", "made", "make", "making", "man", "many", "may", "me", "member", "members", "men", "might", "more", "most", "mostly", "mr", "mrs", "much", "must", "my", "myself", "n", "necessary", "need", "needed", "needing", "needs", "never", "new", "newer", "newest", "next", "no", "nobody", "non", "noone", "not", "nothing", "now", "nowhere", "number", "numbers", "o", "of", "off", "often", "old", "older", "oldest", "on", "once", "one", "only", "open", "opened", "opening", "opens", "or", "order", "ordered", "ordering", "orders", "other", "others", "our", "out", "over", "p", "part", "parted", "parting", "parts", "per", "perhaps", "place", "places", "point", "pointed", "pointing", "points", "possible", "present", "presented", "presenting", "presents", "problem", "problems", "put", "puts", "q", "quite", "r", "rather", "really", "right", "room", "rooms", "s", "said", "same", "saw", "say", "says", "second", "seconds", "see", "seem", "seemed", "seeming", "seems", "sees", "several", "shall", "she", "should", "show", "showed", "showing", "shows", "side", "sides", "since", "small", "smaller", "smallest", "so", "some", "somebody", "someone", "something", "somewhere", "state", "states", "still", "such", "sure", "t", "take", "taken", "than", "that", "the", "their", "them", "then", "there", "therefore", "these", "they", "thing", "things", "think", "thinks", "this", "those", "though", "thought", "thoughts", "three", "through", "thus", "to", "today", "together", "too", "took", "toward", "turn", "turned", "turning", "turns", "two", "u", "under", "until", "up", "upon", "us", "use", "used", "uses", "v", "very", "w", "want", "wanted", "wanting", "wants", "was", "way", "ways", "we", "well", "wells", "went", "were", "what", "when", "where", "whether", "which", "while", "who", "whole", "whose", "why", "will", "with", "within", "without", "work", "worked", "working", "works", "would", "x", "y", "year", "years", "yet", "you", "young", "younger", "youngest", "your", "yours", "z", "yes", "no", "dont", "day"))
corpus <- tm::tm_map(corpus, tm::stemDocument)

#build document term matrix
dtm <- tm::DocumentTermMatrix(corpus, control = list(stopwords = TRUE))

rowTotals <- apply(dtm , 1, sum) #Find the sum of words in each Document
dtm   <- dtm[rowTotals> 0, ] 



# due to vocabulary pruning, we have empty rows in our DTM
# LDA does not like this. So we remove those docs from the
# DTM and the metadata
sel_idx <- slam::row_sums(dtm) > 0
dtm <- dtm[sel_idx, ]
dataset <- dataset[sel_idx, ]



################################################################################

# number of topics
K <- 20
# set random number generator seed
set.seed(9161)
# compute the LDA model, inference via 1000 iterations of Gibbs sampling
topicModel <- LDA(dtm, K, method="Gibbs", control=list(iter = 500, verbose = 25))

topicData <- data.frame(doc_id = topicModel@documents, Topic =topicmodels::topics(topicModel))
terms <- as.data.frame(topicmodels::terms(topicModel,5))
termsList <- apply(terms, MARGIN = 2, FUN = function(x) paste0(x, sep = " :: ", collapse = ""))
termsDf <- data.frame(Topic = 1:length(termsList), Terms = termsList)

#&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
# Sentiment analysis using VADER
#&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
dataset$sentimentScore <- lapply(X = dataset$text, function(x) getSentiment(x))
dataset$sentiment <- ifelse(dataset$sentimentScore <= -0.2, "Negative", ifelse(dataset$sentimentScore >= 0.2, "Positive", "Neutral"))
dataset <- merge(x = dataset, y = topicData, by = c("doc_id"), all.x = TRUE)


#&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
# Plots
#&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
df0 <- dataset %>% group_by(WEEK, `START OF WEEK`) %>% 
  count(sentiment)

surveys_week <- df0 %>% group_by(WEEK, `START OF WEEK`) %>% 
  summarise(Surveys=sum(n))

positives_week <- filter(df0,sentiment =="Positive")


df <- left_join(
  surveys_week,
  positives_week,
  by = NULL,
  copy = FALSE,
  keep = NULL
)

names(df) <- c("WEEK", "START OF WEEK", "SURVEYS" ,"SENTIMENT", "POSITIVES" )
df$POSITIVES[is.na(df$POSITIVES)] <- 0


df$PERC <- round((df$POSITIVES / df$SURVEYS)*100,1)
df$WEEK2 <- as.Date.character(df$`START OF WEEK`)


POSITIVE_AVG_WEEK <- round(mean(df$PERC),1)

plotOutput <- ggplot2::ggplot(data = df, 
                              ggplot2::aes(x = WEEK, y = PERC)) +
  ggplot2::geom_bar(stat = "identity", fill="#11EFE3") + 
  ggplot2::geom_text(ggplot2::aes(label=paste(WEEK," : ",PERC,"%")),color="black",hjust=0, vjust = 0.3, nudge_y = 0,angle=90) +
  ggplot2::labs(x = "WEEK", y = "POSITIVE PERCEPTION (%)") +
  ggplot2::theme(axis.text = ggplot2::element_text(size=12),
                 axis.title = ggplot2::element_text(size=12,face = "bold"))+geom_hline(yintercept = POSITIVE_AVG_WEEK,color="red", linetype=2)+
                 annotate("text", x=max(df$WEEK)-1, y=150, label=paste("AVG:",POSITIVE_AVG_WEEK,"%"), size=7, color="blue")

#render plot onto PBI canvas
plotOutput
