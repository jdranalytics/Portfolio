# The following code to create a dataframe and remove duplicated rows is always executed and acts as a preamble for your script: 

# dataset <- data.frame(text, doc_id)
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
library(readr)

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
names(dataset) <- c("text", "doc_id")

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

###############CÁLCULO DEL NÚMERO ÓPTIMO DE TÓPICOS############################

result <- ldatuning::FindTopicsNumber(
  dtm,
  topics = seq(from = 2, to = 20, by = 1),
  metrics = c("CaoJuan2009",  "Deveaud2014"),
  method = "Gibbs",
  control = list(seed = 77),
  verbose = TRUE
)

FindTopicsNumber_plot(result)

################################################################################

# number of topics
K <- 10
# set random number generator seed
set.seed(9161)
# compute the LDA model, inference via 1000 iterations of Gibbs sampling
topicModel <- LDA(dtm, K, method="Gibbs", control=list(iter = 500, verbose = 25))



# have a look a some of the results (posterior distributions)
tmResult <- posterior(topicModel)
# format of the resulting object
attributes(tmResult)

nTerms(dtm)              # lengthOfVocab

# topics are probability distributions over the entire vocabulary
beta <- tmResult$terms   # get beta from results
dim(beta)                # K distributions over nTerms(DTM) terms

rowSums(beta)            # rows in beta sum to 1

nDocs(dtm)               # size of collection

# for every document we have a probability distribution of its contained topics
theta <- tmResult$topics 
dim(theta)               # nDocs(DTM) distributions over K topics

rowSums(theta)[1:10]     # rows in theta sum to 1

terms(topicModel, 10)

exampleTermData <- terms(topicModel, 10)
exampleTermData[, 1:8]

top5termsPerTopic <- terms(topicModel, 5)
topicNames <- apply(top5termsPerTopic, 2, paste, collapse=" | ")


################################################################################

exampleIds <- c(1)
lapply(corpus[exampleIds], as.character)


############ TOPIC RANKING ##########################funciona###########################


#################### topic ranking Rank-1 how often a topic appears as a primary topic within a paragraph 


countsOfPrimaryTopics <- rep(0, K)
names(countsOfPrimaryTopics) <- topicNames
for (i in 1:nDocs(dtm)) {
  topicsPerDoc <- theta[i, ] # select topic distribution for document i
  # get first element position from ordered list
  primaryTopic <- order(topicsPerDoc, decreasing = TRUE)[1] 
  countsOfPrimaryTopics[primaryTopic] <- countsOfPrimaryTopics[primaryTopic] + 1
}
sort(countsOfPrimaryTopics, decreasing = TRUE)

so <- sort(countsOfPrimaryTopics, decreasing = TRUE)
paste(so, ":", names(so))

RANKING2 <- as.data.frame(paste(so, ":", names(so)))

library(stringr)

# Split name column into firstname and last name
RANKING2 <- RANKING2[c('Rank', 'Topic')] <- str_split_fixed(RANKING2$`paste(so, ":", names(so))`, ' : ', 2)
RANKING2 <- as.data.frame(RANKING2)
colnames(RANKING2) <- c("RANK", "TOPIC")

RANKING2 <- transform(RANKING2, RANK= as.numeric(RANKING2$RANK))

RANKING2$TOPIC <- factor(RANKING2$TOPIC,                                    
                        levels = RANKING2$TOPIC[order(RANKING2$RANK, decreasing = FALSE)])


plotOutput <- ggplot2::ggplot(data = RANKING2, 
                              ggplot2::aes(x = RANK, y = TOPIC )) +
  ggplot2::geom_bar(stat = "identity", fill="#11EFE3") + 
  ggplot2::geom_text(ggplot2::aes(label=RANK),color="black",position = ggplot2::position_stack(vjust = 1)) +
  ggplot2::labs( x = "RANK", y = "TOPIC TERMS") +
  ggplot2::theme(axis.text = ggplot2::element_text(size=14),
                 axis.title = ggplot2::element_text(size=12,face="bold"))

plotOutput