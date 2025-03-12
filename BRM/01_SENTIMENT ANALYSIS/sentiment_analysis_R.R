# Required libraries
library(tidyverse)
library(tidytext)
library(text2vec)
library(caret)
library(textdata)
library(ggplot2)
library(randomForest)

# Read the data
df <- read.csv( "https://raw.githubusercontent.com/ringoquimico/Portfolio/refs/heads/main/Data%20Sources/call_center_data.csv", 
               sep = ';', 
               encoding = 'UTF-8')

# Data preprocessing
df_clean <- df %>%
  select(translated_comments, sentiment_rate) %>%
  na.omit()

# Create document-term matrix
it_train <- itoken(df_clean$translated_comments, 
                  preprocessor = tolower, 
                  tokenizer = word_tokenizer)

vocab <- create_vocabulary(it_train)
vectorizer <- vocab_vectorizer(vocab)
dtm <- create_dtm(it_train, vectorizer)

# Split the data 80-20
set.seed(123)
train_index <- createDataPartition(df_clean$sentiment_rate, p = 0.8, list = FALSE)
train_data <- dtm[train_index, ]
test_data <- dtm[-train_index, ]
train_labels <- df_clean$sentiment_rate[train_index]
test_labels <- df_clean$sentiment_rate[-train_index]

# Train Random Forest model
rf_model <- randomForest(x = as.matrix(train_data), 
                        y = as.factor(train_labels),
                        ntree = 100)

# Make predictions
predictions <- predict(rf_model, newdata = as.matrix(test_data))

# Calculate accuracy
accuracy <- mean(predictions == test_labels)
print(paste("Model Accuracy:", round(accuracy, 4)))

# Create confusion matrix
conf_matrix <- confusionMatrix(predictions, as.factor(test_labels))
print(conf_matrix)

# Visualization
results_df <- data.frame(
  Set = c(rep("Training", length(train_labels)), rep("Testing", length(test_labels))),
  Actual = c(train_labels, test_labels),
  Predicted = c(predict(rf_model, newdata = as.matrix(train_data)), predictions)
)

# Plot comparison
ggplot(results_df, aes(x = Actual, fill = Set)) +
  geom_bar(position = "dodge") +
  facet_wrap(~Set) +
  theme_minimal() +
  labs(title = "Comparison of Actual vs Predicted Sentiment Ratings",
       x = "Sentiment Rating",
       y = "Count") +
  scale_fill_brewer(palette = "Set2")

# Save the model
saveRDS(rf_model, "sentiment_model.rds")


