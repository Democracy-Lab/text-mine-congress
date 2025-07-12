library(tidyverse)
library(data.table)
library(arrow)
library(tidytext)
library(tidylo)

#data("stop_words")
#decades <- c(1870, 1880, 1890, 1900, 1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020)

# ###############################
# ## NOT RUN
# ## Processing Pipeline for Creating Gender Subsets
# ###############################
# 
# wd <- getwd()
# data_dir <- file.path(wd, "data")
# 
# gender_data_dir_name <- file.path("data", "gender_analysis")
# dir.create(gender_data_dir_name)
# 
# for(d in decades) {
#   
#   parsed_file_name <- paste0("us_congress_spacy_parsed_", d, ".parquet")
#   record_file_name <- paste0("us_congress_spacy_ids_", d,".parquet")
#   
#   parsed_decade_subset <- read_parquet(file.path(data_dir, parsed_file_name)) %>%
#     mutate(source_file = str_replace(source_file, "spacy_parsed_", ""))
#   
#   record_decade_subset <- read_parquet(file.path(data_dir, record_file_name)) %>%
#     select(source_file, doc_id, decade, gender, state, party, title)
#   
#   joined_subset <- parsed_decade_subset %>%
#     left_join(record_decade_subset, by = c("source_file", "doc_id"))
#   
#   men <- joined_subset %>%
#     filter(gender == "M") %>%
#     select(sentence_id, token_id, token, pos, gender, state, party, title)
#   
#   write_parquet(men, 
#                 sink = file.path("data", "gender_analysis", paste0("us_congress_men_", d, ".parquet")))
#   
#   women <- joined_subset %>%
#     filter(gender == "F") %>%
#     select(sentence_id, token_id, token, pos, gender, state, party, title) 
#   
#   write_parquet(women, 
#                 sink = file.path("data", "gender_analysis", paste0("us_congress_women_", d, ".parquet"))) }



################ Work Station


# Things to edit: 
target_decade <- 2000
top_n <- 20
######################

# greater or greatest
view_stop_words <- as.data.frame(stop_words)

rm(view_stop_words)

file_name <- paste0("us_congress_men_and_women_clean_", target_decade, ".parquet")
data <- read_parquet(file.path("data", "gender_analysis", file_name)) 

adj_noun <- data %>%
  filter(pos == "ADJ", dep_rel %in% c("amod")) %>%
  left_join(parsed_df, by = c("doc_id", "head_token_id" = "token_id"), suffix = c("_adj", "_noun")) %>%
  filter(pos_noun %in% c("NOUN", "PROPN")) %>%
  transmute(doc_id, 
            phrase = paste(token_adj, token_noun), 
            adjective = token_adj, 
            noun = token_noun)


# Exact match 
#climate_data <- data %>%
#  filter(str_detect(title, fixed("rain")))


# Regex match on multiple words: 
target_words <- c("army", "war", "defence", "military", "\\bbase\\b")
pattern <- paste(target_words, collapse = "|")  
counts <- data %>%
  filter(str_detect(title, regex(pattern, ignore_case = TRUE))) %>%
  count(gender, token, sort = TRUE)

#counts <- data %>%
#  count(gender, token, sort = TRUE) # change to variables of interest

# Examples
#counts <- data %>%
#  count(gender, party, token, sort = TRUE)

#counts <- data %>%
#  filter(pos == "NOUN") %>%
#  count(gender, token, sort = TRUE)

counts <- data %>%
  filter(pos == "VERB") %>%
  filter(str_detect(title, fixed("war"))) %>%
  count(gender, party, token, sort = TRUE)


# TF-IDF 

tf_idf <- counts %>%
  bind_tf_idf(term = token, document = gender, n = n)

top_tf_idf <- tf_idf %>%
  filter(!is.na(tf_idf), tf_idf > 0) %>%
  group_by(gender) %>%
  slice_max(tf_idf, n = top_n, with_ties = FALSE) %>%
  ungroup()

# must assign to ' plot <-  ' to save this: 

ggplot(top_tf_idf,
               aes(x = reorder_within(token, tf_idf, gender), 
                   y = tf_idf, 
                   fill = gender)) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~ gender, scales = "free") +
  scale_x_reordered() +
  coord_flip() +
  labs(title = "Top TF-IDF Words by Gender",
       subtitle = paste0("In the U.S. Congressional Record, ", target_decade),
       x = "Word",
       y = "TF-IDF")

#output_dir <- file.path("data", "gender_analysis")
#ggsave(output_dir, paste0(target_decade, "_TFIDF.png"), plot = plot, width = 10, height = 6, dpi = 300)


# Log-Likelihood

log_likelihood <- counts %>%
  bind_log_odds(set = gender, feature = token, n = n)

top_log_likelihood <- log_likelihood %>%
  group_by(gender) %>%
  slice_max(log_odds_weighted, n = top_n, with_ties = FALSE) %>%
  ungroup()

ggplot(top_log_likelihood,
               aes(x = reorder_within(token, log_odds_weighted, gender), 
                   y = log_odds_weighted, 
                   fill = gender)) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~ gender, scales = "free") +
  scale_x_reordered() +
  coord_flip() +
  labs(title = "Top Log-Likelihood Words by Gender",
       subtitle = paste0("In the U.S. Congressional Record, ", target_decade),
       x = "Word",
       y = "Weighted Log Odds")


#output_dir <- file.path("data", "gender_analysis")
#ggsave(output_dir, paste0(target_decade, "_LL.png"), plot = plot, width = 10, height = 6, dpi = 300)

