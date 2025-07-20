library(tidyverse)
library(data.table)
library(arrow)
library(tidytext)
library(tidylo)
library(here)

viz_dir <- here("visualizations")
if (!dir.exists(viz_dir)) {
  dir.create(viz_dir) 
  message("Created visualizations directory") } else {
    message("Visualizations directory already exists") }

corpus <- "U.S. Congressional Records"

cat_a <- "Civil_and_Constitutional_Rights"
cat_b <- "Commerce_Economy_and_Labor"
cat_c <- "Crime_and_Law_Enforcement"
cat_d <- "Defense_Military_and_International_Affairs"
cat_e <- "Education_Culture_Science_and_Arts"
cat_f <- "Energy_Transportation_and_Technology"
cat_g <- "Environment_Agriculture_and_Public_Land"
cat_h <- "Governmental_Relations"
cat_i <- "Health_and_Families"
cat_j <- "Immigration"
cat_k <- "Social_Welfare_and_Housing"
cat_l <- "Sports_and_Recreation"

cat <- cat_d

cat_list <- c("Civil_and_Constitutional_Rights",
              "Commerce_Economy_and_Labor", 
              "Crime_and_Law_Enforcement",
              "Defense_Military_and_International_Affairs", 
              "Education_Culture_Science_and_Arts", 
              "Energy_Transportation_and_Technology",
              "Environment_Agriculture_and_Public_Land", 
              "Governmental_Relations", 
              "Health_and_Families", 
              "Immigration",
              "Social_Welfare_and_Housing", 
              "Sports_and_Recreation")


top_n <- 30
decades <- c(1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020)
ling_unit <- "NOUN" # WORDS
#target_decade <- 1960

for(target_decade in decades) {
  
  for(cat in cat_list) {
  
  plot_title <- paste0(corpus, ": ", str_replace_all(cat, "_", " "), " for ", target_decade)
  png_title <- str_replace_all(plot_title, "[[:punct:]\\s]+", "_")
  
  data <- read_parquet(file.path("data", "gender_analysis", "categories", paste0(cat, "_", target_decade ,"_congress_filtered_by_gender.parquet")))
  

  if(any(ling_unit != "Words")) {
    counts <- data %>%
      select(gender, token, pos) %>%
      count(gender, token, pos, sort = TRUE) %>%
      filter(pos == ling_unit) } else {
    counts <- data %>%
      select(gender, token) %>%
      count(gender, token, sort = TRUE) }
  
  log_likelihood <- counts %>%
    bind_log_odds(set = gender, feature = token, n = n)
  
  top_log_likelihood <- log_likelihood %>%
    group_by(gender) %>%
    slice_max(log_odds_weighted, n = top_n, with_ties = FALSE) %>%
    ungroup()
  
  p <- ggplot(top_log_likelihood,
         aes(x = reorder_within(token, log_odds_weighted, gender), 
             y = log_odds_weighted, 
             fill = gender)) +
    geom_col(show.legend = FALSE) +
    facet_wrap(~ gender, scales = "free") +
    scale_x_reordered() +
    coord_flip() +
    labs(title = paste0("Top ", str_to_title(ling_unit), "s by Gender Measured with Log Likelihood"),
         subtitle = plot_title,
         x = "Word",
         y = "Weighted Log Odds") 
  
  ggsave(filename = file.path(viz_dir, paste0(png_title, ".png")),
         plot = p,
         width = 10,
         height = 6,
         dpi = 300) 
  
  } } 



#tf_idf <- counts %>%
#  bind_tf_idf(term = token, document = gender, n = n)

#top_tf_idf <- tf_idf %>%
#    filter(!is.na(tf_idf), tf_idf > 0) %>%
#    group_by(gender) %>%
#    slice_max(tf_idf, n = top_n, with_ties = FALSE) %>%
#    ungroup()

#ggplot(top_tf_idf,
#       aes(x = reorder_within(token, tf_idf, gender), 
#           y = tf_idf, 
#           fill = gender)) +
#  geom_col(show.legend = FALSE) +
#  facet_wrap(~ gender, scales = "free") +
#  scale_x_reordered() +
#  coord_flip() +
#  labs(title = "Top TF-IDF Words by Gender",
#       subtitle = paste0("In the "),
#       x = "Word",
#       y = "TF-IDF")
    
    
