library(tidyverse)
library(data.table)
library(arrow)
library(tidytext)
library(tidylo)
library(here)

viz_dir <- here("visualizations")
if (!dir.exists(viz_dir)) {
  dir.create(viz_dir) 
  message("Created visualizations directory")  } else {
  message("Visualizations directory already exists")}

corpus <- "U.S. Congressional Records"

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
ling_unit <- "NOUN" # or "Words"

for (cat in cat_list) {
  message(paste("Processing topic:", cat))
  
  plot_title <- paste0(corpus, ": ", str_replace_all(cat, "_", " "), " for ", target_decade)
  png_title <- str_replace_all(plot_title, "[[:punct:]\\s]+", "_")
  
  all_decades_scores <- list()
  
  for (target_decade in decades) {
    message(paste0("Procesing decade: ", target_decade))
    
    data <- read_parquet(file.path("data", "gender_analysis", "categories",
                                   paste0(cat, "_", target_decade ,"_congress_filtered_by_gender.parquet"))) %>%
      mutate(decade = target_decade, topic = cat)
    
    if (ling_unit != "Words") {
      counts <- data %>%
        filter(pos == ling_unit) %>%
        count(decade, gender, token, topic, pos, sort = TRUE) } else {
      counts <- data %>%
        count(decade, gender, token, topic, sort = TRUE) }
    
    log_likelihood <- counts %>%
      bind_log_odds(set = gender, feature = token, n = n)
    
    top_log_likelihood <- log_likelihood %>%
      group_by(topic, decade, gender) %>%
      slice_max(order_by = log_odds_weighted, n = top_n, with_ties = FALSE) %>%
      ungroup()
    
    all_decades_scores[[as.character(target_decade)]] <- top_log_likelihood
    
    rm(data, counts, log_likelihood, top_log_likelihood)
    gc() }
  
  topic_df <- bind_rows(all_decades_scores) %>%
    mutate(topic_label = str_replace_all(topic, "_", " "),
           gender = factor(gender, levels = c("F", "M")),
           decade = factor(decade, levels = sort(unique(decade))))
  
  p <- ggplot(topic_df,
              aes(x = reorder_within(token, log_odds_weighted, interaction(decade, gender)),
                  y = log_odds_weighted,
                  fill = gender)) +
    geom_col(show.legend = FALSE) +
    facet_grid(decade ~ gender, scales = "free_y") +
    scale_x_reordered() +
    coord_flip() +
    labs(title = paste0("Top ", str_to_title(ling_unit), "s by Gender Measured with Log Likelihood"),
         subtitle = plot_title,
         x = "Token",
         y = "Weighted Log Odds") +
    theme(strip.text = element_text(size = 10))
  
  ggsave(filename = file.path(viz_dir, paste0(png_title, ".png")),
         plot = p,
         width = 24,
         height = 30,
         dpi = 800) }








