# read in the bound data to do TF-IDF

# read R data 

# for(d in decade) {}
corpus <- "U.S. Congressional Records"
ngram_length <- 1
top_n <- 20


tidy_ngrams <- tidy_ngrams %>%
  select(gender, token) %>%
  separate(token, into = paste0("word", 1:ngram_length), sep = " ") %>%
  filter(if_all(starts_with("word"), ~ !tolower(.x) %in% stop_words$word)) %>%
  filter(if_all(starts_with("word"), ~ !str_detect(.x, "^[[:punct:]]+$"))) %>%
  unite(token, starts_with("word"), sep = " ") %>%
  count(gender, token, sort = TRUE) %>%
  group_by(gender, token) %>%
  summarise(n = sum(n), .groups = "drop")

s <- paste0(corpus, " ", ngram_length, "-Grams for ", d)
#save(tidy_ngrams, file = paste0(s, ".RData"))

print(paste0("Applying TF-IDF ", d))
tf_idf <- tidy_ngrams %>%
  bind_tf_idf(term = token, document = gender, n = n)

top_tf_idf <- tf_idf %>%
    filter(!is.na(tf_idf), tf_idf > 0) %>%
    group_by(gender) %>%
    slice_max(tf_idf, n = top_n, with_ties = FALSE) %>%
    ungroup()
  
  print(paste0("Visualizing ", d))
  plot <- ggplot(top_tf_idf,
                 aes(x = reorder_within(token, tf_idf, gender), 
                     y = tf_idf, 
                     fill = gender)) +
    geom_col(show.legend = FALSE) +
    facet_wrap(~ gender, scales = "free") +
    scale_x_reordered() +
    coord_flip() +
    labs(title = "Top TF-IDF Words by Gender",
         subtitle = paste0("In the ", s),
         x = "Word",
         y = "TF-IDF")
  
ggsave(paste0(s, ".png"), plot = plot, width = 10, height = 6, dpi = 300)