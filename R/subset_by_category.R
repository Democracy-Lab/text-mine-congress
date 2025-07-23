keyword_at_least_n_per_doc <- function(collapsed_docs_df, pattern, n = 3) {
  collapsed_docs_df %>%
    mutate(
      matched_words = str_extract_all(sentence, regex(pattern, ignore_case = TRUE))
    ) %>%
    mutate(
      word_table = map(matched_words, function(x) table(tolower(x))),
      max_word_count = map_int(word_table, function(tbl) {
        if (length(tbl) == 0) {
          return(0)
        } else {
          return(max(tbl))
        }
      })
    ) %>%
    filter(max_word_count >= n) %>%
    mutate(
      matched_words = map_chr(word_table, function(tbl) {
        paste0("(", names(tbl), ", ", as.integer(tbl), ")", collapse = " ")
      }),
      keyword_total_count = map_int(word_table, function(tbl) sum(tbl))
    )
}


subset_by_category <- function(f, decade, categories_dir) {
  message(paste0("Processing data: ", decade))
  
  f <- f %>%
    mutate(global_token_id = row_number())
  
  metadata <- f %>%
    select(global_token_id, pos, gender, state, party, title)
  
  collapsed_f <- f %>%
    arrange(global_token_id) %>%
    group_by(doc_id) %>%
    summarise(
      global_token_id = list(global_token_id),
      token = list(token),
      sentence = str_c(unlist(token), collapse = " "),
      .groups = "drop"
    )
  
  url <- "https://raw.githubusercontent.com/stephbuon/text-mine-congress/refs/heads/main/analysis/congress_controlled_vocab.csv"
  
  keywords_df <- read_csv(url) %>%
    mutate(
      Keywords = str_split(Keywords, ",\\s*"),
      Keywords = map_chr(Keywords, function(x) {
        keyword_list <- str_trim(x)
        paste0("\\b", keyword_list, "\\b", collapse = "|")
      })
    )
  
  for (category in keywords_df$Category) {
    message(paste0("Subsetting by category: ", category))
    
    keyword_pattern <- keywords_df %>%
      filter(Category == category) %>%
      pull(Keywords) %>%
      first()
    
    matched_docs <- keyword_at_least_n_per_doc(collapsed_f, keyword_pattern, n = 3)
    
    if (nrow(matched_docs) == 0) {
      message(paste0("No documents met threshold for category: ", category))
      next
    }
    
    # Write debugger CSV (includes total count column)
    write_csv(
      matched_docs %>% select(doc_id, keyword_total_count, matched_words, sentence),
      file.path(categories_dir, paste0("matched_docs_", category, "_", decade, ".csv"))
    )
    
    # Prepare parquet without keyword count
    matches <- matched_docs %>%
      select(doc_id, global_token_id, token) %>%
      unnest(c(global_token_id, token)) %>%
      filter(token != " ")
    
    result <- matches %>%
      left_join(metadata, by = "global_token_id")
    
    category_file_name <- str_replace_all(category, "[,\\s]+", "_")
    
    write_parquet(result,
                  file.path(categories_dir, paste0(category_file_name, "_", decade, "_congress_filtered_by_gender.parquet")))
    
    rm(matches, result)
    gc()
  }
}

