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
      .groups = "drop")
  
  url <- "https://raw.githubusercontent.com/stephbuon/text-mine-congress/refs/heads/main/analysis/congress_controlled_vocab.csv"
  
  keywords_df <- read_csv(url) %>%
    mutate(Keywords = str_split(Keywords, ",\\s*"),
           Keywords = map_chr(Keywords, ~ paste0("\\b", str_trim(.x), "\\b", collapse = "|")))
  
  categories <- keywords_df$Category
  
  for (category in categories) {
    message(paste0("Subsetting by category: ", category))
    
    keywords_df_filtered <- keywords_df %>%
      filter(Category == category)
    
    keywords <- keywords_df_filtered %>% 
      pull(Keywords)
    
    matches <- collapsed_f %>%
      filter(str_detect(sentence, regex(keywords, ignore_case = TRUE))) %>%
      unnest(c(global_token_id, token)) %>%
      select(global_token_id, token) %>%
      filter(token != " ")
    
    test <- matches %>%
      left_join(metadata, by = "global_token_id")
    
    category_file_name <- category %>%
      str_replace_all(",", "") %>%
      str_replace_all("\\s+", "_")
    
    write_parquet(test,
                  sink = file.path(categories_dir, paste0(category_file_name, "_", decade, "_congress_filtered_by_gender.parquet")))
    
    rm(matches, test)
    gc() } }
