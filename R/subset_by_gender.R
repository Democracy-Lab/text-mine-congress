subset_by_gender <- function(d, data_dir) {
  
  message(paste0("Processing ", d))
  
  parsed_file_name <- paste0("us_congress_spacy_parsed_", d, ".parquet")
  record_file_name <- paste0("us_congress_spacy_ids_", d,".parquet")
    
  parsed_decade_subset <- read_parquet(file.path(data_dir, parsed_file_name)) %>%
    mutate(source_file = str_replace(source_file, "spacy_parsed_", ""))
    
  record_decade_subset <- read_parquet(file.path(data_dir, record_file_name)) %>%
    select(source_file, doc_id, decade, gender, state, party, title)
    
  joined_subset <- parsed_decade_subset %>%
    left_join(record_decade_subset, by = c("source_file", "doc_id"))
  
  rm(parsed_decade_subset, record_decade_subset)
  gc()
    
  men <- joined_subset %>%
    filter(gender == "M") %>%
    select(doc_id, sentence_id, token_id, token, pos, gender, state, party, title)
    
  write_parquet(men, 
                sink = file.path("data", "gender_analysis", paste0("us_congress_men_", d, ".parquet")))
    
  women <- joined_subset %>%
    filter(gender == "F") %>%
    select(doc_id, sentence_id, token_id, token, pos, gender, state, party, title) 
    
  write_parquet(women, 
                sink = file.path("data", "gender_analysis", paste0("us_congress_women_", d, ".parquet"))) 
  
  rm(men, women)
  gc() }


