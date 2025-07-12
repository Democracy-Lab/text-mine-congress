# Remove Stop Words

remove_stopwords <- function(dataframe, stopwords_set) {
  dt <- as.data.table(dataframe)
  dt[, token := str_to_lower(token)]
  
  dt <- dt[!(token %in% stopwords_set)]
  dt <- dt[!str_detect(token, "^[[:punct:]]+$")]
  return(dt) }
 
stopwords_set <- unique(tolower(stop_words$word))

for(d in decades) {
  data_dir <- file.path("data", "gender_analysis")
  
  men_file_name <- paste0("us_congress_men_", d, ".parquet")
  women_file_name <- paste0("us_congress_women_", d, ".parquet")
  
  parsed_decade_subset_men <- read_parquet(file.path(data_dir, men_file_name))
  parsed_decade_subset_women <- read_parquet(file.path(data_dir, women_file_name)) 
  
  out1 <- remove_stopwords(parsed_decade_subset_men, stopwords_set)
  out2 <- remove_stopwords(parsed_decade_subset_women, stopwords_set)
  
  all_data <- bind_rows(out1, out2)
  
write_parquet(all_data, 
              sink = file.path("data", "gender_analysis", paste0("us_congress_men_and_women_clean_", d, ".parquet"))) }
 