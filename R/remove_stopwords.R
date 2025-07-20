url <- "https://raw.githubusercontent.com/stephbuon/text-mine-congress/main/analysis/congress_stopwords.csv"

congress_stopwords <- read_csv(url, col_names = FALSE) %>%
  pull(1) %>%
  str_trim() %>%
  discard(~ .x == "") # remove empty strings

stopwords_set <- unique(tolower(congress_stopwords))

remove_stopwords <- function(dataframe, stopwords_set) {
  message("Removing stop words")
  
  dt <- as.data.table(dataframe)
  dt[, token := str_to_lower(token)]
  
  dt <- dt[!(token %in% stopwords_set)]
  dt <- dt[!str_detect(token, "^[[:punct:]]+$")]
  return(dt) }
 