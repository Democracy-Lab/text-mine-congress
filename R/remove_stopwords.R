# Remove Stop Words
stopwords_set <- unique(tolower(stop_words$word))

remove_stopwords <- function(dataframe, stopwords_set) {
  dt <- as.data.table(dataframe)
  dt[, token := str_to_lower(token)]
  
  dt <- dt[!(token %in% stopwords_set)]
  dt <- dt[!str_detect(token, "^[[:punct:]]+$")]
  return(dt) }
 