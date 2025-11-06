library(data.table)
library(stringr)
library(readr)
library(purrr)

url <- "https://raw.githubusercontent.com/stephbuon/text-mine-congress/main/analysis/congress_stopwords.csv"

congress_stopwords <- read_csv(url, col_names = FALSE) %>%
  pull(1) %>%
  str_trim() %>%
  discard(~ .x == "")

stopwords_set <- unique(tolower(congress_stopwords))

remove_stopwords <- function(dataframe, stopwords_set) {
  dt <- as.data.table(dataframe)
  dt[, token := str_to_lower(token)]
  dt <- dt[!(token %in% stopwords_set)]
  dt <- dt[!str_detect(token, "^[[:punct:]]+$")]
  dt <- dt[!str_detect(token, "\\d")]
  dt <- dt[nchar(token) > 1]
  dt <- dt[str_trim(token) != ""]
  return(dt)
}
