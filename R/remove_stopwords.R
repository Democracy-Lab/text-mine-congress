library(data.table)
library(stringr)

custom_sw <- read.csv(
  "/local/scratch/group/guldigroup/climate_change/congress/text-mine-congress/R/congress_stopwords.csv",
  stringsAsFactors = FALSE
)

stopwords_set <- unique(tolower(custom_sw[[1]]))

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
