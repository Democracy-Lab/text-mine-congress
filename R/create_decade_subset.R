library(tidyverse)
library(data.table)
library(arrow)
library(here)

create_decades_col <- function(data) {
  data %>%
    select(-c(granule_id)) %>%
    mutate(year = year(ymd(date)),
           decade = paste0(floor(year / 10) * 10)) }

split_by_decade <- function(data, output_dir) {
  
  decades <- sort(unique(data$decade))
  
  for(d in decades) {
    message(paste0("Subsetting ", d))
    
    decade_subset <- data %>%
      filter(decade == d) 
    
    decade_name <- paste0("us_congress_", d)
    write_parquet(decade_subset, sink = file.path(output_dir, paste0(decade_name, ".parquet"))) } }
