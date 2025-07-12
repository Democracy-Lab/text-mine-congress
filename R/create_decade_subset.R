library(tidyverse)
library(data.table)

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
    save(decade_subset, file = file.path(output_dir, paste0(decade_name, ".RData"))) } }



dir_name <- "Data"
dir.create(dir_name)
us_congress_data <- fread("congress_data_daily_by_speaker_with_metadata.csv")

us_congress_data_w_decade <- create_decades_col(us_congress_data)
split_by_decade(us_congress_data_w_decade, dir_name)

