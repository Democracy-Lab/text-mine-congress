library(tidyverse)
library(tictoc)
library(arrow)

delete_chunks <- function() {
  answer <- readline(prompt = "Are you sure you want to delete all chunks? This action cannot be reversed. [y/N]: ")
  
  if (tolower(answer) != "y") {
    message("Operation cancelled.")
    return(invisible(NULL)) }
  
  chunk_dir <- file.path("data", "chunks")
  
  if (!dir.exists(chunk_dir)) {
    message("Chunk directory does not exist: ", chunk_dir)
    return(invisible(NULL)) }
  
  files_deleted <- list.files(chunk_dir, full.names = TRUE)
  unlink(files_deleted, recursive = TRUE)
  message("Deleted ", length(files_deleted), " file(s) from: ", chunk_dir) }


safe_load_df <- function(file) {
  tryCatch({ read_parquet(file) %>%
      mutate(source_file = basename(file)) }, 
      error = function(e) { 
        message("Skipping file due to error: ", file)
        return(NULL) }) }


bind_chunks <- function(chunk_type) {
  
  if(chunk_type == "spacy_parse") {
    match_pattern = "^us_congress_spacy_parsed_\\d{4}_chunk_\\d+\\.parquet$"
    detection_pattern = "us_congress_spacy_parsed_%d_chunk_\\d+\\.parquet$" 
    output_prefix = "us_congress_spacy_parsed_" }
  
  if(chunk_type == "default") {
    match_pattern = "^us_congress_\\d{4}_chunk_\\d+\\.parquet$"
    detection_pattern = "us_congress_%d_chunk_\\d+\\.parquet$" 
    output_prefix = "us_congress_spacy_ids_" } 
  
  chunk_dir <- file.path("data", "chunks")
  all_chunk_files <- list.files(path = chunk_dir, 
                                pattern = match_pattern, 
                                full.names = TRUE )
  
  # Pre-group all files by decade
  files_by_decade <- map(decades, function(d) {
    decade_pattern <- sprintf(detection_pattern, d)
    all_chunk_files[str_detect(all_chunk_files, decade_pattern)] }) %>% 
    set_names(decades)
  
  # Loop over pre-grouped decades
  for (d in names(files_by_decade)) {
    message("Processing decade: ", d)
    tic(paste("Total time processing ", d, ": "))
    
    decade_files <- files_by_decade[[d]]
    
    if (length(decade_files) == 0) {
      message("No files found for decade ", d)
      next }
    
    df_list <- map(decade_files, safe_load_df)
    combined_df <- bind_rows(compact(df_list))
    
    output_file <- file.path("data", paste0(output_prefix, d, ".parquet"))
    write_parquet(combined_df, output_file)
    
    toc() } 
  gc() }

