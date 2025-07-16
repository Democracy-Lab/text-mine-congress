library(tidyverse)
library(furrr)
library(future)
library(tictoc)
library(spacyr)
library(reticulate)
library(arrow)

log_message <- function(...) {
  log_dir <- "logs"
  log_file <- file.path(log_dir, "logfile.txt")
  
  if (!dir.exists(log_dir)) {
    dir.create(log_dir) }
  
  con <- file(log_file, open = "a")
  on.exit(close(con), add = TRUE)
  
  writeLines(paste0(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), paste(..., collapse = " ")), con) }


spacy_parse_unix <- function(df, chunk_id, current_decade, num_chunks) {
  
  message(paste0("Employing spaCy Parse"))
  spacy_initialize(model = "en_core_web_sm")
  
  message(paste0("Worker starting for chunk ", chunk_id))
  
  original_df <- df %>%
    mutate(doc_id = paste0("text", seq_len(n())))
  
  input_text <- original_df %>%
    select(doc_id, content) %>%
    rename(text = content)
  
  parsed <- spacy_parse(input_text,
                        pos = TRUE,
                        dependency = FALSE,
                        lemma = FALSE,
                        tag = FALSE, 
                        entity = FALSE)
  
  write_parquet(original_df, 
                sink = file.path("data", "chunks", paste0("us_congress_", current_decade, "_chunk_", chunk_id, ".parquet")))
  
  tidy_ngrams <- bind_rows(parsed)
  
  write_parquet(tidy_ngrams, 
                sink = file.path("data", "chunks", paste0("us_congress_spacy_parsed_", current_decade, "_chunk_", chunk_id, ".parquet")))
  
  spacy_finalize() }


spacy_parse_windows <- function(df, chunk_id, current_decade, num_chunks) {
  
  message(paste0("Worker starting for chunk ", chunk_id))
  
  original_df <- df %>%
    mutate(doc_id = paste0("text", seq_len(n())))
  
  input_text <- original_df %>%
    select(doc_id, content) %>%
    rename(text = content)
  
  parsed <- spacy_parse(input_text,
                        pos = TRUE,
                        dependency = TRUE,
                        lemma = FALSE,
                        tag = FALSE, 
                        entity = FALSE)
  
  write_parquet(original_df, 
                sink = file.path("data", "chunks", paste0("us_congress_", current_decade, "_chunk_", chunk_id, ".parquet")))

  tidy_ngrams <- bind_rows(parsed)
  
  write_parquet(tidy_ngrams, 
                sink = file.path("data", "chunks", paste0("us_congress_spacy_parsed_", current_decade, "_chunk_", chunk_id, ".parquet")))
  
  spacy_finalize() }


process <- function(data, d, num_chunks, os) {
  tic(paste0("Total time parallel processing ", d))
  
  log_message("Processing Data")
  log_message(paste0("Number of Chunks: ", num_chunks))

  chunks <- data %>%
    mutate(chunk_id = ntile(row_number(), num_chunks)) %>%
    group_split(chunk_id)
  
  if(os=="Windows") {
    future_map2(chunks, seq_along(chunks),
                ~ spacy_parse_windows(.x, .y, d, num_chunks), 
                .options = furrr_options(seed = TRUE))  } # Seed for reproducability. Is it needed? 
  
  if(os=="Linux") {
    future_map2(chunks, seq_along(chunks),
                ~ spacy_parse_unix(.x, .y, d, num_chunks), 
                .options = furrr_options(seed = TRUE))  } # Seed for reproducability. Is it needed? 
  
  
  timing <- toc(log = TRUE, quiet = TRUE)
  elapsed_sec <- timing$toc - timing$tic
  elapsed_min <- elapsed_sec / 60
  print(paste("Elapsed time:", round(elapsed_min, 2), "minutes")) }

