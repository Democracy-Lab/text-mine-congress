# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
CATEGORY_SCORE_THRESHOLD <- as.numeric(Sys.getenv("CATEGORY_SCORE_THRESHOLD", "0.03"))
PERCENTILE_CUTOFF        <- as.numeric(Sys.getenv("PERCENTILE_CUTOFF", "0.90"))

VOCAB_URL <- Sys.getenv(
  "CONGRESS_VOCAB_URL",
  "https://raw.githubusercontent.com/stephbuon/text-mine-congress/refs/heads/main/analysis/congress_controlled_vocab.csv"
)

MIN_TOKEN_NCHAR <- as.integer(Sys.getenv("MIN_TOKEN_NCHAR", "1"))
DATE_COL        <- Sys.getenv("DATE_COL", "date")

# Batch size for keyword processing (tune for your RAM/CPU)
KW_BATCH_SIZE <- as.integer(Sys.getenv("KW_BATCH_SIZE", "64"))

# Decade test toggle (default FALSE for repo; override via env if desired)
DECADE_TEST      <- as.logical(Sys.getenv("DECADE_TEST", "FALSE"))
DECADE_SELECTION <- as.integer(Sys.getenv("DECADE_SELECTION", "2000"))

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

# Escape keyword to literal regex with word boundaries (ICU-compatible)
escape_kw <- function(x) {
  x  <- str_squish(x)
  rx <- str_replace_all(x, "([.^$|()\\[\\]{}*+?\\\\-])", "\\\\\\1")
  paste0("\\b", rx, "(e?s)?\\b")
}

# Percentile helper
pct_value <- function(x, p) {
  if (length(x) == 0) return(Inf)
  as.numeric(stats::quantile(x, probs = p, na.rm = TRUE, names = FALSE, type = 7))
}

# Extract integer from a string (for ordering doc_id like "text123")
extract_int <- function(x) {
  suppressWarnings(as.integer(stringr::str_extract(x, "\\d+")))
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

# Build a "day key":
# 1) Use DATE_COL if present.
# 2) Else try to parse YYYY-MM-DD from source_file.
# 3) Else fall back to decade (coarse) and warn.
build_day_key <- function(df) {
  if (DATE_COL %in% names(df)) {
    return(as.character(df[[DATE_COL]]))
  }
  guess <- stringr::str_extract(df$source_file %||% "", "\\d{4}-\\d{2}-\\d{2}")
  if (!all(is.na(guess))) return(guess)
  warning("No 'date' column and could not parse date from source_file; falling back to 'decade'.")
  if ("decade" %in% names(df)) as.character(df$decade) else "UNKNOWN_DAY"
}

# ──────────────────────────────────────────────────────────────────────────────
# Core: debate-level categorization via date + title + sequential runs
# - TF–IDF uses CLEANED tokens (input `f`)
# - Also save RAW/original speeches (from us_congress_spacy_ids_<decade>.parquet)
# ──────────────────────────────────────────────────────────────────────────────
subset_by_category <- function(f, decade, categories_dir) {
  # Early-out when running a single-decade test
  if (isTRUE(DECADE_TEST) && as.integer(decade) != as.integer(DECADE_SELECTION)) {
    message(sprintf("Skipping decade %s (DECADE_TEST enabled; running only %s)", decade, DECADE_SELECTION))
    return(invisible(NULL))
  }

  message(sprintf("== Categorizing decade %s at DEBATE level (date + title + sequential runs) ==", decade))

  # Output dirs (tagged by thr/pct)
  tag <- sprintf("thr%s_pct%s",
                 format(CATEGORY_SCORE_THRESHOLD, nsmall = 3, trim = TRUE),
                 format(PERCENTILE_CUTOFF,        nsmall = 2, trim = TRUE))

  score_dir           <- file.path(categories_dir, sprintf("tfidf_norm_scores_%s", tag))
  assign_dir          <- file.path(categories_dir, sprintf("tfidf_norm_assignments_%s", tag))
  filtered_tokens_dir <- file.path(categories_dir, sprintf("tfidf_norm_filtered_tokens_%s", tag))
  filtered_raw_dir    <- file.path(categories_dir, sprintf("tfidf_norm_filtered_raw_%s", tag))
  qc_dir              <- file.path(categories_dir, sprintf("tfidf_norm_qc_%s", tag))

  dir.create(score_dir,           recursive = TRUE, showWarnings = FALSE)
  dir.create(assign_dir,          recursive = TRUE, showWarnings = FALSE)
  dir.create(filtered_tokens_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(filtered_raw_dir,    recursive = TRUE, showWarnings = FALSE)
  dir.create(qc_dir,              recursive = TRUE, showWarnings = FALSE)

  # Prep tokens & basic IDs (CLEANED tokens are already in f)
  f <- f %>%
    mutate(
      token     = tolower(token),
      speech_id = paste0(source_file, "_", doc_id)
    )

  # ── Skip speeches with missing/empty title and report the count
  if (!("title" %in% names(f))) {
    stop("Input data has no 'title' column. Please ensure 'title' is retained earlier in the pipeline.")
  }
  total_speeches_before <- n_distinct(f$speech_id)
  f <- f %>% filter(!is.na(title) & nzchar(title))
  total_speeches_after  <- n_distinct(f$speech_id)
  skipped_count <- total_speeches_before - total_speeches_after

  # Normalize title for grouping (lowercase + squish spaces)
  f <- f %>% mutate(title_key = str_squish(str_to_lower(title)))

  # Build day_key from 'date' if present; else fallback (with warning in helper)
  day_key_vec <- build_day_key(f)
  f <- f %>% mutate(day_key = day_key_vec)

  # Speech order within each day to detect "runs" of the same title.
  f <- f %>%
    mutate(doc_idx = coalesce(extract_int(doc_id), row_number()))

  # One row per speech with its day/title and order
  speech_tbl <- f %>%
    distinct(speech_id, source_file, doc_id, title, title_key, day_key, doc_idx)

  # For each day, order speeches and start a new run whenever the title changes
  speech_tbl <- speech_tbl %>%
    group_by(day_key) %>%
    arrange(source_file, doc_idx, .by_group = TRUE) %>%
    mutate(
      title_changed = (title_key != dplyr::lag(title_key)),
      title_changed = tidyr::replace_na(title_changed, TRUE),
      run_id        = cumsum(title_changed)
    ) %>%
    ungroup()

  # debate_id = date (or day_key) + normalized title_key + run_id
  speech_tbl <- speech_tbl %>%
    mutate(debate_id = paste(day_key, title_key, run_id, sep = "__"))

  # Attach debate_id to tokens
  f <- f %>%
    left_join(speech_tbl %>% select(speech_id, debate_id), by = "speech_id")

  # QC: map of debates
  debate_map <- speech_tbl %>%
    select(debate_id, day_key, title, run_id) %>%
    distinct()
  readr::write_csv(debate_map, file.path(qc_dir, sprintf("debate_map_%s.csv", decade)))

  # Build per-debate text and word counts (compact representation) from CLEANED tokens
  debate_tokens <- f %>%
    group_by(debate_id) %>%
    summarise(
      total_words = sum(nchar(token) >= MIN_TOKEN_NCHAR, na.rm = TRUE),
      text        = str_c(token, collapse = " "),
      .groups     = "drop"
    )

  # We don't need the token-level frame anymore; free it.
  rm(f, speech_tbl); gc()

  n_debates <- nrow(debate_tokens)
  if (n_debates == 0) {
    warning("No debates found after filtering; skipping.")
    message(sprintf("Speeches not grouped to debate (missing/empty title): %d", skipped_count))
    message(sprintf("Speeches grouped to debate and used: %d", total_speeches_after))
    return(invisible(NULL))
  }

  # Controlled vocabulary (Category, Keywords)
  vocab <- readr::read_csv(VOCAB_URL, show_col_types = FALSE) %>%
    mutate(Keywords = str_split(Keywords, ",\\s*")) %>%
    unnest_longer(Keywords, values_to = "keyword") %>%
    mutate(
      keyword = tolower(str_squish(keyword)),
      pattern = map_chr(keyword, escape_kw)
    ) %>%
    select(category = Category, keyword, pattern)

  categories <- unique(vocab$category)

  # ───────────────────────────────────────────────────────────
  # Two-pass TF–IDF, batched by keywords to limit memory
  # ───────────────────────────────────────────────────────────

  # Convenience vectors
  deb_ids   <- debate_tokens$debate_id
  deb_text  <- debate_tokens$text
  deb_words <- pmax(debate_tokens$total_words, 1L)  # avoid divide-by-zero

  # Pass 1: DF per keyword (how many debates contain it)
  message("Pass 1/2: computing DF per keyword (batched) ...")
  K <- nrow(vocab)
  df_counts <- numeric(K)

  batches <- split(seq_len(K), ceiling(seq_len(K) / KW_BATCH_SIZE))
  for (idx in batches) {
    pats <- vocab$pattern[idx]
    has_any <- lapply(pats, function(p)
      stringr::str_count(deb_text, stringr::regex(p, ignore_case = TRUE)) > 0L
    )
    df_counts[idx] <- vapply(has_any, sum, 0L)
    rm(has_any, pats); gc()
  }

  idf <- log((n_debates + 1) / (df_counts + 1)) + 1
  rm(df_counts); gc()

  # Pass 2: accumulate category scores directly (no kw_counts table)
  message("Pass 2/2: accumulating category scores (batched) ...")

  # Initialize per-debate score matrix (debates x categories)
  cat_levels <- sort(unique(categories))
  cat_mat <- matrix(0.0, nrow = n_debates, ncol = length(cat_levels))
  colnames(cat_mat) <- cat_levels

  batches <- split(seq_len(K), ceiling(seq_len(K) / KW_BATCH_SIZE))
  for (idx in batches) {
    pats <- vocab$pattern[idx]
    cats <- vocab$category[idx]
    idfs <- idf[idx]

    for (j in seq_along(idx)) {
      counts <- stringr::str_count(deb_text, stringr::regex(pats[j], ignore_case = TRUE))
      if (!any(counts)) next
      norm_tf_1k <- (counts / deb_words) * 1000.0
      contrib    <- norm_tf_1k * idfs[j]
      cat_col    <- match(cats[j], cat_levels)
      cat_mat[, cat_col] <- cat_mat[, cat_col] + contrib
    }
    rm(pats, cats, idfs); gc()
  }

  # Build long cat_scores table from the matrix
  cat_scores <- as_tibble(cat_mat) %>%
    mutate(debate_id = deb_ids) %>%
    relocate(debate_id) %>%
    pivot_longer(-debate_id, names_to = "category", values_to = "category_score") %>%
    left_join(tibble(debate_id = deb_ids, total_words = as.numeric(deb_words)), by = "debate_id")

  # Free heavy objects
  rm(cat_mat, deb_text, deb_words, debate_tokens, vocab, categories); gc()

  # Persist full score table
  write_parquet(cat_scores, sink = file.path(
    score_dir, sprintf("category_scores_%s_unit-debate_tfidf_norm_1k.parquet", decade)
  ))
  readr::write_csv(cat_scores, file.path(
    score_dir, sprintf("category_scores_%s_unit-debate_tfidf_norm_1k.csv", decade)
  ))

  # Multi-label assignment with absolute threshold and optional percentile gate
  message("Assigning categories (multi-label) ...")
  assignments <- cat_scores %>%
    group_by(debate_id) %>%
    mutate(
      pct_gate = if (PERCENTILE_CUTOFF > 0) pct_value(category_score, PERCENTILE_CUTOFF) else -Inf,
      include  = category_score >= max(CATEGORY_SCORE_THRESHOLD, pct_gate)
    ) %>%
    ungroup() %>%
    filter(include) %>%
    arrange(debate_id, desc(category_score))

  write_parquet(assignments, sink = file.path(
    assign_dir, sprintf("assignments_%s_unit-debate_thr%.3f_pct%.2f.parquet", decade, CATEGORY_SCORE_THRESHOLD, PERCENTILE_CUTOFF)
  ))
  readr::write_csv(assignments, file.path(
    assign_dir, sprintf("assignments_%s_unit-debate_thr%.3f_pct%.2f.csv", decade, CATEGORY_SCORE_THRESHOLD, PERCENTILE_CUTOFF)
  ))

  # Optional: write CLEANED token subsets per category (debate-level)
  message("Preparing filtered token outputs per category (CLEANED tokens) ...")
  cleaned_path <- file.path("data", "gender_analysis", paste0("us_congress_men_and_women_clean_", decade, ".parquet"))
  if (file.exists(cleaned_path)) {
    tokens_decade <- arrow::read_parquet(cleaned_path) %>%
      mutate(
        token     = tolower(token),
        speech_id = paste0(source_file, "_", doc_id),
        title_key = str_squish(str_to_lower(title)),
        day_key   = build_day_key(.),
        doc_idx   = coalesce(extract_int(doc_id), row_number())
      ) %>%
      distinct(speech_id, source_file, doc_id, token, pos, gender, date, state, party, title, .keep_all = TRUE)

    speech_tbl2 <- tokens_decade %>%
      distinct(speech_id, source_file, doc_id, title, title_key, day_key, doc_idx) %>%
      group_by(day_key) %>%
      arrange(source_file, doc_idx, .by_group = TRUE) %>%
      mutate(
        title_changed = (title_key != dplyr::lag(title_key)),
        title_changed = tidyr::replace_na(title_changed, TRUE),
        run_id        = cumsum(title_changed),
        debate_id     = paste(day_key, title_key, run_id, sep = "__")
      ) %>%
      ungroup() %>%
      select(speech_id, debate_id)

    tokens_decade <- tokens_decade %>% left_join(speech_tbl2, by = "speech_id")

    cats <- sort(unique(assignments$category))
    for (cat in cats) {
      keep_debates <- assignments %>% filter(category == cat) %>% distinct(debate_id)
      out_tbl <- tokens_decade %>% semi_join(keep_debates, by = "debate_id")
      safe_cat <- str_replace_all(cat, "[,\\s]+", "_")
      write_parquet(out_tbl, sink = file.path(
        filtered_tokens_dir, sprintf("%s_%s_unit-debate_tfidf_norm_1k_filtered_tokens.parquet", safe_cat, decade)
      ))
    }
    rm(tokens_decade, speech_tbl2); gc()
  } else {
    warning("Cleaned token file not found for filtered outputs: ", cleaned_path)
  }

  # Also write RAW (original speech) subsets per category for validation
  message("Preparing filtered RAW speech outputs per category ...")
  raw_path <- file.path("data", paste0("us_congress_spacy_ids_", decade, ".parquet"))
  if (file.exists(raw_path)) {
    raw_decade <- arrow::read_parquet(raw_path) %>%
      mutate(
        speech_id = paste0(source_file, "_", doc_id),
        title_key = str_squish(str_to_lower(title)),
        day_key   = build_day_key(.),
        doc_idx   = coalesce(extract_int(doc_id), row_number())
      )

    raw_map <- raw_decade %>%
      distinct(speech_id, source_file, doc_id, title, title_key, day_key, doc_idx) %>%
      group_by(day_key) %>%
      arrange(source_file, doc_idx, .by_group = TRUE) %>%
      mutate(
        title_changed = (title_key != dplyr::lag(title_key)),
        title_changed = tidyr::replace_na(title_changed, TRUE),
        run_id        = cumsum(title_changed),
        debate_id     = paste(day_key, title_key, run_id, sep = "__")
      ) %>%
      ungroup() %>%
      select(speech_id, debate_id)

    raw_with_debate <- raw_decade %>% left_join(raw_map, by = "speech_id")

    cats <- sort(unique(assignments$category))
    for (cat in cats) {
      keep_debates <- assignments %>% filter(category == cat) %>% distinct(debate_id)
      out_tbl <- raw_with_debate %>% semi_join(keep_debates, by = "debate_id")
      safe_cat <- str_replace_all(cat, "[,\\s]+", "_")
      write_parquet(out_tbl, sink = file.path(
        filtered_raw_dir, sprintf("%s_%s_unit-debate_tfidf_norm_1k_filtered_RAW_SPEECHES.parquet", safe_cat, decade)
      ))
    }
    rm(raw_with_debate, raw_map, raw_decade); gc()
  } else {
    warning("RAW (spacy_ids) file not found for filtered RAW outputs: ", raw_path)
  }

  # Final report on grouped vs skipped speeches
  message(sprintf("Speeches not grouped to debate (missing/empty title): %d", skipped_count))
  message(sprintf("Speeches grouped to debate and used: %d", total_speeches_after))
  message("subset_by_category complete (debate-level with sequential runs, titles required).")
}

message("Loaded subset_by_category: debate-level (date + title + sequential runs), TF–IDF + normalized frequency, multi-label, tokens-scored + RAW-speech outputs for validation.")
