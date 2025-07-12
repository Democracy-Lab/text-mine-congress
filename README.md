# Text Mine Congress

### Collect the United States Congressional Records: 
Before running any scripts in this repository, you must collect the daily Congressional Record data:
- Required Repository: [uscongress-data](https://github.com/stephbuon/uscongress-data).
- Follow the instructions in that repository to generate: `congress_data_daily_by_speaker_with_metadata.csv`.

### Run: 

### `create_decade_subset.R`
- Inputs `congress_data_daily_by_speaker_with_metadata.csv`.
- Outputs decade subsets stored in `.RData` format.

### `parse_congress.R`
- Inputs the `.RData` decade subsets. 
- Ouputs two types of chunked `.parquet` data that can be joined on `doc_id` and `chunk_id` (that together make a unique ID). These two types are:
    - Chunks of the Congressional Records. 
    - Chunks of the spaCy parsed Congressional Records.

### `bind_chunks.R`
- Inputs the chunked `.parquet` files and combines them by decade. 
- Outputs a combined `.parquet` dataset by decade.

### `remove_stopwords.R`
- Inputs the decade `.parquet` files with combined genders and removes stopwords 
- Outputs a cleaned version of the `.parquet` files without stop words. 

### `tf_idf.R`
- Takes the combined gender .parquet files and runs TF-IDF or log-likelihood
