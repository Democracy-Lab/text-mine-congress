# Text Mine Congress

## Step 1: Collect the United States Congressional Records
Before running any scripts in this repository, you must collect the daily Congressional Record data:
- Required Repository: [uscongress-data](https://github.com/stephbuon/uscongress-data).
- Follow the instructions in that repository to generate: `congress_data_daily_by_speaker_with_metadata.csv`.

## Step 2: Process the Data

#### `create_decade_subset.R`
- Input: `congress_data_daily_by_speaker_with_metadata.csv`.
- Output: decade subsets stored in `.RData` format.

#### `parse_congress.R`
- Input: the `.RData` decade subsets. 
- Output: Two chunked `.parquet` files per decade (original + POS-parsed), each with `doc_id` and `chunk_id` fields (that together make a unique ID).

#### `bind_chunks.R`
- Input: The chunked `.parquet` files (original + POS-parsed).
- Output: Combined `.parquet` files by decade.

#### `remove_stopwords.R`
- Input: The decade `.parquet` files with combined genders and removes stopwords 
- Output: A cleaned version of the decade `.parquet` files without stop words. 

## Step 3: Analyze the Data

#### `tf_idf.R`
- Input: the combined gender .parquet files and runs TF-IDF or log-likelihood
- Output: Visualizations