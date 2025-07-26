# Text Mine Congress

## Step 1: Collect the United States Congressional Records
Before running any scripts in this repository, you must collect the daily Congressional Record data:
- Required Repository: [uscongress-data](https://github.com/stephbuon/uscongress-data).
- Follow the instructions in that repository to generate: `congress_data_daily_by_speaker_with_metadata.csv`.

## Step 2: Process the Data using `pipeline.R`

`pipeline.R` calls these scripts: 

#### `create_decade_subset.R`
- Input: `congress_data_daily_by_speaker_with_metadata.csv`.
- Output: decade subsets stored in `.RData` format.

#### `parse_congress.R`
- Input: the `.RData` decade subsets. 
- Output: Two chunked `.parquet` files per decade (original + POS-parsed), each with `doc_id` and `chunk_id` fields (that together make a unique ID).

#### `bind_chunks.R`
- Input: The combined .parquet files by decade (original + POS-parsed).
- Output: Separate decade subsets as .parquet files for each gender (men and women).

#### (Optional) `subset_by_gender.R`
- Input: The bound chunks as `.parquet` files.
- Output: Decade subsets with combined genders

#### `remove_stopwords.R`
- Input: The decade `.parquet` files with combined genders
- Output: A cleaned version of the decade `.parquet` files without stop words. 

#### `subset_by_category.R`
- Input:
  - Gender-separated decade .parquet files.
  - congress_controlled_vocab.csv file containing keyword categories and patterns.
- Output:
  - CSV debugger files containing matched documents with keyword counts.
  - `.parquet` files filtered by keyword categories for each decade, separated by gender.

## Step 3: Analyze the Data

#### `tf_idf.R`
- Input: the combined gender .parquet files and runs TF-IDF or log-likelihood
- Output: Visualizations