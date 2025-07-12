# Text Mine Congress

## Run: 

## `create_decade_subset.R`
- Takes `congress_data_daily_by_speaker_with_metadata.csv`, the Congressional Records pulled from GovInfo's API
- Returns decade subsets stored in `.RData` format

## `parse_congress.R`
- Takes the decade subsets 
- Returns two spaCy parsed chunk types as ouput that can be joined on `doc_id` and `chunk_id` that together make a unique ID
    - Chunks for the original congressional records
    - Chunks for the parsed congressional records for part-of-speech

## `bind_chunks.R`
- Takes the spaCy parsed chunks and combines them by decade 
- Returns a combined dataset by decade 

## `tf_idf.R`
- ENTER
- ENTER