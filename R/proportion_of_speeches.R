library(tidyverse)

result <- df %>%
  mutate(contains_term = str_detect(content, "\\b(woman|women|female)\\b")) %>%
  group_by(gender) %>%
  summarise(
    total_speeches = n(),
    speeches_with_term = sum(contains_term, na.rm = TRUE),
    proportion = speeches_with_term / total_speeches
  )

print(result)

# child versus argument 
# focus on people versus focus on the debate itself 