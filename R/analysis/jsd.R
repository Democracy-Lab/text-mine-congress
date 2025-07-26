
library(philentropy)  # For Jensen-Shannon Divergence

# Compute token distributions for each gender
token_probs <- counts %>%
  group_by(gender) %>%
  mutate(prob = n / sum(n)) %>%
  ungroup() %>%
  pivot_wider(names_from = gender, values_from = prob, values_fill = 0)  # fill missing tokens with 0

# Convert to matrix for JSD
prob_matrix <- as.matrix(token_probs %>% select(-token, -pos))

# Compute JSD (value between 0 and 1; closer to 0 = more similar)
jsd_value <- distance(prob_matrix, method = "jensen-shannon")

# Optionally print or save
message(paste("JSD for", cat, "in", target_decade, ":", round(jsd_value, 4)))


