import pandas as pd

# Load the intial dataset
subreddits = pd.read_csv("../data/subreddits.csv")

# Load the requested / found dataset.
meta_set = pd.read_csv('../data/merged.csv')


missing_subreddits = meta_set[meta_set['available'] == False]
missing_subreddits = missing_subreddits[['subreddit']]

print(missing_subreddits)

missing_subreddits.to_csv('../data/missing_subreddits.csv', index=False)