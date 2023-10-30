import pandas as pd

from os import listdir
from os.path import isfile, join

FOLDER = "../data/parts"
OUTPUT_FOLDER = "../data/"

onlyfiles = [f for f in listdir(FOLDER) if isfile(join(FOLDER, f))]
print(onlyfiles)


# Load initial dataframe, first one
dataframes = []

for file in onlyfiles:
    print(f"Loading {file} ...")
    df = pd.read_csv(f"{FOLDER}/{file}", index_col=0)
    dataframes.append(df)

# Concat output
final_frame = pd.concat(dataframes)
final_frame.drop_duplicates(subset='subreddit', inplace=True)
final_frame.reset_index(level=None, drop=True, inplace=True, col_level=0, col_fill='')

final_frame.to_csv(f"{OUTPUT_FOLDER}merged.csv")