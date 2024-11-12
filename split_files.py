import pandas as pd
import json
from os.path import isfile, join
from pathlib import Path


# Step 2: Splits the filtered file into intermediate files based on movieId batches
def split_by_movie_id(input_file, output_folder):
    events_df = pd.read_json(input_file, lines=True)
    unique_movie_ids = events_df["movieId"].dropna().unique()

    for movie_id in unique_movie_ids:
        batch_df = events_df[events_df["movieId"] == movie_id]
        batch_df.to_json(
            join(output_folder, "movie_" + str(movie_id) + ".jsonl"),
            orient="records",
            lines=True,
        )

    print(f"Split events into {len(unique_movie_ids)} intermediate files.")


input_file = "out/filtered_rating_events.jsonl"
output_folder = "out/by_movie"
Path(output_folder).mkdir(parents=True, exist_ok=True)


# Runs the process
split_by_movie_id(input_file, output_folder)
