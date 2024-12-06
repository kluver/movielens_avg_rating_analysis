from os import listdir
from os.path import isfile, join
import gzip
from pathlib import Path

import pandas as pd
import json

# Step 1: Filters 'rating' events and save to an intermediate file
def filter_rating_events(input_files, filtered_file):
    with open(filtered_file, "w") as filtered_output:
        for file in input_files:
            for line in file:
                try:
                    # Splits the line by tab to isolate fields
                    fields = line.strip().split("\t")
                    if len(fields) < 5:
                        print(f"Skipping line with unexpected format: {line}")
                        continue

                    timestamp, userId, sessionId, event_type, metadata_json = fields
                    if event_type == "rating":
                        # Parses the metadata JSON to extract relevant fields
                        metadata = json.loads(metadata_json)
                        prediction = metadata.get("pred")  # Extract prediction value

                        # Creates a new dictionary with the relevant data
                        event_data = {
                            "timestamp": timestamp,
                            "userId": userId,
                            "movieId": metadata.get("movieId"),
                            "rating": metadata.get("rating"),
                            "prediction": prediction,
                        }

                        # Writes to output as JSON for consistent structure
                        filtered_output.write(json.dumps(event_data) + "\n")

                except json.JSONDecodeError as e:
                    print(f"Error processing line: {line}. Exception: {e}")
    print(f"Filtered events saved to '{filtered_file}'.")


input_folder = "action_logs"
input_files = [
    join(input_folder, f)
    for f in listdir(input_folder)
    if isfile(join(input_folder, f))
]
input_files = [gzip.open(f, "rt") for f in input_files]


filtered_file = "out/filtered_rating_events.jsonl"
Path("out").mkdir(parents=True, exist_ok=True)

filter_rating_events(input_files, filtered_file)
