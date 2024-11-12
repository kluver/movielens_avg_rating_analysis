from os import listdir
from os.path import isfile, join
import gzip

import pandas as pd
import json
from sqlalchemy import create_engine, text

# Database configuration
db_config = {
    "host": "127.0.0.1",
    "user": "readonly",
    "password": "",
    "database": "ML3_mirror",
}

# Creates SQLAlchemy engine for MySQL connection
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
)


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


# Step 2: Splits the filtered file into intermediate files based on movieId batches
def split_by_movie_id(filtered_file, batch_size=500):
    events_df = pd.read_json(filtered_file, lines=True)
    unique_movie_ids = events_df["movieId"].dropna().unique()

    intermediate_files = []
    for i in range(0, len(unique_movie_ids), batch_size):
        batch_movie_ids = unique_movie_ids[i : i + batch_size]
        batch_df = events_df[events_df["movieId"].isin(batch_movie_ids)]
        intermediate_file = f"intermediate_batch_{i // batch_size}.csv"
        batch_df.to_csv(intermediate_file, index=False)
        intermediate_files.append(intermediate_file)

    print(f"Split events into {len(intermediate_files)} intermediate files.")
    return intermediate_files


# Step 3: Fetches average ratings for all movieIds in a single query
def get_avg_ratings_for_batch(batch_df, engine):
    avg_ratings = {}
    batch_df["timestamp"] = pd.to_datetime(batch_df["timestamp"])

    # Gets unique movieIds
    movie_ids = batch_df["movieId"].unique()

    # Prepares the SQL query with placeholders
    query = text(
        """
        SELECT movieId, AVG(rating) AS avg_rating_before_event
        FROM user_rating_pairs_history
        WHERE movieId IN :movieIds
          AND user_tstamp < :event_timestamp
          AND userId != :current_userId
        GROUP BY movieId
    """
    )

    # Uses the maximum timestamp from the batch for the event_timestamp
    max_timestamp = batch_df["timestamp"].max()

    # Prepars the query parameters
    params = {
        "movieIds": tuple(movie_ids),
        "event_timestamp": max_timestamp,
        "current_userId": batch_df["userId"].iloc[0],
    }

    # Executes the query
    with engine.connect() as connection:
        result = connection.execute(query, params)

        # Stores the results in the avg_ratings dictionary
        for row in result.fetchall():
            avg_ratings[row[0]] = row[1]

    # Maps avg ratings back to the original batch DataFrame
    batch_df["avg_rating_before_event"] = (
        batch_df["movieId"].map(avg_ratings).fillna(0.0)
    )

    return batch_df


# Step 4: Processes each intermediate file and save results to the final output file
def process_intermediate_files(intermediate_files, output_file, engine):
    for intermediate_file in intermediate_files:
        print(f"Processing {intermediate_file}...")
        batch_df = pd.read_csv(intermediate_file)
        batch_df = get_avg_ratings_for_batch(batch_df, engine)

        # Appends to the output file, ensuring headers are written only once
        batch_df.to_csv(
            output_file,
            mode="a",
            header=not pd.io.common.file_exists(output_file),
            index=False,
        )
        print(
            f"Processed and appended results from {intermediate_file} to '{output_file}'."
        )


# Main Execution
input_folder = "action_logs"

input_files = [
    join(input_folder, f)
    for f in listdir(input_folder)
    if isfile(join(input_folder, f))
]
input_files = [gzip.open(f, "rt") for f in input_files]


filtered_file = "out/filtered_rating_events.jsonl"
output_file = "out/final.csv"


# Runs the process
filter_rating_events(input_files, filtered_file)
# intermediate_files = split_by_movie_id(filtered_file, batch_size=500)
# process_intermediate_files(intermediate_files, output_file, engine)

print("Processing completed.")

## Filter data based off of movieIds
## Make sure to include different files and make temporal ordering
## Then split off into data that is only relevant
## Then do just one query that finds the average rating before this event
## Output should be userid, movieid, rating, prediction, average ratin
