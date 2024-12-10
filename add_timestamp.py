import pandas as pd
from pathlib import Path
import sys
from sqlalchemy import create_engine, text
import json
from datetime import timedelta
import pytz

# Ensures a file is provided as an argument
if len(sys.argv) != 2:
    print("Usage: python script.py <input_file>")
    sys.exit(1)

# Gets the input file from command-line arguments
input_file = sys.argv[1]

# Converts to a Path object and resolve the absolute path
file_path = Path(input_file).resolve()

# Checks to see if the file exists
if not file_path.is_file():
    print(f"Error: File '{file_path}' not found.")
    sys.exit(1)

print(f"Processing file: {file_path}")

# Extracts movie_id from the file name safely
try:
    file_name = file_path.stem
    movie_id = int(file_name.split("_")[1])  # Extracts the number after "movie_"
except (IndexError, ValueError) as e:
    print(f"Error: Unable to extract movie ID from the file name '{file_path.name}'.")
    sys.exit(1)

# Database configuration
db_config = {
    "host": "127.0.0.1",
    "user": "readonly",
    "password": "",
    "database": "ML3_mirror",
}
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
)

# Queries historic ratings from the database
historic_ratings = []
query = text(
    """
    SELECT userId, rating, unix_timestamp(user_tstamp)
    FROM user_rating_pairs_history
    WHERE movieId = :movieID
    ORDER BY user_tstamp ASC;
    """
)
params = {"movieID": movie_id}

with engine.connect() as connection:
    result = connection.execute(query, params)
    for row in result.fetchall():
        historic_ratings.append(
            {
                "userId": row[0],
                "rating": row[1],
                "user_tstamp": row[2],  # is_unix
            }
        )

# Reads the events data
try:
    # Attempts to read JSONL data with error handling
    events_df = pd.read_json(file_path, lines=True, convert_dates=False)
    print(f"Successfully read the JSONL file: {file_path}")
except ValueError as e:
    print(f"Error reading JSON file: {e}")
    print("Attempting to read and print a sample of the file to inspect the issue...")

    # Tries to manually load the JSON to find the error
    with open(file_path, "r", encoding="utf-8") as file:
        for i, line in enumerate(file):
            try:
                json.loads(line)  # Attempt to parse each line
            except json.JSONDecodeError as json_error:
                print(f"Error decoding JSON on line {i + 1}: {json_error}")
                print(f"Line content: {line.strip()}")
                break

    sys.exit(1)

events_df.sort_values(by=["timestamp"], inplace=True, ascending=True)
events_df["avg_rating"] = 0.0

# Processes the data
row_num = 0
rating_num = 0

user_ratings = {}


def average(user_ratings):
    if len(user_ratings) == 0:
        return None
    else:
        return sum(user_ratings.values()) / len(user_ratings)


FUDGE_FACTOR = 5  # treats all rows as happening 5 seconds earlier than the ratings to ensure a rating is never averaged with it's own value.
# In theory, this does distort the truth, but this distortion is part of the existing distortion around what average rating the user actually
# saw in the first place (since they wouldn't see averages including ratings that came in after they loaded the page.)

while row_num < len(events_df):
    next_rating = None
    rating_tstamp = None
    if rating_num < len(historic_ratings):
        next_rating = historic_ratings[rating_num]
        rating_tstamp = next_rating["user_tstamp"]

    next_row = events_df.iloc[row_num]
    row_tstamp = int(next_row["timestamp"])

    # Moves to the next row or rating
    if rating_tstamp and (rating_tstamp + FUDGE_FACTOR) < row_tstamp:
        # advance a rating -- update rating dictionary
        if next_rating["rating"] is None or next_rating["rating"] == -1:
            if next_rating["userId"] in user_ratings:
                del user_ratings[next_rating["userId"]]
        else:
            user_ratings[next_rating["userId"]] = next_rating["rating"]
        rating_num += 1
    else:
        # advance a row -- assign in average rating value.
        events_df.iloc[row_num, events_df.columns.get_loc("avg_rating")] = average(
            user_ratings
        )
        row_num += 1

output_file = file_path.parent / f"processed_{file_path.name}"
events_df.to_csv(output_file, index=False)
print(f"Processed data saved to: {output_file}")

##look at action logs in database
## pull movieeId from website and see if it updated correctly in databse
