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
    movie_id = int(file_name.split('_')[1])  # Extracts the number after "movie_"
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
    SELECT userId, rating, system_tstamp
    FROM user_rating_pairs_history
    WHERE movieId = :movieID
    ORDER BY system_tstamp ASC;
    """
)
params = {"movieID": movie_id}

with engine.connect() as connection:
    result = connection.execute(query, params)
    for row in result.fetchall():
        historic_ratings.append({
            "userId": row[0],
            "rating": row[1],
            "system_tstamp": pd.Timestamp(row[2], tz="UTC")  # Ensure UTC timezone
        })

# Reads the events data
try:
    # Attempts to read JSONL data with error handling
    events_df = pd.read_json(file_path, lines=True)
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
events_df["avg_rating"] = 0

# Function to format the timedelta as days, hours, minutes, seconds
def format_timedelta(td):
    if td is None:
        return "No timestamp available"
    if isinstance(td, timedelta):
        sign = "-" if td.days < 0 else ""
        td = abs(td)  # Works with the absolute value for formatting
        return f"{sign}{td.days} days {td.seconds // 3600} hours {(td.seconds % 3600) // 60} minutes {(td.seconds % 60)} seconds"
    return "N/A"

# Defines timezones
utc = pytz.utc
central = pytz.timezone("US/Central")

# Tolerance for matching timestamps
tolerance = timedelta(seconds=30)  # Allows a maximum of 30 seconds mismatch

# Processes the data
row_num = 0
rating_num = 0

while row_num < len(events_df):
    next_rating = None
    rating_tstamp = None
    if rating_num < len(historic_ratings):
        next_rating = historic_ratings[rating_num]
        rating_tstamp = next_rating["system_tstamp"]

    next_row = events_df.iloc[row_num]
    row_tstamp = pd.Timestamp(next_row["timestamp"])  # Convert to pandas Timestamp

    # Handles timezone-awareness for row_tstamp
    if row_tstamp.tzinfo is None:
        # Assume row_tstamp is in Central Time (if naive), then convert to UTC
        row_tstamp = row_tstamp.tz_localize(central).astimezone(utc)

# Debug: Print raw and converted timestamps
    print(f"Raw Row Timestamp: {next_row['timestamp']} (Assumed Central Time)")
    print(f"Converted Row Timestamp: {row_tstamp} (UTC)")
    print(f"Historic Rating Timestamp: {rating_tstamp} (UTC)")

    # Calculates the time difference
    time_diff = (rating_tstamp - row_tstamp) if rating_tstamp and row_tstamp else None


    print(
    f"Rating {rating_num}: {rating_tstamp} (UTC), Row {row_num}: {row_tstamp} (UTC), Time Difference: {format_timedelta(time_diff)}",
    sep="\t"
)
    
    # Checks if timestamps match within the tolerance
    if time_diff and abs(time_diff) <= tolerance:
        # Matching timestamps found
        events_df.at[row_num, "avg_rating"] = next_rating["rating"]
        rating_num += 1  # Move to the next rating
    else:
        # Moves to the next row or rating
        if rating_tstamp and rating_tstamp < row_tstamp:
            rating_num += 1
        else:
            row_num += 1

output_file = file_path.parent / f"processed_{file_path.name}"
events_df.to_csv(output_file, index=False)
print(f"Processed data saved to: {output_file}")

##look at action logs in database
## pull movieeId from website and see if it updated correctly in databse