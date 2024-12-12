import pandas as pd
import sys
from pathlib import Path

# Ensures a directory is provided as an argument
if len(sys.argv) != 2:
    print("Usage: python merging.py <directory_with_jsonl_files>")
    sys.exit(1)

# Get the directory from the command-line argument
directory = Path(sys.argv[1])

# Ensure the directory exists
if not directory.is_dir():
    print(f"Error: Directory '{directory}' not found.")
    sys.exit(1)

# Define the subfolder where processed movie files are located
processed_folder = directory / "by_movie"

# Ensure the subfolder exists
if not processed_folder.is_dir():
    print(f"Error: 'by_movie' folder not found in '{directory}'.")
    sys.exit(1)

# List all processed movie files in the 'by_movie' folder
jsonl_files = list(processed_folder.glob("processed_movie_*.jsonl"))

# Check if any files were found
if not jsonl_files:
    print(f"Error: No processed movie files found in '{processed_folder}'.")
    sys.exit(1)

# Initialize an empty DataFrame to hold the merged data
merged_df = pd.DataFrame()

# Loop through all jsonl files and merge them
for file in jsonl_files:
    try:
        # Read the current jsonl file
        df = pd.read_json(file, lines=True)
        # Append the data to the merged DataFrame
        merged_df = pd.concat([merged_df, df], ignore_index=True)
        print(f"Successfully added data from: {file}")
    except Exception as e:
        print(f"Error reading {file}: {e}")

# Save the merged data to a CSV file
output_file = directory / "merged_data.csv"
merged_df.to_csv(output_file, index=False)
print(f"Merged data saved to: {output_file}")