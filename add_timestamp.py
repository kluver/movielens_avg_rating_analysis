import pandas as pd
import json
from os.path import isfile, join
from pathlib import Path
import sys
from sqlalchemy import create_engine, text

if len(sys.argv) != 2:
    print("oh no. call with filename tho")
    sys.exit(1)
input_file = sys.argv[1]
movie_id = int(input_file[19:-6])


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


historic_ratings = []

query = text(
    """
    SELECT userId, rating, system_tstamp
    FROM user_rating_pairs_history
    where movieId = :movieID
    order by system_tstamp asc;
"""
)

params = {"movieID": movie_id}

with engine.connect() as connection:
    result = connection.execute(query, params)

    # Stores the results in the avg_ratings dictionary
    for row in result.fetchall():
        historic_ratings.append(tuple(row))

events_df = pd.read_json(input_file, lines=True)
events_df.sort_values(by=["timestamp"], inplace=True, ascending=True)
events_df["avg_rating"] = 0


row_num = 0
rating_num = 0

while row_num < len(events_df):
    next_rating = None
    rating_tstamp = None
    if rating_num < len(historic_ratings):
        next_rating = historic_ratings[rating_num]
        rating_tstamp = next_rating[2]

    
    next_row = events_df.iloc[row_num]
    row_tstamp = next_row["timestamp"]
## added in the block below to convert both times to UTC format 
    if rating_tstamp and row_tsamp:
        if rating_tstamp.tzinfo is None:
            rating_tstamp = rating_tstamp.replace(tzinfo=pytz.UTC)
        if row_tstamp.tzinfo is None:
            row_tstamp = row_tstamp.replace(tzinfo=pytz.UTC)
    print(
        rating_num,
        rating_tstamp.tzinfo,
        row_num,
        row_tstamp.tzinfo,
        (rating_tstamp - row_tstamp) if rating_tstamp and row_tstamp else "N/A", ## added in to check whether or not the timestamps exist 
        sep="\t",
    )
    if rating_tstamp is not None and rating_tstamp < row_tstamp:
        # advance rating
        rating_num += 1
        pass
    else:
        # advance row
        row_num += 1
        pass
