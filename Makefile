out/filtered_rating_events.jsonl: process_action_logs.py action_logs/*
	pipenv run python3 process_action_logs.py

out/by_movie/.done: out/filtered_rating_events.jsonl
	pipenv run python3 split_files.py
	touch out/by_movie/.done

# Rule for adding timestamps to each movie_id
out/processed_with_timestamp/.done: out/by_movie/.done
	pipenv run python3 add_timestamp.py
	touch out/processed_with_timestamp/.done

# Rule for merging all processed data
out/merged_data.csv: out/by_movie/.done
	pipenv run python3 merging.py out/by_movie/
	touch out/merged_data.csv
