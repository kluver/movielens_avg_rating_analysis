out/filtered_rating_events.jsonl: process_action_logs.py action_logs/*
	pipenv run python3 process_action_logs.py

out/by_movie/.done: out/filtered_rating_events.jsonl
	pipenv run python3 split_files.py
	touch out/by_movie/.done
