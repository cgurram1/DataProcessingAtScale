#!/bin/bash

echo "Assignment-1: Database Creation & Loading & Querying Started...."
echo "Running create_tables.sql..."
psql -U postgres -d postgres -f create_tables.sql


echo "Loading data into tables..."

# Loading data into 'authors' table
psql -U postgres -d postgres -c "\COPY authors (id, retrieved_on, name, created_utc, link_karma, comment_karma, profile_img, profile_color, profile_over_18) FROM 'authors.csv' DELIMITER ',' CSV HEADER;"

# Loading data into 'subreddits' table
psql -U postgres -d postgres -c "\COPY subreddits (banner_background_image, created_utc, description, display_name, header_img, hide_ads, id, over_18, public_description, retrieved_utc, name, subreddit_type, subscribers, title, whitelist_status) FROM 'subreddits.csv' DELIMITER ',' CSV HEADER;"

# Loading data into 'submissions' table
psql -U postgres -d postgres -c "\COPY submissions (downs, url, id, edited, num_reports, created_utc, name, title, author, permalink, num_comments, likes, subreddit_id, ups) FROM 'submissions.csv' DELIMITER ',' CSV HEADER;"

# Loading data into 'comments' table
psql -U postgres -d postgres -c "\COPY comments (distinguished, downs, created_utc, controversiality, edited, gilded, author_flair_css_class, id, author, retrieved_on, score_hidden, subreddit_id, score, name, author_flair_text, link_id, archived, ups, parent_id, subreddit, body) FROM 'comments.csv' DELIMITER ',' CSV HEADER;"

echo "Running create_relations.sql..."
psql -U postgres -d postgres -f create_relations.sql

echo "Running queries.sql..."
psql -U postgres -d postgres -f queries.sql

echo "Assignment-1: Database Creation & Loading & Querying Completed"