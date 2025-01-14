
ALTER TABLE authors
ADD CONSTRAINT unique_author_name
UNIQUE (name);

ALTER TABLE subreddits
ADD CONSTRAINT unique_subreddit_name
UNIQUE (name);

ALTER TABLE submissions
ADD CONSTRAINT fk_submissions_author
FOREIGN KEY (author) REFERENCES authors(name);

ALTER TABLE submissions
ADD CONSTRAINT fk_submissions_subreddit
FOREIGN KEY (subreddit_id) REFERENCES subreddits(name);


ALTER TABLE comments
ADD CONSTRAINT fk_comments_author
FOREIGN KEY (author) REFERENCES authors(name);

ALTER TABLE comments
ADD CONSTRAINT fk_comments_subreddit
FOREIGN KEY (subreddit_id) REFERENCES subreddits(name);
