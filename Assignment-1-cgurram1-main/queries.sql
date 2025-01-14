-- Query 1 --
CREATE TABLE query1 AS
SELECT COUNT(*) AS "count of comments"
FROM comments
WHERE author = 'xymemez';


-- Query 2
CREATE TABLE query2 AS
SELECT subreddit_type AS "subreddit type", COUNT(*) AS "subreddit count"
FROM subreddits
GROUP BY subreddit_type;

-- Query 3 
CREATE TABLE query3 AS
SELECT subreddit AS "name",
       COUNT(id) AS "comments count",
       ROUND(AVG(score), 2) AS "average score"
FROM comments
GROUP BY subreddit
ORDER BY COUNT(id) DESC
LIMIT 10;

-- Query 4 -- 
CREATE TABLE query4 AS
SELECT name AS "name",
       link_karma AS "link karma",
       comment_karma AS "comment karma",
       CASE
           WHEN link_karma >= comment_karma THEN 1
           ELSE 0
       END AS "label"
FROM authors
WHERE (link_karma + comment_karma) / 2 > 1000000
ORDER BY (link_karma + comment_karma) / 2 DESC;

-- Query 5 --
CREATE TABLE query5 AS
SELECT s.subreddit_type AS "sr type",
       COUNT(c.id) AS "comments num"
FROM comments c
JOIN subreddits s ON c.subreddit_id = s.name
WHERE c.author = '[deleted_user]'
GROUP BY s.subreddit_type;