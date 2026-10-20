-- Hive (HiveQL) version of the analysis. Load first:  hdfs dfs -put data/raw/*.dat /movies/
CREATE EXTERNAL TABLE ratings_raw (line STRING) LOCATION '/movies/ratings/';
CREATE EXTERNAL TABLE movies_raw  (line STRING) LOCATION '/movies/movies/';
CREATE VIEW ratings AS SELECT p[0] uid, p[1] mid, CAST(p[2] AS INT) rating, CAST(p[3] AS BIGINT) ts
  FROM (SELECT split(line,'::') p FROM ratings_raw) t WHERE size(p)>=4;
CREATE VIEW movie_genre AS SELECT p[0] mid, p[1] title, g genre
  FROM (SELECT split(line,'::') p FROM movies_raw) t LATERAL VIEW explode(split(coalesce(p[2],'Unknown'),'\\|')) x AS g;
-- 1. most-rated movies
SELECT mid, count(*) n, round(avg(rating),2) avg_r FROM ratings GROUP BY mid ORDER BY n DESC LIMIT 20;
-- 2. highest-rated genres
SELECT genre, count(*) n, round(avg(rating),2) avg_r FROM ratings r JOIN movie_genre m ON r.mid=m.mid GROUP BY genre ORDER BY avg_r DESC;
-- 3. most active users
SELECT uid, count(*) n, round(avg(rating),2) avg_r FROM ratings GROUP BY uid ORDER BY n DESC LIMIT 20;
-- 4. genre-wise preference: each heavy user's favourite genre
SELECT genre, count(*) users FROM (SELECT uid, genre, row_number() OVER (PARTITION BY uid ORDER BY count(*) DESC) rk
  FROM ratings r JOIN movie_genre m ON r.mid=m.mid GROUP BY uid, genre) t WHERE rk=1 GROUP BY genre ORDER BY users DESC;
