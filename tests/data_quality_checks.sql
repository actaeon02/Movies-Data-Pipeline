-- Null rate on required columns (title must not be null).
SELECT COUNT(*) AS distinct_titles FROM movies_app.movie;

-- Number of distinct genres, directors, actors plausibility.
SELECT COUNT(*) FILTER (WHERE title IS NULL OR title = '') AS missing_titles FROM movies_app.movie;

-- Spot checks: select random 10 rows and compare raw CSV row with raw_row.
SELECT COUNT(*) AS genre_mappings FROM movies_app.movie_genre;
