-- 4a. Number of unique film titles

CREATE OR REPLACE VIEW movies_app.v_unique_titles AS
    SELECT COUNT(DISTINCT title) AS unique_titles_count 
    FROM movies_app.movie;


SELECT * FROM movies_app.v_unique_titles;
