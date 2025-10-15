-- 4d. Film Title, Year of Release, and Rating of 5 films that have comedy genre with the largest gross

CREATE OR REPLACE VIEW movies_app.v_top5_comedy_by_gross AS
    SELECT m.title, m.year_start AS year_of_release, m.rating, m.gross
    FROM movies_app.movie m
    JOIN movies_app.movie_genre mg ON mg.movie_id = m.movie_id
    JOIN movies_app.genre g ON g.genre_id = mg.genre_id
    WHERE LOWER(g.name) = LOWER('comedy')
    ORDER BY m.gross DESC NULLS LAST
    LIMIT 5;

SELECT * FROM movies_app.v_top5_comedy_by_gross;