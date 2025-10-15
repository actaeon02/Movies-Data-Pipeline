-- 4b. Film Title, Year of Release, and Rating of the film starring Lena Headey Sort By Year of Release

CREATE OR REPLACE VIEW movies_app.v_films_with_lena_headey AS
    SELECT m.title, m.year_start AS year_of_release, m.rating
    FROM movies_app.movie m
    JOIN movies_app.movie_actor ma ON ma.movie_id = m.movie_id
    JOIN movies_app.actor a ON a.actor_id = ma.actor_id
    WHERE LOWER(a.name) = LOWER('Lena Headey')
    ORDER BY m.year_start;

SELECT *
FROM movies_app.v_films_with_lena_headey;