-- 4e. Film Title, Year of Release and Rating of the film directed by Martin Scorsese and starring Robert De Niro

CREATE OR REPLACE VIEW movies_app.v_scorsese_deniro_films AS
    SELECT DISTINCT m.title, m.year_start AS year_of_release, m.rating
    FROM movies_app.movie m
    JOIN movies_app.movie_director md ON md.movie_id = m.movie_id
    JOIN movies_app.director d ON d.director_id = md.director_id
    JOIN movies_app.movie_actor ma ON ma.movie_id = m.movie_id
    JOIN movies_app.actor a ON a.actor_id = ma.actor_id
    WHERE LOWER(d.name) = LOWER('Martin Scorsese')
    AND LOWER(a.name) = LOWER('Robert De Niro')
    ORDER BY m.year_start;

SELECT * FROM movies_app.v_scorsese_deniro_films;