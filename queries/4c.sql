-- 4c. The name of the director and total gross of the films that have been directed.

CREATE OR REPLACE VIEW movies_app.v_director_total_gross AS
    SELECT d.name AS director_name, SUM(m.gross) AS total_gross
    FROM movies_app.director d
    JOIN movies_app.movie_director md ON md.director_id = d.director_id
    JOIN movies_app.movie m ON m.movie_id = md.movie_id
    GROUP BY d.name
    ORDER BY total_gross DESC NULLS LAST;

SELECT * FROM movies_app.v_director_total_gross;