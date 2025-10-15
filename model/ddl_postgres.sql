CREATE SCHEMA IF NOT EXISTS movies_app;

CREATE TABLE IF NOT EXISTS movies_app.movie (
  movie_id      TEXT PRIMARY KEY,
  title         TEXT NOT NULL,
  year_start    INT,
  year_end      INT,
  rating        FLOAT,
  gross         NUMERIC(14,2),
  runtime_min   INT,
  description   TEXT,
  raw_row       JSONB,
  created_at    TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS movies_app.director (
  director_id   TEXT PRIMARY KEY,
  name          TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movies_app.actor (
  actor_id      TEXT PRIMARY KEY,
  name          TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movies_app.genre (
  genre_id      TEXT PRIMARY KEY,
  name          TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movies_app.movie_director (
  movie_id      TEXT NOT NULL REFERENCES movies_app.movie(movie_id) ON DELETE CASCADE,
  director_id   TEXT NOT NULL REFERENCES movies_app.director(director_id),
  PRIMARY KEY (movie_id, director_id)
);

CREATE TABLE IF NOT EXISTS movies_app.movie_actor (
  movie_id      TEXT NOT NULL REFERENCES movies_app.movie(movie_id) ON DELETE CASCADE,
  actor_id      TEXT NOT NULL REFERENCES movies_app.actor(actor_id),
  billing_order SMALLINT,
  PRIMARY KEY (movie_id, actor_id)
);

CREATE TABLE IF NOT EXISTS movies_app.movie_genre (
  movie_id      TEXT NOT NULL REFERENCES movies_app.movie(movie_id) ON DELETE CASCADE,
  genre_id      TEXT NOT NULL REFERENCES movies_app.genre(genre_id),
  PRIMARY KEY (movie_id, genre_id)
);

-- Indexes for performance on common queries
CREATE INDEX IF NOT EXISTS idx_movie_title ON movies_app.movie(LOWER(title));
CREATE INDEX IF NOT EXISTS idx_movie_year_start ON movies_app.movie(year_start);
CREATE INDEX IF NOT EXISTS idx_movie_gross ON movies_app.movie(gross DESC);
