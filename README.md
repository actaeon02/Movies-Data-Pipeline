# 🎬 Movies Data Engineering Take-Home Test

![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-blue?logo=postgresql)
![Python](https://img.shields.io/badge/Language-Python_3.x-yellow?logo=python)
![ETL](https://img.shields.io/badge/Pipeline-ETL-green)
![Status](https://img.shields.io/badge/Status-Completed-success)

---

## 📘 Overview

This project demonstrates an end-to-end **data engineering workflow** — from **data modeling** to **ETL** and **analytics** — using a dataset containing movie information and ratings.

**Objectives**
1. Design a conceptual and logical data model for movies and related entities.  
2. Implement the model in a relational database (PostgreSQL).  
3. Build an ETL pipeline to ingest the provided `movies.csv` dataset.  
4. Write analytical SQL queries (and optional views) to answer business questions.

---

## 🧩 Project Structure

```plaintext
movies_takehome/
│
├── data/
│   └── movies.csv                 # Provided dataset
│
├── model/
│   ├── ERD_Conceptual.png         # Conceptual data model
│   ├── ERD_Logical.png            # Logical data model
│   ├── ERD_Physical.png           # Physical data model
│   └── ddl_postgres.sql           # Database schema DDL
│
├── etl/
│   └── etl_load_data.py           # Python ETL script
│
├── queries/
│   ├── 4a.sql                     # Business query (4a)
│   ├── 4b.sql                     # Business query (4b)
│   ├── 4c.sql                     # Business query (4c)
│   ├── 4d.sql                     # Business query (4d)
│   └── 4e.sql                     # Business query (4e)
│
├── README.md                      # Documentation
└── requirements.txt               # Python dependencies
```


## ⚙️ Tech Stack

- **Database:** PostgreSQL (can also use SQL Server / Oracle / etc.)  
- **Programming Language:** Python 3.x  
- **Libraries:** `pandas`, `psycopg2` (or `SQLAlchemy`)  
- **Diagram Tools:** [Visual Paradigm Online](https://online.visual-paradigm.com) & [SQLFlow](https://sqlflow.gudusoft.com)  

> Refer to `requirements.txt` for detailed package.

---

## 🧠 Data Model Design

### 🧩 Conceptual Model
The conceptual model defines high-level entities and their relationships:

- 🎞️ **Movie** → central entity containing metadata (title, year range, gross, rating, etc.)  
- 🎭 **Genre** → multiple genres per movie (many-to-many)  
- 🎬 **Director** → one or more directors per movie  
- 👥 **Actor** → one or more actors per movie  

---

### 🧱 Logical / Physical Model

#### Example — Main Movie Table
```sql
CREATE TABLE movies_app.movie (
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
```

### 📅 Why year_start and year_end
The year field in the dataset sometimes shows a range (For Series) — for example:

`The Walking Dead,(2010–2022)`

To represent both movies and TV series accurately:

- **year_start** stores the year of release.
- **year_end** stores the final year (nullable for movies).


**🔄 ETL Flow**
The ETL script (etl_load_data.py) performs the following steps:

**1. Extract**
   
    - Reads movies.csv using pandas.
    - Normalizes text encodings and removes whitespace.

**2. Transform**
   
    - Cleans malformed characters (e.g., â€“ → –).
    - Parses year field into year_start and year_end.
    - Converts numeric columns (gross, runtime) into consistent types.
    - Optionally stores the raw row as JSON for traceability.

**3. Load**

    - Connects to PostgreSQL via psycopg2 or SQLAlchemy.
    - Creates tables if not exist (using create_tables.sql).
    - Loads cleaned records into movies_app.movie.


## 🧮 Analytical Queries

#### File: queries/*

**(4a) — Number of unique film titles**
```sql
    SELECT COUNT(DISTINCT title) AS unique_titles FROM movies_app.movie;
```

**(4b) — Films starring Lena Headey**
```sql
    SELECT m.title, m.year_start AS year_of_release, m.rating
    FROM movies_app.movie m
    JOIN movies_app.movie_actor ma ON ma.movie_id = m.movie_id
    JOIN movies_app.actor a ON a.actor_id = ma.actor_id
    WHERE LOWER(a.name) = LOWER('Lena Headey')
    ORDER BY m.year_start;
```

**(4c) — The name of the director and total gross of the films that have been directed.**
```sql
    SELECT d.name AS director_name, SUM(m.gross) AS total_gross
    FROM movies_app.director d
    JOIN movies_app.movie_director md ON md.director_id = d.director_id
    JOIN movies_app.movie m ON m.movie_id = md.movie_id
    GROUP BY d.name
    ORDER BY total_gross DESC NULLS LAST;
```

**(4d) — Top 5 comedy films by gross**
```sql
    SELECT m.title, m.year_start AS year_of_release, m.rating, m.gross
    FROM movies_app.movie m
    JOIN movies_app.movie_genre mg ON mg.movie_id = m.movie_id
    JOIN movies_app.genre g ON g.genre_id = mg.genre_id
    WHERE LOWER(g.name) = LOWER('comedy')
    ORDER BY m.gross DESC NULLS LAST
    LIMIT 5;
```

**(4e) — Films directed by Martin Scorsese starring Robert De Niro**
```sql
    SELECT DISTINCT m.title, m.year_start AS year_of_release, m.rating
    FROM movies_app.movie m
    JOIN movies_app.movie_director md ON md.movie_id = m.movie_id
    JOIN movies_app.director d ON d.director_id = md.director_id
    JOIN movies_app.movie_actor ma ON ma.movie_id = m.movie_id
    JOIN movies_app.actor a ON a.actor_id = ma.actor_id
    WHERE LOWER(d.name) = LOWER('Martin Scorsese')
    AND LOWER(a.name) = LOWER('Robert De Niro')
    ORDER BY m.year_start;
```

## 🧾 Execution Flow

1. **Model Design** — Review the conceptual ERD and generate the SQL schema definitions.  
2. **Database Setup** — Deploy a PostgreSQL instance (locally via Docker or in the cloud).  
3. **Schema Initialization** — Execute `model/ddl_postgres.sql` to create all required tables.  
4. **Data Ingestion** — Run `etl/etl_load_data.py` to extract, transform, and load data from `movies.csv`.  
5. **Data Validation** — Verify record counts and inspect sample data using `tests/data_quality_checks.sql`.  
6. **Analytics & Queries** — Execute SQL scripts under `queries/` to generate analytical results.  
7. (Optional) Deploy stored procedures/views for recurring reports.


## 👤 Author

**Mikael Andrew**

*`(Created for CAD-IT Technical Take-Home Test)`*

