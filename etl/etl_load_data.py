import pandas as pd
import sqlalchemy as sa
import re
import json
import uuid
from sqlalchemy.dialects.postgresql import insert
from pathlib import Path
from typing import Optional, List, Any, Dict

# Database configuration
DB_URL = "postgresql+psycopg2://msyadm:skt890:)@localhost:5432/postgres"
CSV_PATH = Path("data/movies.csv")


def clean_numeric(value: Any) -> Optional[float]:
    """
    Clean and convert a value to numeric format.
    
    Args:
        value: Input value to clean (string, int, float, etc.)
        
    Returns:
        Cleaned numeric value as float, or None if conversion fails
    """
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    
    value_str = str(value).strip()
    # Remove non-numeric characters except decimal point and minus sign
    value_str = re.sub(r'[^0-9\.-]', '', value_str)
    
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return None


def split_multi_value(value: Any) -> List[str]:
    """
    Split multi-value strings into lists using common delimiters.
    
    Args:
        value: Input value containing delimited strings
        
    Returns:
        List of cleaned and stripped strings
    """
    if pd.isna(value):
        return []
    if isinstance(value, list):
        return value

    # Split by common delimiters: semicolon, pipe, or comma
    parts = re.split(r';|\||,', str(value))
    return [part.strip() for part in parts if part.strip()]


def generate_uuid_id(df: pd.DataFrame) -> pd.DataFrame:
    """Generates a unique UUID for each row and sets it as the 'movie_id'."""
    # Generate a unique UUID for every row as a string
    df["movie_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    print(f"Generated {len(df)} unique UUIDs.")
    return df


def upsert_entity(conn: sa.engine.Connection, table: sa.Table, name: str) -> int:
    """
    Upsert an entity and return its ID.
    
    Args:
        conn: Database connection
        table: SQLAlchemy table object
        name: Entity name to upsert
        
    Returns:
        Entity ID from the database
    """
    # Determine primary key column name based on table type
    if table.name == 'genre':
        pk_column = table.c.genre_id
    elif table.name == 'director':
        pk_column = table.c.director_id
    elif table.name == 'actor':
        pk_column = table.c.actor_id
    else:
        raise ValueError(f"Unknown table type: {table.name}")

    # Clean the name first
    clean_name = str(name).strip()
    if not clean_name or clean_name.lower() == "nan":
        raise ValueError(f"Invalid entity name: {name}")

    # First try to find existing entity
    select_stmt = sa.select(pk_column).where(table.c.name == clean_name)
    existing_row = conn.execute(select_stmt).fetchone()

    if existing_row:
        return existing_row[0]

    # If not exists, insert new entity with a generated UUID
    entity_uuid = str(uuid.uuid4())
    print(f"Generated new UUID for {table.name} '{clean_name}': {entity_uuid}")

    try:
        insert_stmt = (
            insert(table)
            .values(**{'name':clean_name, pk_column.name: entity_uuid})
            .on_conflict_do_nothing(index_elements=["name"])
            .returning(pk_column)
        )

        result = conn.execute(insert_stmt)
        row = result.fetchone()

        if row:
            return row[0]

        # If insert returned nothing (due to conflict), try select again
        existing_row = conn.execute(select_stmt).fetchone()
        if existing_row:
            return existing_row[0]
        raise ValueError(f"Failed to upsert entity: {clean_name}")

    except Exception as e:
        # If there's still a conflict, try one more time to select
        existing_row = conn.execute(select_stmt).fetchone()
        if existing_row:
            return existing_row[0]
        raise e


def extract_year_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and clean year ranges from the year column.
    
    Args:
        df: DataFrame containing year data in parentheses
        
    Returns:
        DataFrame with start_year and end_year columns added
    """
    # Extract content within parentheses (e.g., '2010–2022' or '2013– ')
    year_range_full = df['year'].astype(str).str.extract(r'\((.*?)\)').iloc[:, 0]
    
    # Split into start and end years
    df[['start_year', 'end_year_temp']] = year_range_full.str.split(r'[–-]', expand=True)
    
    # Convert to numeric types
    df['start_year'] = (
        pd.to_numeric(df['start_year'], errors='coerce', downcast='integer')
        .astype('Int64')
    )
    
    df['end_year'] = (
        pd.to_numeric(
            df['end_year_temp'].str.replace(r'\s+', '', regex=True), 
            errors='coerce', 
            downcast='integer'
        )
        .astype('Int64')
    )
    
    # Clean up temporary columns
    df.drop(columns=['year', 'end_year_temp'], inplace=True)
    
    return df


def clean_genre_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the genre column by removing newlines and extra whitespace.
    
    Args:
        df: DataFrame with genre column
        
    Returns:
        DataFrame with cleaned genre column
    """
    df['genre'] = (
        df['genre']
        .astype(str)
        .str.replace('\n', '', regex=False)
        .str.strip()
    )
    return df


def clean_one_line_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the one-line description column.
    
    Args:
        df: DataFrame with one-line column
        
    Returns:
        DataFrame with cleaned one-line column
    """
    df['one-line'] = (
        df['one-line']
        .astype(str)
        .str.replace('\n', ' ', regex=False)
        .str.strip()
    )
    return df


def extract_director_and_stars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract director and stars from the combined stars column.
    
    Args:
        df: DataFrame with combined stars/director column
        
    Returns:
        DataFrame with separate director and stars columns
    """
    # Clean the stars column first
    df['stars_cleaned'] = (
        df['stars']
        .astype(str)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )
    
    # Extract director information
    df['director'] = (
        df['stars_cleaned']
        .str.extract(
            r'Director:\s*(.*?)(?=\s*\|\s*Stars:|\s*Stars:|\Z)', 
            flags=re.IGNORECASE
        )
        .iloc[:, 0]
        .str.strip()
    )
    
    # Extract stars/cast information
    df['stars_cast'] = (
        df['stars_cleaned']
        .str.extract(r'Stars:\s*(.*?)\s*$', flags=re.IGNORECASE)
        .iloc[:, 0]
        .str.strip()
    )
    
    # Clean up temporary columns and rename
    df.drop(columns=['stars', 'stars_cleaned'], inplace=True)
    df.rename(columns={'stars_cast': 'stars'}, inplace=True)
    
    # Handle missing values
    df.fillna({'director': 'Unknown'}, inplace=True)
    df.fillna({'stars': 'Unknown'}, inplace=True)
    
    return df


def fix_column_shift(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix data shift issues in votes, runtime, and gross columns.
    
    Args:
        df: DataFrame with potentially shifted columns
        
    Returns:
        DataFrame with corrected column values
    """
    # Clean votes column for analysis
    df['votes_cleaned'] = (
        df['votes']
        .astype(str)
        .str.replace(r'[^\d,]', '', regex=True)
    )
    
    # Identify rows with potential data shift (single comma in votes)
    has_shift = df['votes_cleaned'].str.count(',').eq(1)
    
    # Create temporary columns for correction
    temp_columns = ['true_votes', 'true_runtime', 'true_gross']
    for col in temp_columns:
        df[col] = None
    
    # Apply corrections only to shifted rows
    if has_shift.any():
        split_values = df.loc[has_shift, 'votes_cleaned'].str.split(',', expand=True)
        
        # Correct the data shift
        df.loc[has_shift, 'true_votes'] = split_values[0]  # First part becomes votes
        df.loc[has_shift, 'true_runtime'] = split_values[1]  # Second part becomes runtime
        df.loc[has_shift, 'true_gross'] = df.loc[has_shift, 'runtime']  # Runtime becomes gross
    
    # Update original columns with corrected values where applicable
    mask = ~df['true_votes'].isna()
    df.loc[mask, 'votes'] = df.loc[mask, 'true_votes']
    df.loc[mask, 'runtime'] = df.loc[mask, 'true_runtime']
    df.loc[mask, 'gross'] = df.loc[mask, 'true_gross']
    
    # Clean up temporary columns
    df.drop(columns=['votes_cleaned'] + temp_columns, errors='ignore', inplace=True)
    
    return df


def ensure_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure critical columns are properly typed as numeric.
    
    Args:
        df: DataFrame with columns to convert
        
    Returns:
        DataFrame with properly typed numeric columns
    """
    # Replace NA values with appropriate defaults
    df['start_year'] = pd.to_numeric(df['start_year'], errors='coerce').fillna(0).astype(int)
    df['end_year'] = pd.to_numeric(df['end_year'], errors='coerce').fillna(0).astype(int)
    df['runtime'] = pd.to_numeric(df['runtime'], errors='coerce').fillna(0).astype(int)

    numeric_columns = {
        'votes': 'Int64',
        'runtime': 'Int64', 
        'gross': float,
        'rating': float
    }

    for column, dtype in numeric_columns.items():
        df[column] = pd.to_numeric(df[column], errors='coerce').astype(dtype)

    # Fill missing gross values with 0
    df.fillna({'gross': 0}, inplace=True)

    return df


def prepare_json_data(row: pd.Series) -> Dict[str, Any]:
    """
    Prepare row data for JSON storage, handling special characters and NA values.

    Args:
        row: DataFrame row to convert to JSON

    Returns:
        Dictionary suitable for JSON serialization
    """
    json_data = {}

    for col in row.index:
        value = row[col]

        # Handle pandas NA values
        if pd.isna(value):
            json_data[col] = None
        # Handle numeric values
        elif isinstance(value, (int, float)):
            json_data[col] = value
        # Handle string values - escape quotes properly
        elif isinstance(value, str):
            # Clean the string: remove extra whitespace and handle special characters
            cleaned_value = value.strip()
            # Use json.dumps for proper string escaping instead of manual replacement
            json_data[col] = cleaned_value
        else:
            json_data[col] = str(value)

    return json_data


def upsert_movie_data(conn: sa.engine.Connection, row: pd.Series, 
                     movie_table: sa.Table, genre_table: sa.Table,
                     director_table: sa.Table, actor_table: sa.Table,
                     mgenre: sa.Table, mdirector: sa.Table, mactor: sa.Table) -> None:
    """
    Upsert movie data and its relationships into the database.
    
    Args:
        conn: Database connection
        row: Movie data row
        movie_table: Movies table
        genre_table: Genres table
        director_table: Directors table
        actor_table: Actors table
        mgenre: Movie-genre relationship table
        mdirector: Movie-director relationship table
        mactor: Movie-actor relationship table
    """
    # Extract basic movie information
    movie_id = row.get('movie_id')
    title = str(row.get('movies')).strip()
    start_year = row.get('start_year')
    end_year = row.get('end_year')
    rating = row.get('rating')
    gross = row.get('gross')
    runtime_min = row.get('runtime')

    # Prepare JSON data safely
    json_data_dict = prepare_json_data(row)
    json_string = json.dumps(json_data_dict, ensure_ascii=False)

    # Find or create movie
    movie_id = _find_or_create_movie(
        conn, movie_table, movie_id, title, start_year, end_year, 
        rating, gross, runtime_min, json_string
    )

    # Handle relationships
    _upsert_movie_relationships(
        conn, row, movie_id, genre_table, director_table, actor_table,
        mgenre, mdirector, mactor
    )


def _find_or_create_movie(conn: sa.engine.Connection, movie_table: sa.Table,
                         movie_id: str, title: str, start_year: int, end_year: int,
                         rating: float, gross: float, runtime_min: int,
                         json_data: str) -> int:
    """
    Find existing movie or create new one.
    
    Returns:
        Movie ID
    """
    try:
        # Build query to find existing movie
        query = sa.select(movie_table.c.movie_id).where(
            sa.func.lower(movie_table.c.title) == title.lower()
        )

        if start_year is not None:
            query = query.where(movie_table.c.year_start == start_year)

        existing = conn.execute(query).fetchone()

        if existing:
            movie_id = existing[0]
            # Update existing movie
            update_stmt = (
                movie_table.update()
                .where(movie_table.c.movie_id == movie_id)
                .values(
                    rating=rating if rating is not None else sa.null(),
                    gross=gross if gross is not None else sa.null(),
                    runtime_min=runtime_min if runtime_min is not None else sa.null(),
                    raw_row=sa.cast(json_data, sa.JSON),
                )
            )
            conn.execute(update_stmt)
            return movie_id
        else:
            # Insert new movie
            insert_stmt = (
                movie_table.insert()
                .values(
                    movie_id=movie_id,
                    title=title,
                    year_start=start_year if start_year is not None else sa.null(),
                    year_end=end_year if end_year is not None else sa.null(),
                    rating=rating if rating is not None else sa.null(),
                    gross=gross if gross is not None else sa.null(),
                    runtime_min=runtime_min if runtime_min is not None else sa.null(),
                    raw_row=sa.cast(json_data, sa.JSON),
                )
                .returning(movie_table.c.movie_id)
            )
            result = conn.execute(insert_stmt)
            return result.fetchone()[0]
    except Exception as e:
        # If there's an error, raise it with more context
        raise Exception(f"Error processing movie '{title}': {str(e)}")


def _upsert_movie_relationships(conn: sa.engine.Connection, row: pd.Series,
                               movie_id: int, genre_table: sa.Table,
                               director_table: sa.Table, actor_table: sa.Table,
                               mgenre: sa.Table, mdirector: sa.Table,
                               mactor: sa.Table) -> None:
    """
    Upsert movie relationships (genres, directors, actors).
    """
    # Handle genres
    genres = split_multi_value(row.get('genre', ''))
    seen_genres = set()
    for genre_name in genres:
        if genre_name and genre_name.strip() and genre_name.lower() != 'nan':
            if genre_name not in seen_genres:
                try:
                    genre_id = upsert_entity(conn, genre_table, genre_name)
                    _insert_relationship(conn, mgenre, {'movie_id': movie_id, 'genre_id': genre_id})
                    seen_genres.add(genre_name)
                except Exception as e:
                    print(f"Warning: Failed to upsert genre '{genre_name}': {e}")
                    continue

    # Handle directors
    directors = split_multi_value(row.get('director', ''))
    seen_directors = set()
    for director_name in directors:
        if director_name and director_name.strip() and director_name.lower() != 'nan' and director_name.lower() != 'unknown':
            if director_name not in seen_directors:
                try:
                    director_id = upsert_entity(conn, director_table, director_name)
                    _insert_relationship(conn, mdirector, {'movie_id': movie_id, 'director_id': director_id})
                    seen_directors.add(director_name)
                except Exception as e:
                    print(f"Warning: Failed to upsert director '{director_name}': {e}")
                    continue

    # Handle actors (limit to first 20)
    actors = split_multi_value(row.get('stars', ''))[:20]
    seen_actors = set()
    for actor_name in actors:
        if actor_name and actor_name.strip() and actor_name.lower() != 'nan' and actor_name.lower() != 'unknown':
            if actor_name not in seen_actors:
                try:
                    actor_id = upsert_entity(conn, actor_table, actor_name)
                    _insert_relationship(conn, mactor, {'movie_id': movie_id, 'actor_id': actor_id})
                    seen_actors.add(actor_name)
                except Exception as e:
                    print(f"Warning: Failed to upsert actor '{actor_name}': {e}")
                    continue


def _insert_relationship(conn: sa.engine.Connection, table: sa.Table, 
                        values: Dict[str, int]) -> None:
    """
    Safely insert relationship data, ignoring duplicates.
    """
    try:
        conn.execute(table.insert().values(**values))
    except Exception:
        # Relationship likely already exists
        pass


def test_database_connection() -> bool:
    """
    Test database connection before proceeding with data processing.
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        engine = sa.create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        print("Database connection successful")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        print("Please check your database URL and ensure PostgreSQL is running")
        return False


def main():
    """
    Main function to load, clean, and import movie data into database.
    """
    # Test database connection first
    if not test_database_connection():
        return

    # Initialize database connection and metadata
    engine = sa.create_engine(DB_URL)
    try:
        # Test reflection with a temporary connection
        with engine.connect() as test_conn:
            meta = sa.MetaData(schema="movies_app")
            meta.reflect(bind=test_conn)
    except Exception as e:
        print(f"Error reflecting database metadata: {e}")
        print("Please ensure the database and schema exist")
        return

    # Get table references
    try:
        movie_table = meta.tables["movies_app.movie"]
        genre_table = meta.tables["movies_app.genre"]
        director_table = meta.tables["movies_app.director"]
        actor_table = meta.tables["movies_app.actor"]
        mgenre = meta.tables["movies_app.movie_genre"]
        mdirector = meta.tables["movies_app.movie_director"]
        mactor = meta.tables["movies_app.movie_actor"]
    except KeyError as e:
        print(f"Required table not found: {e}")
        print("Please ensure all required tables exist in the movies_app schema")
        return

    try:
        # Load CSV data
        df = pd.read_csv(CSV_PATH)
        print("Initial data sample:")
        print(df.head(5))

    except FileNotFoundError:
        print(f"Error: The file '{CSV_PATH}' was not found.")
        return
    except Exception as e:
        print(f"An unexpected error occurred while reading the file: {e}")
        return

    # Data cleaning pipeline
    cleaning_steps = [
        ("Standardizing column names", 
         lambda df: df.rename(columns={c: c.strip().lower().replace(' ', '_') for c in df.columns})),
        ("Generating unique movie IDs", generate_uuid_id), 
        ("Extracting year ranges", extract_year_ranges),
        ("Cleaning genre column", clean_genre_column),
        ("Cleaning one-line descriptions", clean_one_line_column),
        ("Extracting director and stars", extract_director_and_stars),
        ("Fixing column shifts", fix_column_shift),
        ("Ensuring numeric columns", ensure_numeric_columns),
        ("Removing duplicates", lambda df: df.drop_duplicates(inplace=True) or df)
    ]

    # Execute cleaning pipeline
    for step_name, step_function in cleaning_steps:
        print(f"Executing: {step_name}")
        df = step_function(df)

    print("Final cleaned data sample:")
    print(df.head(5))

    # Import data to database
    successful_count = 0
    error_count = 0

    for idx, row in df.iterrows():
        with engine.begin() as conn:
            try:
                upsert_movie_data(
                    conn, row, movie_table, genre_table,
                    director_table, actor_table, mgenre, mdirector, mactor
                )
                print(f"✓ Processed movie: {row['movies']}")
                successful_count += 1
            except Exception as e:
                print(f"✗ Error processing movie {row['movies']}: {e}")
                error_count += 1

    print(f"\nData import completed. Successful: {successful_count}, Errors: {error_count}")


if __name__ == "__main__":
    main()
