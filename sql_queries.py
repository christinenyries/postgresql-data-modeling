# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

user_table_create = """
    CREATE TABLE users (
        user_id VARCHAR,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR,
        PRIMARY KEY(user_id)
    )
"""

song_table_create = """
    CREATE TABLE songs(
        song_id VARCHAR,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration NUMERIC,
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id) 
            REFERENCES artists (artist_id)
            ON UPDATE CASCADE ON DELETE CASCADE
    )
"""

artist_table_create = """
    CREATE TABLE artists(
        artist_id VARCHAR,
        name VARCHAR,
        location VARCHAR,
        latitude REAL,
        longitude REAL,
        PRIMARY KEY (artist_id)
    )
"""

time_table_create = """
    CREATE TABLE time(
        start_time timestamp,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT,
        PRIMARY KEY(start_time)
    )
"""

songplay_table_create = """
    CREATE TABLE songplays (
        songplay_id SERIAL,
        start_time timestamp NOT NULL,
        user_id VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY (start_time)
            REFERENCES time (start_time)
            ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY (user_id)
            REFERENCES users (user_id)
            ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY (song_id)
            REFERENCES songs (song_id)
            ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY (artist_id)
            REFERENCES artists (artist_id)
            ON UPDATE CASCADE ON DELETE CASCADE
    )
"""


# INSERT RECORDS

songplay_table_insert = """
    INSERT INTO songplays(start_time, user_id, session_id, location, user_agent, song_id, artist_id) 
    VALUES %s;
"""

user_table_insert = """
    INSERT INTO users(user_id, first_name, last_name, gender, level) 
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;
"""

song_table_insert = """
    INSERT INTO songs(song_id, title, artist_id, year, duration) 
    VALUES %s
    ON CONFLICT (song_id) DO NOTHING;
"""

artist_table_insert = """
    INSERT INTO artists(artist_id, name, location, latitude, longitude) 
    VALUES %s
    ON CONFLICT (artist_id) DO NOTHING;
"""

time_table_insert = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    VALUES %s
    ON CONFLICT (start_time) DO NOTHING;
"""


# FIND SONGS
song_artist_table_select = """
    SELECT s.song_id, s.artist_id, s.title, s.duration, a.name
    FROM songs s
        INNER JOIN artists a
        ON s.artist_id = a.artist_id;
"""


# QUERY LISTS
create_table_queries = [
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create,
]

drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
