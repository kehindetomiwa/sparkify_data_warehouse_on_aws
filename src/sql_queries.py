import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_stage;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_stage;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS event_stage (
    artist TEXT,
    auth TEXT,
    user_first_name TEXT,
    user_gender TEXT,
    item_in_session INTEGER,
    user_last_name TEXT,
    song_length FLOAT,
    user_level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration TEXT,
    session_id TEXT,
    ts BIGINT,
    user_agent TEXT,
    user_id FLOAT
     
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS song_stage (
    song_id TEXT PRIMARY KEY,
    num_songs FLOAT,
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(1024),
    artist_name VARCHAR(1024),
    title VARCHAR(1024),
    duration FLOAT,
    year FLOAT,
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id IDENTITY (0,1) PRIMARY KEY,
    start_time BIGINT NOT NULL REFERENCES time(start_time) sortkey,
    user_id INTEGER NOT NULL REFERENCES  users(user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL REFERENCES songs(song_id) distkey
    artist_id VARCHAR NOT NULL artists(artist_id),
    session_id INTEGER NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level VARCHAR NOT NULL
)DISTSTYLE all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR(1024) NOT NULL,
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id) sortkey distkey,
    year INTEGER,
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR(1024) NOT NULL,
    location VARCHAR(1024),
    lattitude FLOAT,
    longitude FLOAT
)DISTSTYLE all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)DISTSTYLE all;
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
