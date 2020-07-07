from config import config

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
COPY event_stage
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""").format(config['S3']['LOG_DATA'],
            config["IAM_ROLE"]["ARN"],
            config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
COPY song_stage
FROM {}
CREDENTIALS 'aws_iam_role{}'
REGION 'us-west-2'
JSON 'auto'
""").format(config['S3']['LOG_DATA'],
            config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent    
)
select timestamp 'epoch' + ts * interval '0.001 seconds' as start_time,
    user_id,
    level,
    song.song_id as song_id,
    sessionId as session_id,
    staging_events.location as location,
    userAgent as user_agent
from event_stage
inner join artists on artist.name = event_stage.artist
inner join songs on songs.title = event_stage.song
where page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT user_id, firstName, lastName, gender, level
FROM event_stage
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECTION DISTINCT song_id, title, artist_id, year, duration
FROM event_stage
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM event_stage
""")

time_table_insert = ("""
insert into time (
    start_time,
    hour,
    day,
    week,
    year,
    weekday
)
SELECT DISTINCT start_time,
                extract(hour from timestamp 'epoch' + start_time * interval '0.001 seconds') as hour,
                extract(day from timestamp 'epoch' + start_time * interval '0.001 seconds') as day,
                extract(week from timestamp 'epoch' + start_time * interval '0.001 seconds') as week,
                extract(year from timestamp 'epoch' + start_time * interval '0.001 seconds') as year,
                extract(weekday from timestamp 'epoch' + start_time * interval '0.001 seconds') as weekday
from songplays 
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
