import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INT,
        last_name       TEXT,
        length          FLOAT4,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        session_id      INT,
        song            TEXT,
        status          INT,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         TEXT

)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
        song_id             TEXT,
        title               TEXT,
        duration            FLOAT4,
        year                SMALLINT,
        artist_id           TEXT,
        artist_name         TEXT,
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     TEXT,
        num_songs           INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
        songplay_id           INT IDENTITY(1, 1) PRIMARY KEY,
        start_time            TIMESTAMP  SORTKEY,
        user_id               TEXT,
        level                 TEXT,
        song_id               TEXT,
        artist_id             TEXT,
        session_id            INT,
        location              TEXT,
        user_agent            TEXT

) diststyle all; 

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
        (user_id      TEXT PRIMARY KEY,
        first_name    TEXT,
        last_name     TEXT,
        gender        TEXT,
        level         TEXT) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
        song_id        TEXT  PRIMARY KEY,
        title          TEXT,
        artist_id      TEXT,
        year           INT,
        duration       FLOAT
) diststyle all;
""") 

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
        artist_id       TEXT  PRIMARY KEY,
        name            TEXT,
        location        TEXT,
        lattitude       FLOAT,
        longitude       FLOAT
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time       TIMESTAMP PRIMARY KEY,
hour             INT,
day              INT,
week             INT,
month            INT,
year             INT,
weekday          INT
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""").format(
    'staging_events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['REGION']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
""").format(
    'staging_songs',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION']
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT
TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'), 
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON
    e.song = s.title AND
    e.artist = s.artist_name AND
    ABS(e.length - s.duration) < 2
WHERE
    e.page = 'NextSong'

""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT (user_id) user_id, first_name, last_name, gender, level FROM staging_events
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT (song_id) song_id, title, artist_id, year, duration FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
FROM
staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as start_time FROM staging_events)
SELECT DISTINCT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
extract(month from start_time), extract(year from start_time), extract(weekday from start_time)
FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
