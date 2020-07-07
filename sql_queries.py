import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DISTRIBUTION SCHEMA
schema = ("""CREATE SCHEMA IF NOT EXISTS public;
             SET search_path TO public;""")

# DROP TABLES

staging_events_table_drop = ("DROP TABLE IF EXISTS staging_events;") 
staging_songs_table_drop = ("DROP TABLE IF EXISTS staging_songs;") 
songplay_table_drop = ("DROP TABLE IF EXISTS songplay;") 
users_table_drop = ("DROP TABLE IF EXISTS users;")
song_table_drop = ("DROP TABLE IF EXISTS song;")
artist_table_drop = ("DROP TABLE IF EXISTS artist;") 
time_table_drop = ("DROP TABLE IF EXISTS time;") 

# CREATE STAGING TABLES

staging_events_table_create = ("""CREATE TABLE staging_events(
                               artist            VARCHAR,
                               auth              VARCHAR,
                               first_name        VARCHAR,
                               gender            CHAR(16),
                               item_in_session   INTEGER,
                               last_name         VARCHAR,
                               length            FLOAT,
                               level             VARCHAR(10),
                               location          VARCHAR,
                               method            VARCHAR(4),
                               page              VARCHAR(16),
                               registration      VARCHAR,
                               session_id        INTEGER,
                               song              VARCHAR,
                               status            INTEGER,
                               ts                BIGINT,
                               user_agent        VARCHAR,
                               user_id           INTEGER);""") 

staging_songs_table_create =  ("""CREATE TABLE staging_songs(
                               song_id          VARCHAR,
                               num_songs        INTEGER,
                               artist_id        VARCHAR,
                               artist_latitude  VARCHAR,
                               artist_longitude VARCHAR,
                               artist_location  VARCHAR,
                               artist_name      VARCHAR,
                               title            VARCHAR,
                               duration         FLOAT,
                               year             INTEGER);""")

# CREATE FACT TABLE
songplay_table_create =    ("""CREATE TABLE songplay (
                               songplay_id      INTEGER IDENTITY(0,1) sortkey,
                               start_time       TIMESTAMP, 
                               user_id          INTEGER,
                               level            VARCHAR(10),
                               song_id          VARCHAR distkey,
                               artist_id        VARCHAR,
                               session_id        INTEGER,
                               location         VARCHAR,
                               user_agent        VARCHAR);""") 

# CREATE DIMENSION TABLES
users_table_create =       ("""CREATE TABLE users (
                               user_id           INTEGER sortkey distkey,
                               first_name        VARCHAR,
                               last_name         VARCHAR,
                               gender           CHAR(16),
                               level            VARCHAR(10));""")

song_table_create =        ("""CREATE TABLE song (
                               song_id          VARCHAR sortkey distkey,
                               title            VARCHAR,
                               artist_id        VARCHAR,
                               year             INTEGER,
                               duration         FLOAT);""")


artist_table_create =      ("""CREATE TABLE artist (
                               artist_id        VARCHAR sortkey distkey,
                               artist_name      VARCHAR,
                               artist_location  VARCHAR,
                               artist_latitude  VARCHAR,
                               artist_longitude VARCHAR);""")              

time_table_create =        ("""CREATE TABLE time (
                               start_time       TIMESTAMP sortkey distkey,
                               hour             INTEGER,
                               day              INTEGER,
                               week             INTEGER,
                               month            INTEGER,
                               year             INTEGER,
                               weekday          INTEGER);""") 

# COPY FROM S3 INTO STAGING TABLES

staging_events_copy = ("""COPY staging_events
                          FROM 's3://udacity-dend/log_data'
                          CREDENTIALS 'aws_iam_role={}' 
                          COMPUPDATE OFF REGION 'us-west-2'
                          FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
                        """).format("IAM ARN")

#limiting data due to execution time - remove prefix /A/A/ to copy entire file
staging_songs_copy =  ("""COPY staging_songs 
                          FROM 's3://udacity-dend/song_data'
                          CREDENTIALS 'aws_iam_role={}'
                          COMPUPDATE OFF REGION 'us-west-2'
                          JSON 'auto' truncatecolumns;
                      """).format("IAM ARN")

# INSERT FROM STAGING TO FINAL TABLES

songplay_table_insert =("""INSERT INTO songplay(start_time, user_id, level, 
                           song_id, artist_id, session_id,location, user_agent)
                           SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' as start_time,
                           se.user_id, 
                           se.level, 
                           ss.song_id,
                           ss.artist_id,
                           se.session_id,
                           se.location,
                           se.user_agent
                           FROM staging_events se, staging_songs ss
                           WHERE se.page = 'NextSong'
                           AND se.artist = ss.artist_name
                           AND se.length = ss.duration""")

users_table_insert =   ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                           SELECT 
                           se.user_id, 
                           se.first_name, 
                           se.last_name,
                           se.gender,
                           se.level
                           FROM staging_events se""")

song_table_insert =    ("""INSERT INTO song (song_id, title, artist_id, year, duration)
                           SELECT 
                           ss.song_id, 
                           ss.title, 
                           ss.artist_id,
                           ss.year,
                           ss.duration
                           FROM staging_songs ss""")

artist_table_insert =  ("""INSERT INTO artist (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                           SELECT 
                           ss.artist_id,
                           ss.artist_name, 
                           ss.artist_location, 
                           ss.artist_latitude,
                           ss.artist_longitude
                           FROM staging_songs ss""")

time_table_insert =    ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                           SELECT start_time, 
                           EXTRACT(hour from start_time),
                           EXTRACT(day from start_time),
                           EXTRACT(week from start_time),
                           EXTRACT(month from start_time),
                           EXTRACT(year from start_time),
                           EXTRACT(dayofweek from start_time)
                           FROM songplay""")


#TEST QUERIES

test1 = ("""SELECT * FROM songplay LIMIT 1; """)
test2 = ("""SELECT * FROM users LIMIT 1; """)
test3 = ("""SELECT * FROM song LIMIT 1; """)
test4 = ("""SELECT * FROM artist LIMIT 1; """)
test5 = ("""SELECT * FROM time LIMIT 1; """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, users_table_create, 
                        song_table_create, artist_table_create, time_table_create]
    
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, 
                      song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]


insert_table_queries = [songplay_table_insert, users_table_insert, song_table_insert, artist_table_insert, time_table_insert]


test_queries = [test1, test2, test3, test4, test5]