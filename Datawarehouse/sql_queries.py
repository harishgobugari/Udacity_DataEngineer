import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table stagingevents"
staging_songs_table_drop = "drop table stagingsongs"
songplay_table_drop = "drop table songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time_table"

# CREATE TABLES

staging_events_table_create= ("""create table if not exists stagingevents(
                                artist TEXT,
                                auth TEXT,
                                first_name TEXT,
                                gender CHAR(1),
                                item_session INTEGER,
                                last_name TEXT,
                                length NUMERIC,
                                level TEXT,
                                location TEXT,
                                method TEXT,
                                page TEXT,
                                registration NUMERIC,
                                session_id INTEGER,
                                song TEXT,
                                status INTEGER,
                                ts BIGINT,
                                user_agent TEXT,
                                user_id INTEGER
                            ) 
""")

staging_songs_table_create = ("""create table if not exists stagingsongs(
                                num_songs INTEGER,
                                artist_id TEXT,
                                artist_latitude NUMERIC,
                                artist_longitude NUMERIC,
                                artist_location TEXT,
                                artist_name TEXT,
                                song_id TEXT,
                                title TEXT,
                                duration NUMERIC,
                                year INTEGER
                            )
""")

songplay_table_create = ("""create table if not exists songplays (songplay_id int identity(1,1) primary key,
                            start_time timestamp,
                            user_id int NOT NULL,
                            level varchar,
                            artist_id varchar,
                            song_id varchar,
                            session_id int,
                            location text,
                            user_agent text)
""")

user_table_create = ("""create table if not exists users(user_id int NOT NULL,
                        first_name varchar NOT NULL,
                        last_name varchar NOT NULL,
                        gender char,
                        level varchar,
                        PRIMARY KEY (user_id))
""")

song_table_create = ("""create table if not exists songs(song_id varchar NOT NULL,
                        title varchar,
                        artist_id varchar,
                        year int,
                        duration float,
                        PRIMARY KEY (song_id)))
""")

artist_table_create = ("""create table if not exists artists(artist_id varchar NOT NULL,
                        name varchar,
                        location varchar,
                        lattitude float,
                        longitude float,
                        PRIMARY KEY (artist_id))
""")

time_table_create = ("""create table if not exists time_table(
                        start_time timestamp NOT NULL,
                        hour int,
                        day int,
                        week int,
                        month int,
                        year int,
                        weekday varchar,
                        PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""copy stagingevents 
                          from {}
                          iam_role {}
                          json {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""copy stagingsongs 
                          from {} 
                          iam_role {}
                          json 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT  timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.user_id, se.level, 
                                    ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
                            FROM stagingevents se, stagingsongs ss
                            WHERE se.page = 'NextSong' AND
                            se.song =ss.title AND
                            se.artist = ss.artist_name AND
                            se.length = ss.duration
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT distinct  user_id, first_name, last_name, gender, level
                        FROM stagingevents
                        WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id, year, duration
                        FROM stagingsongs
                        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT distinct artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
                          FROM stagingsongs
                          WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time_table(start_time, hour, day, week, month, year, weekDay)
                        SELECT start_time, extract(hour from start_time), extract(day from start_time),
                                extract(week from start_time), extract(month from start_time),
                                extract(year from start_time), extract(dayofweek from start_time)
                        FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
