# Schema for fictitious music store that captures structured data from song data and event log data.
# SQL queries to drop tables for fresh database tables.
# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# SQL queries to create database tables for fact and dimension tables
# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id bigserial PRIMARY KEY, start_time bigint, 
                            user_id varchar, song_id varchar, artist_id varchar, session_id varchar, location varchar,
                            user_agent varchar) """)

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (user_id varchar PRIMARY KEY, first_name varchar, last_name varchar,
                        gender varchar, level varchar) """)

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar,
                        year varchar, duration numeric) """)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar, location varchar,
                            latitude numeric, longitude numeric) """)

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (start_time bigint, hour int, day int, week varchar, month int, 
                            year int, weekday varchar) """)

# SQL queries to insert records into database tables
# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, \
                            location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s) """)

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level ) VALUES (%s, %s, %s, %s, %s) \
                         ON CONFLICT (user_id) DO NOTHING """)

song_table_insert = (""" INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT ON CONSTRAINT songs_pkey DO NOTHING""")

artist_table_insert = (""" INSERT INTO artists(artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (artist_id) DO NOTHING """)


time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, \
                        %s, %s) """)

# SQL qyery to retrive song_id and artist_id from song title, artists name and song length.
# FIND SONGS

song_select = (""" SELECT song_id, a.artist_id FROM songs s INNER JOIN artists a ON s.artist_id = a.artist_id \
                    WHERE title = %s AND name = %s AND duration = %s """)


# SQL queries list to create tables
# SQL queries list to drop tables
# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]