# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP  REFERENCES time(start_time),
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    level VARCHAR(10),
    song_id VARCHAR(20)  REFERENCES songs(song_id),
    artist_id VARCHAR(20)  REFERENCES artists(artist_id),
    session_id INTEGER,
    location VARCHAR(50),
    user_agent VARCHAR(150)
);
""")

user_table_create = ("""
create table if not exists users (
    user_id integer primary key,
    first_name varchar(40),
    last_name varchar(40),
    gender char(1),
    level varchar(20)
);
""")

artist_table_create = ("""
create table if not exists artists (
    artist_id varchar(20) primary key,
    name varchar(100),
    location varchar(200),
    lattitude float(10),
    longitude float(10)
);
""")

song_table_create = ("""
create table if not exists songs (
    song_id varchar(20) primary key,
    title varchar(100),
    artist_id varchar(20),
    year integer,
    duration numeric
);
""")


time_table_create = ("""
create table if not exists time (
    start_time timestamp primary key,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday varchar(15)
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values (%s, %s, %s, %s, %s, %s, %s, %s)
on conflict(songplay_id) do nothing;
""")

#Existing users' level can be changed so an update will be needed.
user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
values (%s, %s, %s, %s, %s) on conflict (user_id) do update set level = excluded.level;
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
values (%s, %s, %s, %s, %s) on conflict do nothing;
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, lattitude, longitude)
values (%s, %s, %s, %s, %s) on conflict do nothing;
""")


time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
values (%s, %s, %s, %s, %s, %s, %s) on conflict do nothing;
""")

# FIND SONGS

song_select = ("""
select ss.song_id, ss.artist_id from songs ss 
join artists ars on ss.artist_id = ars.artist_id
where ss.title = %s
and ars.name = %s
and ss.duration = %s;
""")


# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]