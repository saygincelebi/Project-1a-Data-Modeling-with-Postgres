

Project Name: Data Modeling with Postgres

Introduction: A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results. 


Tables:

Fact Table
songplays - records in log data associated with song plays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
users - users in the app : user_id, first_name, last_name, gender, level
songs - songs in music database: song_id, title, artist_id, year, duration
artists - artists in music database: artist_id, name, location, latitude, longitude
time - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday

Pipeline:

1- On terminal run > python3 create_tables.py 

This script will create a fresh Sparkifydb database and create dim & fact tables under the public(default) schema using the student user. sql_queries.py will be internally used by this script in order to run the drop, create table & insert scripts as well as "song_select" SQL.

2-On terminal run > python3 etl.py

This script will first read the song data files, extract song & artist data from these files and load it into the related dimension tables. Then, the log files will be read and users & time dimensions will be inserted from these log data files. Lastly the songplays table will be filled by using the "song_select" SQL that takes the song_id, artist_id & song duration as input in order to find the matching songs that users listed to. 

Stats:

Total song files read: 76
Total log files read: 30

Total number of distinct songs: 71
Total number of artists: 69
Total number of users: 96
Total number of songplays: 6820

Total number of artists & songplays matching: 1








