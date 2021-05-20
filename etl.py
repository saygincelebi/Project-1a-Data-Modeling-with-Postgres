import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    '''
    This function reads a song file, extracts the related information for songs and artists dimension tables and writes them to these tables.

            Parameters:
                    cur: Connection cursor for sparkifydb
                    filepath (string): full path of a single file

            Returns:
                    None
    '''
    df = pd.read_json(filepath, lines = True)

    # insert song record
    songs_dict = {} #created a dictionary to store songs in each file
    
    for index,row in df.iterrows(): #for each row in the dataframe
        for column in df.columns:   #for each column in the row
            songs_dict[column] = df[column].values[0]
            
    song_data = [songs_dict['song_id'], songs_dict['title'], songs_dict['artist_id'], int(songs_dict['year']), float(songs_dict['duration'])] #extract related fields
    
    cur.execute(song_table_insert, song_data) #insert into songs table
    
    # insert artist record
    artist_data = [songs_dict['artist_id'], songs_dict['artist_name'], songs_dict['artist_location'], float(songs_dict['artist_longitude']), float(songs_dict['artist_latitude'])] #extract related fields

    cur.execute(artist_table_insert, artist_data) #insert into artists table

    
def process_log_file(cur, filepath):
    '''
    This function reads a log file, extracts the related information for users and time dimension tables, writes them to these tables. Also matches the songs played with song and artist information and fills the songsplayed fact table.

            Parameters:
                    cur: Connection cursor for sparkifydb
                    filepath (string): full path of a single file

            Returns:
                    None
    '''
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    # insert time data records

    time_data = [] #create an empty list
    
    for each_time in t: #for each value in the t series, time_data list is filled
        time_data.append([each_time, 
                          each_time.hour, 
                          each_time.day, 
                          each_time.week, 
                          each_time.month, 
                          each_time.year, 
                          each_time.day_name()])

    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    #time_df is filled by matching labels
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)
    
    #for each row in time_df
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row)) #insert into time table

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row) #insert into users table
         
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results: #when matched assign values, else assign null
            songid, artistid = results
            print("Match: "+artistid+" "+songid)
        else:
            songid, artistid = None, None

        # insert songplay record
                 
        songplay_data = ( pd.to_datetime(row.ts, unit='ms'), 
                     int(row.userId), 
                     row.level, 
                     songid, 
                     artistid, 
                     row.sessionId, 
                     row.location, 
                     row.userAgent)

        cur.execute(songplay_table_insert, songplay_data) #insert into songplay table 

def process_data(cur, conn, filepath, func):
    '''
    This function calls the processing function either for each nested song or log file
    
            Parameters:
                    cur: Connection cursor for sparkifydb
                    conn: Connection for sparkifydb
                    filepath (string): generic path of song or log files 
                    func (function): function for processing song or log files

            Returns:
                    None
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    '''
    The main function connects to the database and calls the process_data functions of song and log files.
    
            Parameters:
                    None
            Returns:
                    None
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()