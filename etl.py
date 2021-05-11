import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ A function to process song json file to extract data, process it and \ 
    load into songs dimension and artists dimension table  """
    
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # extract/filter song data from the song json file data frame 
    filtered_song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    
    # Convert filtered song data into a list of values to be inserted into songs table
    song_data = filtered_song_data.values.tolist()[0]
    
    # insert song record
    cur.execute(song_table_insert, song_data)
    

    # extract/filter artist data from the song json file data frame
    filtered_artist_data = df[["artist_id", "artist_name", "artist_location", \
                               "artist_latitude", "artist_longitude"]]
    # Convert filtered artist data into a list of values to be inserted into artists table
    artist_data = filtered_artist_data.values.tolist()[0]
    
    # insert artist record
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ A function to process event log json file to extract data, process it and \
    load into time table, users dimension table and songplays fact table """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action. Each log file may have more than one records.Get all data 
    #df = 
    filtered_ts_values = df[["ts"]].values
    
    ts_data = []
    # Iterate through each record for ts and get corresponding timestamp break up value 
    # like week, month etc. 
    for x in filtered_ts_values:
        # interim data list
        interim_data = []
        # convert timestamp column to datetime
        t = pd.Timestamp(x[0]/1000.0, unit='s', tz='US/Pacific')
        
        interim_data.append(t)
        interim_data.append(t.hour)
        interim_data.append(t.day)
        interim_data.append(t.weekofyear)
        interim_data.append(t.month)
        interim_data.append(t.year)
        interim_data.append(t.weekday())
        
        # append timestamp break up data row into time data set
        ts_data.append(tuple(interim_data))
    
    # insert time data records
    time_data = ts_data
    
    # Create the timestamp data dictionary column labels 
    column_labels = ["start_time","hour", "day", "week", "month", "year", "weekday"]
    
    # Generate a time series data frame from the timestamp data dictionary
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    # Iterate through each row of the data and insert into the time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    
    #Extract user data set from the data frame
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        # Ignore row if userId is not a valid integer
        if row.userId is None or row.userId == '':
            continue;
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        
        # Convert start_time in timestamp before insertion
        l_start_time = pd.Timestamp(row.ts/1000.0, unit='s', tz='US/Pacific')
        
        # Ignore row if userId is not a valid integer
        if row.userId is None or row.userId == '':
            continue;
        songplay_data = (l_start_time, row.userId, songid, artistid, row.sessionId, \
                         row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ A function to collect all valid JSON files from the specified file path.
        Iterare through each file and send it to respective specified function
        to process data source file.
        Display informative message to user about the number of files processed
    """
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
    """ Main function that creates a connection object on successful connection to PostgreSQL
        From connection object it creates a cursor object execute SQL queries.
        Subsequently it makes call to process_data function to handle processing of
        Song data files and event log data files specifying the filepath for each.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()