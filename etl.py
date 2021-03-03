import numpy as np
import pandas as pd

from pathlib2 import Path
from psycopg2 import extras

from sql_queries import *
from db_init import connect


def main():
    with connect("sparkifydb") as cursor:
        song_df = read_jsons("data/song_data")
        log_df = read_jsons("data/log_data")

        # must run in order
        process_df(cursor, song_df, process_song_df)
        process_df(cursor, log_df, process_log_df)


def read_jsons(path):
    jsons = Path(path).rglob("*.json")
    dfs = (pd.read_json(j, orient="records", lines=True) for j in jsons)
    return pd.concat(dfs, ignore_index=True)


def process_df(cursor, df, action):
    action(cursor, df)


def process_song_df(cursor, df):
    # fill artists table
    artist_cols = [ # order matters
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude",
    ]
    artist_df = df[artist_cols]
    bulk_insert_into_table(cursor, artist_table_insert, artist_df)
    print("Table 'artists' successfully filled...")

    # fill songs table
    song_cols = [ # order matters
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration",
    ]
    song_df = df[song_cols]
    bulk_insert_into_table(cursor, song_table_insert, song_df)
    print("Table 'songs' successfully filled...")


def process_log_df(cursor, df):
    df = df[df["page"] == "NextSong"].copy()
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")

    # fill time table
    time_cols = ["ts"]
    time_df = df[time_cols].copy()

    # order matters
    time_df['hour'] = time_df["ts"].dt.hour
    time_df['day'] = time_df["ts"].dt.day
    time_df['week'] = time_df["ts"].dt.isocalendar().week
    time_df['month'] = time_df["ts"].dt.month
    time_df['year'] = time_df["ts"].dt.year
    time_df['dayofweek'] = time_df["ts"].dt.dayofweek

    bulk_insert_into_table(cursor, time_table_insert, time_df)
    print("Table 'time' successfully filled...")

    # fill users table
    user_cols = [ # order matters
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level",
    ]
    user_df = df[user_cols]
    single_insert_into_table(cursor, user_table_insert, user_df)
    print("Table 'users' successfully filled...")

    # fill songplays table
    songplay_cols = [ # order matters
        "ts",
        "userId",
        "sessionId",
        "location",
        "userAgent",
    ]
    to_get_other_foreign_key_cols = [
        "song",
        "length",
        "artist",
    ]
    other_foreign_key_cols = [ # order matters
        'song_id',
        'artist_id',
    ]
    temp_df = df[songplay_cols + to_get_other_foreign_key_cols]
    song_artist_df = select_merged_song_artist_df(cursor)
    temp_df = temp_df.merge(
        song_artist_df,
        how="inner",
        left_on=["song", "length", "artist"],
        right_on=["title", "duration", "name"],
    )
    songplay_df = temp_df[songplay_cols + other_foreign_key_cols]
    bulk_insert_into_table(cursor, songplay_table_insert, songplay_df)
    print("Table 'songplays' successfully filled...")


def bulk_insert_into_table(cursor, query, df):
    values = [tuple(a) for a in df.values]
    extras.execute_values(cursor, query, values)

def single_insert_into_table(cursor, query, df):
    for row in df.itertuples(index=False):
        cursor.execute(query, row)

def select_merged_song_artist_df(cursor):
    cursor.execute(song_artist_table_select)
    results = cursor.fetchall()

    columns = (c[0] for c in cursor.description)

    song_artist_df = pd.DataFrame(np.array(results), columns=columns)
    return song_artist_df


if __name__ == "__main__":
    main()
