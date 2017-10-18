from datetime import datetime

import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window


class LastFmAggregates:

    def __init__(self, lastfm_df):
        self.df = lastfm_df

    def count_distinct_songs_per_user(self):
        return self.df.groupby('user_id').agg(func.countDistinct('artist_name', 'track_name').alias('distinct_tracks'))

    def get_top_listened_songs(self, n=100):
        return self.df.groupby('artist_name', 'track_name').count().orderBy('count', ascending=False).limit(n)

    def get_top_user_sessions(self, n=10, session_length_mins=20):
        diff_in_secs_func = func.udf(_get_diff_in_secs, IntegerType())
        sessions = self._calculate_sessions(session_length_mins, diff_in_secs_func)
        grouped_sessions = sessions.select("*").groupBy('user_id', 'session')
        grouped_sessions = grouped_sessions.agg(
            func.min('timestamp').alias('begin'),
            func.max('timestamp').alias('end'),
            func.collect_list('track_name').alias('songs')
        ).withColumn('session_length', diff_in_secs_func('begin', 'end'))
        return grouped_sessions.select(['user_id', 'session_length', 'songs'])\
            .orderBy('session_length', ascending=False).limit(n)

    def _calculate_sessions(self, session_length_mins, diff_in_secs_func):
        w = Window.partitionBy('user_id').orderBy('timestamp')
        lag_df = self.df.withColumn('prev_time', func.lag('timestamp').over(w))
        lag_df = lag_df.withColumn('lag', diff_in_secs_func(lag_df.prev_time, lag_df.timestamp))
        session_length_secs = session_length_mins * 60
        lag_df = lag_df.withColumn('new_session', (lag_df.lag > session_length_secs).cast('integer'))
        lag_df = lag_df.withColumn('session', func.sum('new_session').over(w).alias('session'))
        return lag_df


def _get_diff_in_secs(str_date1, str_date2):
    if str_date1 is None:
        return 0
    date1 = datetime.strptime(str_date1, '%Y-%m-%dT%H:%M:%SZ')
    date2 = datetime.strptime(str_date2, '%Y-%m-%dT%H:%M:%SZ')
    delta = (date2-date1).total_seconds()
    return int(delta)
