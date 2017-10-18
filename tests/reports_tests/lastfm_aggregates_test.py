import os
import shutil

import pytest
from pyspark.sql import Row

from source.pipelines.lastfm_etl import LastFmETL
from source.reports.lastfm_aggregates import LastFmAggregates, _get_diff_in_secs

EXTRACT_FOLDER = 'test_data/uncompressed/'
TEST_FILE_PATH = 'test_data/lastfm_test.tar.gz'


class TestLastFmAggregates:

    @pytest.fixture(autouse=True, scope='class')
    def setup(self, lastfm_etl):
        """Remove traces of previous tests in case they failed"""
        self._remove_extract_folder_if_exists()
        yield
        lastfm_etl.get_spark_context().stop()
        self._remove_extract_folder_if_exists()

    @staticmethod
    def _remove_extract_folder_if_exists():
        if os.path.isdir(EXTRACT_FOLDER):
            shutil.rmtree(EXTRACT_FOLDER)

    @pytest.fixture(scope='class')
    def lastfm_etl(self):
        lastfm_etl = LastFmETL()
        return lastfm_etl

    @pytest.fixture(scope='class')
    def lastfm_dataframe(self, lastfm_etl):
        lastfm_df = lastfm_etl.process_lastfm_file(TEST_FILE_PATH, EXTRACT_FOLDER)
        return lastfm_df

    def test_count_songs_per_user(self, lastfm_dataframe):
        lastfm_agg = LastFmAggregates(lastfm_dataframe)
        expected_data = [  # The content of the file
            Row(user_id='user_000003', distinct_tracks=3),
            Row(user_id='user_000001', distinct_tracks=2),
            Row(user_id='user_000002', distinct_tracks=1),
        ]
        distinct_songs_per_user = lastfm_agg.count_distinct_songs_per_user()
        data = distinct_songs_per_user.collect()
        assert len(expected_data) == len(data)
        for expected_row, data_row in zip(expected_data, data):
            assert expected_row['user_id'] == data_row['user_id']
            assert expected_row['distinct_tracks'] == data_row['distinct_tracks']

    def test_get_top_listened_songs(self, lastfm_dataframe):
        lastfm_agg = LastFmAggregates(lastfm_dataframe)
        expected_data = [  # The content of the file
            Row(artist_name='Singer 1', track_name='Song 1', count=3),
            Row(artist_name='Singer 3', track_name='Song 3', count=2),
        ]
        top_2_songs = lastfm_agg.get_top_listened_songs(n=2)
        data = top_2_songs.collect()
        assert len(expected_data) == len(data)
        for expected_row, data_row in zip(expected_data, data):
            assert expected_row['artist_name'] == data_row['artist_name']
            assert expected_row['track_name'] == data_row['track_name']
            assert expected_row['count'] == data_row['count']

    def test_get_top_user_sessions(self, lastfm_dataframe):
        expected_data = [
            Row(user_id='user_000001', session_length=13, songs=['Song 1', 'Song 2']),
            Row(user_id='user_000003', session_length=2, songs=['Song 3', 'Song 4']),
        ]
        lastfm_agg = LastFmAggregates(lastfm_dataframe)
        top_2_sessions = lastfm_agg.get_top_user_sessions(n=2, session_length_mins=20)
        data = top_2_sessions.collect()
        assert len(expected_data) == len(data)
        for expected_row, data_row in zip(expected_data, data):
            assert expected_row['user_id'] == data_row['user_id']
            assert expected_row['session_length'] == data_row['session_length']
            assert expected_row['songs'] == data_row['songs']

    def test_get_diff_in_secs(self):
        t1 = '2009-05-04T23:09:10Z'
        t2 = '2009-05-04T23:09:15Z'
        assert _get_diff_in_secs(t1, t2) == 5
