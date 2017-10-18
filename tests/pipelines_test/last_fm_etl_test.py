import os
import shutil

import pytest
from pyspark.sql import Row

from pipelines.lastfm_etl import LastFmETL

EXTRACT_FOLDER = 'test_data/uncompressed/'
LASTFM_COLUMNS = ['user_id', 'timestamp', 'artist_id', 'artist_name', 'track_id', 'track_name']
TEST_FILE_PATH = 'test_data/lastfm_test.tar.gz'


class TestLastFmETL:

    @pytest.fixture(autouse=True, scope='class')
    def setup(self, lastfm_object):
        """Remove traces of previous tests in case they failed"""
        self._remove_extract_folder_if_exists()
        yield
        self._remove_extract_folder_if_exists()
        lastfm_object.get_spark_context().stop()

    @staticmethod
    def _remove_extract_folder_if_exists():
        if os.path.isdir(EXTRACT_FOLDER):
            shutil.rmtree(EXTRACT_FOLDER)

    @pytest.fixture(scope='class')
    def lastfm_object(self):
        lastfm_etl = LastFmETL()
        return lastfm_etl

    def test_extract(self, lastfm_object):
        # I am calling protected methods but I rather doing that to have more unit-tests and understand quicker when
        # something fails instead of just one long end-to-end test for the whole ETL that I'll have to debug a lot...
        expected_files_paths = [
            os.path.join(EXTRACT_FOLDER, 'lastfm_test/user-profile.tsv'),
            os.path.join(EXTRACT_FOLDER, 'lastfm_test/userid-timestamp-artid-artname-traid-traname.tsv'),
        ]
        files_paths = lastfm_object._extract(TEST_FILE_PATH, EXTRACT_FOLDER)
        assert expected_files_paths == files_paths

    def test_transform(self, lastfm_object):
        files_paths = [
            'a/fake/file/path',
            os.path.join(EXTRACT_FOLDER, 'lastfm_test/user-profile.tsv'),
            os.path.join(EXTRACT_FOLDER, 'lastfm_test/userid-timestamp-artid-artname-traid-traname.tsv'),
            os.path.join(EXTRACT_FOLDER, 'another/fake/file/path.tsv'),
        ]
        expected_path = os.path.join(EXTRACT_FOLDER, 'lastfm_test/userid-timestamp-artid-artname-traid-traname.tsv')
        file_path = lastfm_object._transform(files_paths)
        assert file_path == expected_path

    def test_load(self, lastfm_object):
        _ = lastfm_object._extract(TEST_FILE_PATH, EXTRACT_FOLDER)
        file_path = os.path.join(EXTRACT_FOLDER, 'lastfm_test/userid-timestamp-artid-artname-traid-traname.tsv')
        expected_data = [  # The content of the file
            Row(user_id='user_000001', timestamp='2009-05-04T23:08:57Z', artist_id='aa1-aa2-aa1',
                artist_name='Singer 1', track_id='bb1-bb2-bb1', track_name='Song 1'),
            Row(user_id='user_000001', timestamp='2009-05-04T23:09:10Z', artist_id='aa1-aa2-aa2',
                artist_name='Singer 2', track_id='', track_name='Song 2'),
            Row(user_id='user_000002', timestamp='2009-05-04T23:08:57Z', artist_id='aa1-aa2-aa3',
                artist_name='Singer 3', track_id='bb1-bb2-bb2', track_name='Song 3'),
            Row(user_id='user_000003', timestamp='2009-05-04T23:08:57Z', artist_id='aa1-aa2-aa4',
                artist_name='Singer 1', track_id='bb1-bb2-bb1', track_name='Song 1'),
            Row(user_id='user_000003', timestamp='2009-05-04T23:08:58Z', artist_id='aa1-aa2-aa5',
                artist_name='Singer 1', track_id='bb1-bb2-bb1', track_name='Song 1'),
            Row(user_id='user_000003', timestamp='2009-05-04T23:33:30Z', artist_id='aa1-aa2-aa6',
                artist_name='Singer 3', track_id='bb1-bb2-bb2', track_name='Song 3'),
            Row(user_id='user_000003', timestamp='2009-05-04T23:33:32Z', artist_id='aa1-aa2-aa7',
                artist_name='Singer 1', track_id='', track_name='Song 4')
        ]
        dataframe = lastfm_object._load(file_path)
        data = dataframe.collect()
        assert len(expected_data) == len(data)
        for expected_row, data_row in zip(expected_data, data):
            for col in LASTFM_COLUMNS:
                assert expected_row[col] == data_row[col]

    def test_process_lastfm_file(self, lastfm_object):
        """
        End to end test of the ETL pipeline
        """
        expected_data = [  # The content of the file
            Row(user_id='user_000001', timestamp='2009-05-04T23:08:57Z', artist_id='aa1-aa2-aa1',
                artist_name='Singer 1', track_id='bb1-bb2-bb1', track_name='Song 1'),
            Row(user_id='user_000001', timestamp='2009-05-04T23:09:10Z', artist_id='aa1-aa2-aa2',
                artist_name='Singer 2', track_id='', track_name='Song 2'),
            Row(user_id='user_000002', timestamp='2009-05-04T23:08:57Z', artist_id='aa1-aa2-aa3',
                artist_name='Singer 3', track_id='bb1-bb2-bb2', track_name='Song 3'),
            Row(user_id='user_000003', timestamp='2009-05-04T23:08:57Z', artist_id='aa1-aa2-aa4',
                artist_name='Singer 1', track_id='bb1-bb2-bb1', track_name='Song 1'),
            Row(user_id='user_000003', timestamp='2009-05-04T23:08:58Z', artist_id='aa1-aa2-aa5',
                artist_name='Singer 1', track_id='bb1-bb2-bb1', track_name='Song 1'),
            Row(user_id='user_000003', timestamp='2009-05-04T23:33:30Z', artist_id='aa1-aa2-aa6',
                artist_name='Singer 3', track_id='bb1-bb2-bb2', track_name='Song 3'),
            Row(user_id='user_000003', timestamp='2009-05-04T23:33:32Z', artist_id='aa1-aa2-aa7',
                artist_name='Singer 1', track_id='', track_name='Song 4')
        ]
        dataframe = lastfm_object.process_lastfm_file(TEST_FILE_PATH, EXTRACT_FOLDER)
        data = dataframe.collect()
        assert len(expected_data) == len(data)
        for expected_row, data_row in zip(expected_data, data):
            for col in LASTFM_COLUMNS:
                assert expected_row[col] == data_row[col]

        # Much slower that what I ended up using...
        # for col in LASTFM_COLUMNS:
        #     assert dataframe.select(col).subtract(expected_df.select(col)).count() == 0
