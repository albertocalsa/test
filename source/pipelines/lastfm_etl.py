import os

from pyspark import SparkContext
from pyspark.sql import SQLContext

from utils.file_utils import extract_targz_file, get_files_paths_recursively

DATA_EXTRACT_FOLDER = '/data/unzipped/'
LASTFM_COLUMNS = ['user_id', 'timestamp', 'artist_id', 'artist_name', 'track_id', 'track_name']


class LastFmETL:
    """
    ETL pipeline process for the LastFm data-sets. Ideally this would extend from an ETL (ABC) base class
    """
    def __init__(self):
        self.sc = SparkContext(master='local', appName='lastfm')
        self.sqlc = SQLContext(self.sc)

    def process_lastfm_file(self, lastfm_file_path, extract_folder=DATA_EXTRACT_FOLDER):
        files_paths = self._extract(lastfm_file_path, extract_folder)
        data_file = self._transform(files_paths)
        rdd = self._load(data_file)
        return rdd

    def get_spark_context(self):
        return self.sc

    def get_spark_sql_context(self):
        return self.sqlc

    @staticmethod
    def _extract(lastfm_file_path, extract_folder):
        """
        Gets the files out of the compressed folder
        :param lastfm_file_path: Path of the tar.gz lastfm file
        :param extract_folder: The folder where the data should be extracted
        :return: The list of files extracted
        """
        extract_targz_file(lastfm_file_path, os.path.join(extract_folder))
        extracted_files_paths = get_files_paths_recursively(extract_folder)
        return sorted(extracted_files_paths)

    @staticmethod
    def _transform(files_paths):
        """
        In this case, transform is very simple: just fetching the data file we are interested on among all the files
        in the lastfm tar.gz
        :param files_paths: The files extracted from the lastfm tar.gz file
        :return: The file path of the data we need
        """
        for fp in files_paths:
            # I was going to use regex but it'd be over-killing it (if it had some timestamp in the name maybe...)
            if fp.endswith('userid-timestamp-artid-artname-traid-traname.tsv'):
                return fp

    def _load(self, file_path):
        """
        We load the file as an RDD, transform it into a DataFrame, and return it
        :param file_path: The path of the file to be loaded
        :return: The DataFrame of the file loaded
        """
        rdd = self.sc.textFile(file_path).map(lambda row: row.split('\t'))
        df = rdd.toDF(LASTFM_COLUMNS)
        return df
