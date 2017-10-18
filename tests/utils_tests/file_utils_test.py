import os
import shutil

import pytest

from source.utils.file_utils import extract_targz_file, get_files_paths_recursively

EXTRACT_FOLDER = 'test_data/uncompressed/'
TEST_FILE_PATH = 'test_data/targz_test.tar.gz'


class TestFileUtils:

    @pytest.fixture(autouse=True, scope='function')
    def setup(self):
        """Remove traces of previous tests in case they failed"""
        dirs = [EXTRACT_FOLDER, os.path.join(os.getcwd(), 'test_files')]
        for d in dirs:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_extract_targz_file_current_working_dir(self):
        extract_targz_file(tar_gz_file_path=TEST_FILE_PATH)
        current_working_directory = os.getcwd()
        # Checking that it contains the directory of the tar unzipped
        unzipped_tar_folder = os.path.join(current_working_directory, 'test_files')
        assert os.path.isdir(unzipped_tar_folder)
        # Checking that files inside the tar were unzipped in the expected folder
        assert os.path.isfile(os.path.join(unzipped_tar_folder, 'test1.txt'))
        assert os.path.isfile(os.path.join(unzipped_tar_folder, 'test2.txt'))
        shutil.rmtree(unzipped_tar_folder)

    def test_extract_targz_file_new_folder(self):
        extract_targz_file(tar_gz_file_path=TEST_FILE_PATH,
                           extract_folder=EXTRACT_FOLDER)
        # Checking that extract folder was created
        assert os.path.isdir(EXTRACT_FOLDER)
        # Checking that it contains the directory of the tar unzipped
        unzipped_tar_folder = os.path.join(EXTRACT_FOLDER, 'test_files')
        assert os.path.isdir(unzipped_tar_folder)
        # Checking that files inside the tar were unzipped in the expected folder
        assert os.path.isfile(os.path.join(unzipped_tar_folder, 'test1.txt'))
        assert os.path.isfile(os.path.join(unzipped_tar_folder, 'test2.txt'))
        shutil.rmtree(EXTRACT_FOLDER)

    def test_extract_targz_not_targz_file(self):
        # Checking that the exception is raised
        with pytest.raises(ValueError):
            extract_targz_file(tar_gz_file_path='this_file_is_not_tar_gz.csv')

    def test_get_files_paths_recursively(self):
        # Re-using a test file to get a directory with a sub-directory and some files in it
        extract_targz_file(tar_gz_file_path=TEST_FILE_PATH,
                           extract_folder=EXTRACT_FOLDER)
        expected_files = [os.path.join(EXTRACT_FOLDER, 'test_files/test1.txt'),
                          os.path.join(EXTRACT_FOLDER, 'test_files/test2.txt')
                          ]
        files = get_files_paths_recursively(EXTRACT_FOLDER)
        assert files == expected_files
