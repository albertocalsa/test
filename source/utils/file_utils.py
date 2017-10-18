import os
import shutil
import tarfile


def get_files_paths_recursively(folder):
    """
    Returns the list of files' paths in a directory, iterating recursively
    :param folder:
    :return:
    """
    all_files = []
    for root, dir, files in os.walk(folder):
        all_files += [os.path.join(root, f) for f in files if not f.startswith('._')]
    return all_files


def extract_targz_file(tar_gz_file_path, extract_folder=None):
    """
    Extracts a tar.gz file to the folder specified. If no folder is passed, the file is extracted to the current
    working directory
    :param tar_gz_file_path: The path to the tar.gz file whose files are to be extracted
    :param extract_folder: The path of the directory to extract the files. If it exists, it gets overwritten
    :return: Nothing. Files are extracted to the directory given
    """
    if not tar_gz_file_path.endswith('tar.gz'):
        raise ValueError('File passed as argument is not `tar.gz`')
    tar = tarfile.open(tar_gz_file_path)
    current_working_directory = os.getcwd()
    _setup_extract_folder(extract_folder)
    tar.extractall()
    tar.close()
    os.chdir(current_working_directory)


def _setup_extract_folder(extract_folder):
    """
    For clarity and to isolate functionality from the main method, here it is ensured that the output dir for the
    tar file is ready
    """
    if extract_folder is not None:
        if os.path.exists(extract_folder):
            shutil.rmtree(extract_folder)
        os.mkdir(extract_folder)
        os.chdir(extract_folder)
