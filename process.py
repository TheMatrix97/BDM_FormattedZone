
from abc import abstractmethod
from controllers.database_controller import Database
from utils.properties_parser import parse_properties
from hdfs import InsecureClient
import os


class Process:

    def __init__(self):
        self._host = parse_properties('hdfs')['hdfs.host']
        self._user = parse_properties('hdfs')['hdfs.user']
        self._hdfs_client = InsecureClient(f'http://{self._host}', user=self._user)  # Connect to HDFS
        self._database = Database('p2')

    @abstractmethod
    def run_process(self):
        pass

    @staticmethod
    def _linux_normalize_path(path: str):  # required for windows compatibility
        return path.replace('\\', '/')

    @staticmethod
    def remove_file(file_path):
        if os.path.exists(file_path):
            os.remove(file_path)

