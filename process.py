
from abc import abstractmethod
from controllers.database_controller import DatabaseMongo
from utils.properties_parser import parse_properties
from hdfs import InsecureClient
import pymonetdb
import os


class Process:

    def __init__(self):
        self._hdfs_client = InsecureClient(f"http://{parse_properties('hdfs')['hdfs.host']}", user=parse_properties('hdfs')['hdfs.user'])  # Connect to HDFS
        self._database = DatabaseMongo('p2')
        #monetdb
        self._database_monet = pymonetdb.connect(username=parse_properties('monetdb')['database.user'], 
                                        password=parse_properties('monetdb')['database.password'], 
                                        hostname=parse_properties('monetdb')['database.host'], 
                                        database="mydb")

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

