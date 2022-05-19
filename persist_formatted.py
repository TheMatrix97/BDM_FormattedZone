from cgitb import scanvars
import glob
import json
import os
import time

from pyspark.sql import SparkSession

from models.datasource import Datasource
from models.format_log_entry import FormatLogEntry

from process import Process
from utils.properties_parser import parse_properties


class FormatLoadProcess(Process):
    _log_collection_name = 'formatLog'

    def __init__(self):
        super().__init__()
    
    def run_process(self):
        # Enable database
        res = self._database.find('datasources', {})
        for datasource_str in res:
            datasource = Datasource(datasource_str)
            print('processing -> ' + datasource.name)
            self._batch_load_format(datasource)
            # TODO remove this (only run one time for testing)
            break

    def _batch_load_format(self, datasource):
        # Get files to process
        files_list = self._hdfs_client.list(datasource.dest_path_landing)
        # TODO Process all this in pyspark 
        files_processed = self._get_files_processed()
        files_list = self._get_files_to_process(files_list, files_processed, datasource.dest_path_landing)
        files_list = list(filter(lambda item: item not in list(files_processed), files_list))
        # Build pySpark pipeline for processing
        spark = SparkSession.builder.master("local[*]").appName("datasource - " + datasource.name).config('spark.driver.extraClassPath',
                './drivers/monetdb-jdbc-3.2.jre8.jar').getOrCreate()
        df = spark.read.parquet(*files_list)
        #jdbc url
        properties = {
            "user": parse_properties('monetdb')['database.user'],
            "password": parse_properties('monetdb')['database.password'],
            "driver": "org.monetdb.jdbc.MonetDriver"
        }
        #all files to add
        print(df.count())
        df.select("propertyCode").write.format("jdbc").mode('append').options(url="jdbc:monetdb://dodrio.fib.upc.es:50000/mydb", 
            dbtable="res", **properties).save()

    def _get_files_to_process(self, files_list, files_processed, dest_path_landing):
        res = []
        for file_name in files_list:
            if file_name not in list(files_processed):
                res.append('hdfs://dodrio.fib.upc.es:27000' + os.path.normpath(os.path.join(dest_path_landing, file_name)))
        return res

    def _get_files_processed(self):
        loaded_files = set()
        for item in self._database.find(self._log_collection_name, {}):
            loaded_files.add(item['file_name'])
        return loaded_files

    
    def _batch_load_format_inner(self, datasource):
        print(datasource.to_json())

    def save_to_log(self, file_name, datasource_name):
        entry = FormatLogEntry(file_name, time.time(), datasource_name)
        self._database.insert_one("formatLog",entry.to_dict())