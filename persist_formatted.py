from cgitb import scanvars
import glob
import json
import os
from sys import path
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, input_file_name, current_timestamp
from pyspark.sql.types import BooleanType

from models.datasource import Datasource
from models.format_log_entry import FormatLogEntry

from process import Process
from utils.properties_parser import parse_properties


class FormatLoadProcess(Process):
    _log_collection_name = 'formatLog'

    def __init__(self):
        super().__init__()
        self.sources_list = ['idealista', 'opendatabcn-income', 'opendatabcn-comercial']
    
    def run_process(self):
        # Enable database
        res = self._database.find('datasources', {'name': {'$in': self.sources_list}})
        for datasource_str in res:
            datasource = Datasource(datasource_str)
            print('processing -> ' + datasource.name)
            self._batch_load_format(datasource)

    # Common code for source processing
    def _batch_load_format(self, datasource):
        # Get files to process
        files_list = self._get_files_pending_process(datasource)

        if len(files_list) == 0: # If no files are need to process, just skip
            print("No files to process")
            return None
        else:
            print("let's process -> " + str(files_list))
    
        # Build pySpark pipeline for processing
        spark = SparkSession.builder.master("local[*]").appName("datasource - " + datasource.name).config('spark.driver.extraClassPath',
                './drivers/monetdb-jdbc-3.2.jre8.jar').getOrCreate()
        df = spark.read.parquet(*files_list).withColumn("_process_time", current_timestamp()).withColumn("_input_file_name", input_file_name())
        
        properties = {
            "user": parse_properties('monetdb')['database.user'],
            "password": parse_properties('monetdb')['database.password'],
            "driver": "org.monetdb.jdbc.MonetDriver"
        }

        column_types = ''
        if datasource.name == 'idealista': #Custom steps idealista
            custom_steps_res = self._custom_steps_idealista(spark, df)
            df = custom_steps_res['df']
            column_types = custom_steps_res['table_types_custom']
        
        # Write to SQL
        df.write.option("createTableColumnTypes", column_types).format("jdbc").mode('append').options(url=f"jdbc:monetdb://{parse_properties('monetdb')['database.host']}:50000/mydb", 
            dbtable=datasource.dest_table, **properties).save()
        
        #Collect processed files and store in log
        self._save_to_log_batch(files_list, datasource.name)

    def _custom_steps_idealista(self, spark, df):
        #all files to add
        df_lookup = self._get_lookup_idealista(spark)

        #Join with lookup table
        df = df.join(df_lookup, ['district', 'neighborhood'])

        #json normalizations detailedType & suggestedTexts
        
        df=df.withColumn('detailedType', to_json('detailedType'))
        df=df.withColumn('suggestedTexts', to_json('suggestedTexts'))

        # Get boolean columns to cast from BIT to Boolean
        boolean_columns = [x.name + " BOOLEAN" for x in df.schema.fields if isinstance(x.dataType, BooleanType)]
        table_types_custom = ', '.join(boolean_columns)
        return {'df': df, 'table_types_custom': table_types_custom}


    def _get_files_pending_process(self, datasource):
        files_list = self._hdfs_client.list(datasource.dest_path_landing)
        # TODO Process all this in pyspark 
        files_processed = self._get_files_processed()
        files_list = self._get_files_to_process(files_list, files_processed, datasource.dest_path_landing)
        files_list = list(filter(lambda item: item not in list(files_processed), files_list))
        return files_list

    
    def _get_lookup_idealista(self, spark):
        idealista_lookup = Datasource(self._database.find('datasources', {"name": "lookup_tables_idealista"})[0])
        file_name = self._hdfs_client.list(idealista_lookup.dest_path_landing)[0] # TODO Get latest (or first)
        path_file = 'hdfs://dodrio.fib.upc.es:27000' + os.path.normpath(os.path.join(idealista_lookup.dest_path_landing, file_name))
        lookup_idealista = spark.read.parquet(path_file)
        return lookup_idealista

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


    def _save_to_log_batch(self, files, datasource_name):
        to_insert = []
        t = time.time()
        for file in files:
            to_insert.append(FormatLogEntry(file, t, datasource_name).to_dict())
        self._database.insert_many("formatLog",to_insert)