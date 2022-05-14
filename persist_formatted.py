import json
import time
from models.datasource import Datasource
from models.format_log_entry import FormatLogEntry

from process import Process

class FormatLoadProcess(Process):
    _log_collection_name = 'formatLog'

    def __init__(self):
        super().__init__()

    def run_process(self):
        res = self._database.find('datasources', {})
        for datasource_str in res:
            datasource = Datasource(datasource_str)
            print('processing -> ' + datasource.name)
            self._batch_load_format_inner(datasource)
    
    def _batch_load_format_inner(self, datasource):
        print(datasource.to_json())

    def save_to_log(self, file_name, datasource_name):
        entry = FormatLogEntry(file_name, time.time(), datasource_name)
        self._database.insert_one("formatLog", entry.to_dict())