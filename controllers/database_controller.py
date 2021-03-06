from utils.properties_parser import parse_properties
import pymongo



class DatabaseMongo:
    _uri = None
    _database = None

    def __init__(self, database_name):
        self._initialize(database_name)

    def _generate_uri(self):
        props = parse_properties('mongodb')
        username = props['database.user']
        password = props['database.password']
        host = props['database.host']
        self._uri = f'mongodb://{username}:{password}@{host}'

    def _initialize(self, database_name):
        self._generate_uri()
        client = pymongo.MongoClient(self._uri)
        self._database = client[database_name]

    def insert_one(self, collection, data):
        self._database[collection].insert_one(data)
    
    def insert_many(self, collection, data):
        self._database[collection].insert_many(data)

    def find(self, collection, query):
        return self._database[collection].find(query)

    def find_one(self, collection, query):
        return self._database[collection].find_one(query)

    def aggregate(self, collection, pipeline):
        return self._database[collection].aggregate(pipeline)

    def collection(self, collection):
        return self._database[collection]
