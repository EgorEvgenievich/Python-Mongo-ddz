import pymongo

class Connect(object):
    @staticmethod
    def get_connection():
        return pymongo.MongoClient('mongodb://localhost:27017/')
