import pymongo

class Connect(object):
    """Get connection to localhost:27017"""
    @staticmethod
    def get_connection():
        """return mongo client"""
        return pymongo.MongoClient('mongodb://localhost:27017/')
