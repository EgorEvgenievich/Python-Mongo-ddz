import pymongo

class Connect(object):
    def __init__(self,hostname='localhost',port=27017):
        self.hostname=hostname
        self.port=port
    """Get connection to localhost:27017"""
    @staticmethod
    def get_connection(hostname='localhost',port=27017):
        """return mongo client"""
        return pymongo.MongoClient(hostname,port)
