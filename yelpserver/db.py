from pymongo import MongoClient

class DataBase:
    def __init__(self):
        pass

    def connect(self, host, port):
        self.connection = MongoClient(host, port)
        return self.connection

    def getCollection(self, dbName, collectionName):
        self.collection = self.connection[dbName][collectionName]      
        return self.collection
