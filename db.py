from pymongo import MongoClient

class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance.client = MongoClient("mongodb+srv://lotiyahard_db_user:H@rdik71971@cluster.mongodb.net/")
            cls._instance.db = cls._instance.client["nifty50"]
        return cls._instance

# Singleton instance
db = Database().db