# config/db_config.py

from pymongo import MongoClient
from dotenv import load_dotenv
import os
from utils.logger import logger
from pathlib import Path
from urllib.parse import quote_plus



env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

class Database:
    """Singleton MongoDB connection handler with logging."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)

            mongo_uri = os.getenv("MONGO_URI")
            db_name = os.getenv("DB_NAME")

            if not mongo_uri or not db_name:
                logger.error("Missing MONGO_URI or DB_NAME in environment variables.")
                raise ValueError("Missing MONGO_URI or DB_NAME in environment variables.")

            try:
                user = os.getenv("MONGO_USER")
                password = os.getenv("MONGO_PASS")
                cluster = os.getenv("MONGO_CLUSTER")
                db_name = os.getenv("DB_NAME")

                if not all([user, password, cluster, db_name]):
                    logger.error("Missing MongoDB credentials in .env")
                    raise ValueError("Missing MongoDB credentials in .env")

                # Encode password safely
                #mongo_uri = f"mongodb+srv://{user}:{quote_plus(password)}@{cluster}/"

                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
                db = client[db_name]
                client.admin.command('ping')
                logger.info(f"✅ Connected to MongoDB: {db_name}")
                cls._instance.client = client
                cls._instance.db = db
            except Exception as e:
                logger.exception(f"❌ MongoDB connection failed: {e}")
                raise e

        return cls._instance

# Global instance
db = Database().db
