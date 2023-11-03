from pymongo import MongoClient

def extract_mongodb(uri, mongo_dbname):
    client = MongoClient(uri)
    dbname = client[mongo_dbname]
    collections = dbname.list_collection_names()
    return client[mongo_dbname], collections