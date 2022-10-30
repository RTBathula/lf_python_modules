import os
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId

mongodb_client = None

def db_instance():
    global mongodb_client
    
    if mongodb_client is None:
        mongodb_client = MongoClient(os.getenv('MONGODB_URI'))
        return mongodb_client

    return mongodb_client

def close_db_client():
    mongodb_client.close()

def insert_many(db, collection, doc_list):
    return mongodb_client[db][collection].insert_many(doc_list)

def find(db, collection, query, sort_name, sort_order):    
    return mongodb_client[db][collection].find(query).sort(sort_name, sort_order)

def distinct(db, collection, column):
    return mongodb_client[db][collection].distinct(column)