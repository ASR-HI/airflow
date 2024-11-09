"""from pymongo import MongoClient
import pandas as pd
import os

def load_mongo_data():
    mongo_uri = os.getenv("MONGO_URI")
    database_name = os.getenv("DB_NAME")
    collection_name = os.getenv("COLLECTION_NAME")
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    # Récupérer les données sans l'ID MongoDB
    mongo_data = list(collection.find())
    for document in mongo_data:
        if '_id' in document:
            del document['_id']
    client.close()
    
    # Convertir en DataFrame
    mongo_df = pd.DataFrame(mongo_data)
    return mongo_df
"""