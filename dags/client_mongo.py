import csv
from pymongo import MongoClient

MONGO_URI = "mongodb://mongodb:27017"
DATABASE_NAME = "default"
COLLECTION_NAME = "aggregates"


def insert_aggregates_to_mongo(**kwargs):
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    with open('/opt/airflow/data/aggregate_res.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)
        data = list(csv_reader)
        if data:
            for row in data:
                doc_id = row['fiscal_year'] + "_audited_" + row['audited']
                document = {
                    "_id": doc_id,
                    "value": row['count(1)']
                }
                print(document)
                collection.insert_one(document)

    client.close()
