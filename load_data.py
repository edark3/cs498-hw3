import pandas as pd
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)
db = client["ev_db"]
collection = db["vehicles"]

csv_file = "Electric_Vehicle_Population_Data.csv"

df = pd.read_csv(csv_file)
records = df.fillna("").to_dict(orient="records")

if records:
    batch_size = 1000

    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        collection.insert_many(batch)
        print(f"Inserted {i + len(batch)} records")

    print(f"Finished inserting {len(records)} records.")
else:
    print("No records found.")
