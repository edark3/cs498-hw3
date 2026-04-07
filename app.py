from flask import Flask, request, jsonify
from pymongo import MongoClient, WriteConcern, ReadPreference

app = Flask(__name__)

MONGO_URI = "mongodb+srv://edark3_db_user:nFGRJMFMmYKqdDis@cluster0.cdo6mbn.mongodb.net/ev_db?retryWrites=true&w=majority"

client = MongoClient(MONGO_URI)
db = client["ev_db"]
collection = db["vehicles"]

# Fast write (primary only)
@app.route('/insert-fast', methods=['POST'])
def insert_fast():
    data = request.json
    col = collection.with_options(write_concern=WriteConcern(w=1))
    result = col.insert_one(data)
    return jsonify({"id": str(result.inserted_id)})

# Safe write (majority)
@app.route('/insert-safe', methods=['POST'])
def insert_safe():
    data = request.json
    col = collection.with_options(write_concern=WriteConcern(w="majority"))
    result = col.insert_one(data)
    return jsonify({"id": str(result.inserted_id)})

# Strong read (primary)
@app.route('/count-tesla-primary', methods=['GET'])
def count_tesla_primary():
    col = collection.with_options(read_preference=ReadPreference.PRIMARY)
    count = col.count_documents({"Make": "TESLA"})
    return jsonify({"count": count})

# Eventual read (secondary preferred)
@app.route('/count-bmw-secondary', methods=['GET'])
def count_bmw_secondary():
    col = collection.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
    count = col.count_documents({"Make": "BMW"})
    return jsonify({"count": count})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
