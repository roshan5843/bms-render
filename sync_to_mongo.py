# bms-render/sync_to_mongo.py
import json
import os
from pymongo import MongoClient, UpdateOne

MONGO_URI = os.getenv("MONGO_URI")  # e.g., from GitHub Secrets
DB_NAME = "bms_data"
COLLECTION = "daily_summary"

DATE_CODE = os.getenv("DATE_CODE") or ""
DATA_PATH = f"daily/data/{DATE_CODE}/finalsummary.json"

if not os.path.exists(DATA_PATH):
    print(f"❌ File not found: {DATA_PATH}")
    exit(1)

with open(DATA_PATH, "r", encoding="utf-8") as f:
    payload = json.load(f)

last_updated = payload.get("last_updated", "")
movies = payload.get("movies", {})

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[COLLECTION]

operations = []
for movie, data in movies.items():
    operations.append(
        UpdateOne(
            {"movie": movie, "date_code": DATE_CODE},
            {"$set": {"data": data, "last_updated": last_updated}},
            upsert=True
        )
    )

if operations:
    result = col.bulk_write(operations, ordered=False)
    print(
        f"✅ Upserted {result.upserted_count + result.modified_count} movie records")

print("🚀 MongoDB sync completed successfully")
client.close()
