# #bms-render/sync_to_mongo.py
# import json
# import os
# from pymongo import MongoClient, UpdateOne
#
# MONGO_URI = os.getenv("MONGO_URI")  # e.g., from GitHub Secrets
# DB_NAME = "movie-blog"
# COLLECTION = "bms_data"
#
# DATE_CODE = os.getenv("DATE_CODE") or ""
# DATA_PATH = f"daily/data/{DATE_CODE}/finalsummary.json"
#
# if not os.path.exists(DATA_PATH):
#     print(f"❌ File not found: {DATA_PATH}")
#     exit(1)
#
# with open(DATA_PATH, "r", encoding="utf-8") as f:
#     payload = json.load(f)
#
# last_updated = payload.get("last_updated", "")
# movies = payload.get("movies", {})
#
# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]
# col = db[COLLECTION]
#
# operations = []
# for movie, data in movies.items():
#     operations.append(
#         UpdateOne(
#             {"movie": movie, "date_code": DATE_CODE},
#             {"$set": {"data": data, "last_updated": last_updated}},
#             upsert=True
#         )
#     )
#
# if operations:
#     result = col.bulk_write(operations, ordered=False)
#     print(
#         f"✅ Upserted {result.upserted_count + result.modified_count} movie records")
#
# print("🚀 MongoDB sync completed successfully")
# client.close()


import os
import json
import glob
from datetime import datetime
from pymongo import MongoClient, UpdateOne

# ========================================================
# 🔧 CONFIG
# ========================================================
MONGO_URI = os.getenv("MONGO_URI")  # GitHub Secret
DB_NAME = "movie-blog"
COLLECTION = "bms_data"

# Auto-detect latest daily folder — no need for DATE_CODE env
base_dir = "daily/data"
folders = sorted(glob.glob(os.path.join(base_dir, "*")), reverse=True)
if not folders:
    raise FileNotFoundError("❌ No date folders found under daily/data/")
latest_dir = folders[0]
summary_path = os.path.join(latest_dir, "finalsummary.json")

if not os.path.exists(summary_path):
    raise FileNotFoundError(f"❌ finalsummary.json missing in: {latest_dir}")

print(f"📁 Using summary file: {summary_path}")

# ========================================================
# 📖 LOAD DATA
# ========================================================
with open(summary_path, "r", encoding="utf-8") as f:
    payload = json.load(f)

last_updated = payload.get("last_updated", datetime.utcnow().isoformat())
movies = payload.get("movies", {})

if not movies:
    print("⚠️ No movie data found in finalsummary.json")
    exit(0)

# ========================================================
# 💾 CONNECT MONGODB
# ========================================================
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[COLLECTION]

# ========================================================
# 🧠 PREPARE UPSERTS
# ========================================================
date_code = os.path.basename(latest_dir)

ops = []
for movie, info in movies.items():
    doc = {
        "movie": movie,
        "date_code": date_code,
        "last_updated": last_updated,
        "meta": {
            "shows": info.get("shows", 0),
            "gross": info.get("gross", 0.0),
            "sold": info.get("sold", 0),
            "totalSeats": info.get("totalSeats", 0),
            "fastfilling": info.get("fastfilling", 0),
            "housefull": info.get("housefull", 0),
            "occupancy": info.get("occupancy", 0.0),
            "venues": info.get("venues", 0),
            "cities": info.get("cities", 0),
        },
        "details": info.get("details", []),
        "chain_details": info.get("Chain_details", []),
    }

    ops.append(UpdateOne(
        {"movie": movie, "date_code": date_code},
        {"$set": doc},
        upsert=True
    ))

# ========================================================
# 🚀 EXECUTE BULK UPSERT
# ========================================================
if ops:
    result = col.bulk_write(ops, ordered=False)
    inserted = result.upserted_count
    modified = result.modified_count
    print(f"✅ Mongo sync complete: inserted={inserted}, updated={modified}")
else:
    print("⚠️ No upsert operations generated (empty data?)")

client.close()
