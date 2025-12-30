# import json
# import os
# from datetime import datetime, timedelta
# import pytz
# from collections import defaultdict
#
# # =====================================================
# # DATE (IST TODAY) MAKE FOR TODAY!!
# # =====================================================
# IST = pytz.timezone("Asia/Kolkata")
# NOW_IST = datetime.now(IST)
# DATE_CODE = NOW_IST.strftime("%Y%m%d")
# LAST_UPDATED = NOW_IST.strftime("%Y-%m-%d %H:%M IST")
#
# BASE_DIR = f"daily/data/{DATE_CODE}"
# FINAL_DETAILED = os.path.join(BASE_DIR, "finaldetailed.json")
# FINAL_SUMMARY  = os.path.join(BASE_DIR, "finalsummary.json")
#
# print(f"📁 Using directory: {BASE_DIR}")
# print(f"⏱ Last updated: {LAST_UPDATED}")
#
# # =====================================================
# # HELPERS
# # =====================================================
# def load_json(path):
#     try:
#         with open(path, "r", encoding="utf-8") as f:
#             data = json.load(f)
#             return data if isinstance(data, list) else []
#     except Exception:
#         return []
#
# def save_json(path, data):
#     os.makedirs(os.path.dirname(path), exist_ok=True)
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(data, f, ensure_ascii=False, indent=2)
#
# # =====================================================
# # NORMALIZE ROW (🔥 KEY FIX)
# # =====================================================
# def normalize_row(r):
#     r["movie"] = r.get("movie") or "Unknown"
#     r["city"] = r.get("city") or "Unknown"
#     r["state"] = r.get("state") or "Unknown"
#     r["venue"] = r.get("venue") or "Unknown"
#     r["address"] = r.get("address") or ""
#     r["time"] = r.get("time") or ""
#     r["audi"] = r.get("audi") or ""
#     r["session_id"] = str(r.get("session_id") or "")
#     r["chain"] = r.get("chain") or "Unknown"
#     r["source"] = r.get("source") or "Unknown"
#     r["date"] = r.get("date") or DATE_CODE
#
#     r["totalSeats"] = int(r.get("totalSeats") or 0)
#     r["available"] = int(r.get("available") or 0)
#     r["sold"] = int(r.get("sold") or 0)
#     r["gross"] = float(r.get("gross") or 0.0)
#
#     occ = r.get("occupancy", "")
#     if isinstance(occ, (int, float)):
#         r["occupancy"] = f"{round(float(occ), 2)}%"
#     elif isinstance(occ, str):
#         if not occ.endswith("%"):
#             try:
#                 r["occupancy"] = f"{round(float(occ), 2)}%"
#             except:
#                 r["occupancy"] = "0%"
#     else:
#         r["occupancy"] = "0%"
#
#     return r
#
# # =====================================================
# # DEDUPE
# # =====================================================
# def dedupe(rows):
#     seen = set()
#     out = []
#     dupes = 0
#
#     for r in rows:
#         key = (
#             r.get("venue", ""),
#             r.get("time", ""),
#             r.get("session_id", ""),
#             r.get("audi", ""),
#         )
#         if key in seen:
#             dupes += 1
#             continue
#         seen.add(key)
#         out.append(r)
#
#     return out, dupes
#
# # =====================================================
# # LOAD + COMBINE SHARDS
# # =====================================================
# all_rows = []
#
# for i in range(1, 10):
#     path = os.path.join(BASE_DIR, f"detailed{i}.json")
#     data = load_json(path)
#     if data:
#         print(f"✅ detailed{i}.json → {len(data)} rows")
#         all_rows.extend(data)
#
# print(f"📊 Raw rows: {len(all_rows)}")
#
# # =====================================================
# # NORMALIZE ALL ROWS (🔥 IMPORTANT)
# # =====================================================
# all_rows = [normalize_row(r) for r in all_rows]
#
# # =====================================================
# # DEDUPE FINAL
# # =====================================================
# final_rows, dupes = dedupe(all_rows)
# print(f"🧹 Duplicates removed: {dupes}")
# print(f"🎯 Final detailed rows: {len(final_rows)}")
#
# # =====================================================
# # SORT FINAL DETAILED
# # =====================================================
# final_rows.sort(
#     key=lambda x: (
#         x["movie"],
#         x["city"],
#         x["venue"],
#         x["time"],
#     )
# )
#
# # =====================================================
# # SAVE finaldetailed.json
# # =====================================================
# save_json(
#     FINAL_DETAILED,
#     {
#         "last_updated": LAST_UPDATED,
#         "data": final_rows
#     }
# )
#
# print("🎉 finaldetailed.json saved")
#
# # =====================================================
# # BUILD FINAL SUMMARY
# # =====================================================
# summary = {}
#
# for r in final_rows:
#     movie = r["movie"]
#     city = r["city"]
#     state = r["state"]
#     venue = r["venue"]
#     chain = r["chain"]
#
#     total = r["totalSeats"]
#     sold = r["sold"]
#     gross = r["gross"]
#     occ = (sold / total * 100) if total else 0
#
#     if movie not in summary:
#         summary[movie] = {
#             "shows": 0,
#             "gross": 0.0,
#             "sold": 0,
#             "totalSeats": 0,
#             "venues": set(),
#             "cities": set(),
#             "fastfilling": 0,
#             "housefull": 0,
#             "details": {},
#             "Chain_details": {}
#         }
#
#     m = summary[movie]
#     m["shows"] += 1
#     m["gross"] += gross
#     m["sold"] += sold
#     m["totalSeats"] += total
#     m["venues"].add(venue)
#     m["cities"].add(city)
#
#     if occ >= 98:
#         m["housefull"] += 1
#     elif occ >= 50:
#         m["fastfilling"] += 1
#
#     ck = (city, state)
#     if ck not in m["details"]:
#         m["details"][ck] = {
#             "city": city,
#             "state": state,
#             "venues": set(),
#             "shows": 0,
#             "gross": 0.0,
#             "sold": 0,
#             "totalSeats": 0,
#             "fastfilling": 0,
#             "housefull": 0
#         }
#
#     d = m["details"][ck]
#     d["venues"].add(venue)
#     d["shows"] += 1
#     d["gross"] += gross
#     d["sold"] += sold
#     d["totalSeats"] += total
#     if occ >= 98:
#         d["housefull"] += 1
#     elif occ >= 50:
#         d["fastfilling"] += 1
#
#     if chain not in m["Chain_details"]:
#         m["Chain_details"][chain] = {
#             "chain": chain,
#             "venues": set(),
#             "shows": 0,
#             "gross": 0.0,
#             "sold": 0,
#             "totalSeats": 0,
#             "fastfilling": 0,
#             "housefull": 0
#         }
#
#     c = m["Chain_details"][chain]
#     c["venues"].add(venue)
#     c["shows"] += 1
#     c["gross"] += gross
#     c["sold"] += sold
#     c["totalSeats"] += total
#     if occ >= 98:
#         c["housefull"] += 1
#     elif occ >= 50:
#         c["fastfilling"] += 1
#
# # =====================================================
# # FINALIZE SUMMARY
# # =====================================================
# final_summary = {}
#
# for movie, m in summary.items():
#     final_summary[movie] = {
#         "shows": m["shows"],
#         "gross": round(m["gross"], 2),
#         "sold": m["sold"],
#         "totalSeats": m["totalSeats"],
#         "venues": len(m["venues"]),
#         "cities": len(m["cities"]),
#         "fastfilling": m["fastfilling"],
#         "housefull": m["housefull"],
#         "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
#         "details": [],
#         "Chain_details": []
#     }
#
#     for d in m["details"].values():
#         final_summary[movie]["details"].append({
#             "city": d["city"],
#             "state": d["state"],
#             "venues": len(d["venues"]),
#             "shows": d["shows"],
#             "gross": round(d["gross"], 2),
#             "sold": d["sold"],
#             "totalSeats": d["totalSeats"],
#             "fastfilling": d["fastfilling"],
#             "housefull": d["housefull"],
#             "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0
#         })
#
#     for c in m["Chain_details"].values():
#         final_summary[movie]["Chain_details"].append({
#             "chain": c["chain"],
#             "venues": len(c["venues"]),
#             "shows": c["shows"],
#             "gross": round(c["gross"], 2),
#             "sold": c["sold"],
#             "totalSeats": c["totalSeats"],
#             "fastfilling": c["fastfilling"],
#             "housefull": c["housefull"],
#             "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0
#         })
#
# # =====================================================
# # SAVE finalsummary.json
# # =====================================================
# save_json(
#     FINAL_SUMMARY,
#     {
#         "last_updated": LAST_UPDATED,
#         "movies": final_summary
#     }
# )
#
# print("🎉 finalsummary.json created successfully")
# print("📄 Files ready:")
# print(f"   • {FINAL_DETAILED}")
# print(f"   • {FINAL_SUMMARY}")


"""
combine_dailyshards_with_sync.py
================================
Modified version of combine_dailyshards.py that automatically syncs to MongoDB.
Replace your existing combine_dailyshards.py with this file.
"""

import json
import os
import hashlib
import time
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

# =====================================================
# MONGODB CONFIG
# =====================================================
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "movietracker")
SYNC_TO_MONGO = os.environ.get("SYNC_TO_MONGO", "true").lower() == "true"

# =====================================================
# DATE (IST TODAY)
# =====================================================
IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)
DATE_CODE = NOW_IST.strftime("%Y%m%d")
LAST_UPDATED = NOW_IST.strftime("%Y-%m-%d %H:%M IST")

BASE_DIR = f"daily/data/{DATE_CODE}"
FINAL_DETAILED = os.path.join(BASE_DIR, "finaldetailed.json")
FINAL_SUMMARY = os.path.join(BASE_DIR, "finalsummary.json")

print(f"📁 Using directory: {BASE_DIR}")
print(f"⏱ Last updated: {LAST_UPDATED}")


# =====================================================
# HELPERS
# =====================================================
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize_row(r):
    r["movie"] = r.get("movie") or "Unknown"
    r["city"] = r.get("city") or "Unknown"
    r["state"] = r.get("state") or "Unknown"
    r["venue"] = r.get("venue") or "Unknown"
    r["address"] = r.get("address") or ""
    r["time"] = r.get("time") or ""
    r["audi"] = r.get("audi") or ""
    r["session_id"] = str(r.get("session_id") or "")
    r["chain"] = r.get("chain") or "Unknown"
    r["source"] = r.get("source") or "Unknown"
    r["date"] = r.get("date") or DATE_CODE

    r["totalSeats"] = int(r.get("totalSeats") or 0)
    r["available"] = int(r.get("available") or 0)
    r["sold"] = int(r.get("sold") or 0)
    r["gross"] = float(r.get("gross") or 0.0)

    occ = r.get("occupancy", "")
    if isinstance(occ, (int, float)):
        r["occupancy"] = f"{round(float(occ), 2)}%"
    elif isinstance(occ, str):
        if not occ.endswith("%"):
            try:
                r["occupancy"] = f"{round(float(occ), 2)}%"
            except:
                r["occupancy"] = "0%"
    else:
        r["occupancy"] = "0%"

    return r


def dedupe(rows):
    seen = set()
    out = []
    dupes = 0

    for r in rows:
        key = (
            r.get("venue", ""),
            r.get("time", ""),
            r.get("session_id", ""),
            r.get("audi", ""),
        )
        if key in seen:
            dupes += 1
            continue
        seen.add(key)
        out.append(r)

    return out, dupes


# =====================================================
# MONGODB SYNC
# =====================================================
def sync_to_mongodb(final_summary: dict) -> bool:
    """Sync summary to MongoDB with bulk upserts."""
    if not SYNC_TO_MONGO:
        print("⏭ MongoDB sync disabled")
        return True

    start_time = time.time()
    movies = final_summary.get("movies", {})

    if not movies:
        print("⚠ No movies to sync")
        return False

    print(f"🔄 Syncing {len(movies)} movies to MongoDB...")

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db["movies"]

        # Ensure indexes
        collection.create_index(
            [("movie_name", 1), ("date_code", 1)], unique=True)
        collection.create_index([("gross", -1)])
        collection.create_index([("occupancy", -1)])

        # Compute hash for change detection
        data_hash = hashlib.md5(
            json.dumps(movies, sort_keys=True, default=str).encode()
        ).hexdigest()

        # Check if changed
        meta = db["sync_meta"].find_one({"_id": f"sync_{DATE_CODE}"})
        if meta and meta.get("hash") == data_hash:
            print("✅ No changes detected, skipping MongoDB sync")
            client.close()
            return True

        # Prepare bulk operations
        now = NOW_IST
        operations = []

        for movie_name, movie_data in movies.items():
            doc = {
                "movie_name": movie_name,
                "date_code": DATE_CODE,
                "last_updated": LAST_UPDATED,
                "shows": movie_data.get("shows", 0),
                "gross": movie_data.get("gross", 0.0),
                "sold": movie_data.get("sold", 0),
                "total_seats": movie_data.get("totalSeats", 0),
                "venues": movie_data.get("venues", 0),
                "cities": movie_data.get("cities", 0),
                "fastfilling": movie_data.get("fastfilling", 0),
                "housefull": movie_data.get("housefull", 0),
                "occupancy": movie_data.get("occupancy", 0.0),
                "city_details": movie_data.get("details", []),
                "chain_details": movie_data.get("Chain_details", []),
                "updated_at": now,
            }

            operations.append(
                UpdateOne(
                    {"movie_name": movie_name, "date_code": DATE_CODE},
                    {"$set": doc},
                    upsert=True
                )
            )

        # Execute bulk upsert
        result = collection.bulk_write(operations, ordered=False)
        print(
            f"📝 MongoDB: Upserted={result.upserted_count}, Modified={result.modified_count}")

        # Update sync metadata
        db["sync_meta"].update_one(
            {"_id": f"sync_{DATE_CODE}"},
            {
                "$set": {
                    "hash": data_hash,
                    "synced_at": now,
                    "movies_count": len(movies)
                }
            },
            upsert=True
        )

        elapsed = time.time() - start_time
        print(f"✅ MongoDB sync complete in {elapsed:.2f}s")

        client.close()
        return True

    except Exception as e:
        print(f"❌ MongoDB sync error: {e}")
        return False


# =====================================================
# MAIN PROCESSING
# =====================================================
def main():
    # Load all shards
    all_rows = []

    for i in range(1, 10):
        path = os.path.join(BASE_DIR, f"detailed{i}.json")
        data = load_json(path)
        if data:
            print(f"✅ detailed{i}.json → {len(data)} rows")
            all_rows.extend(data)

    print(f"📊 Raw rows: {len(all_rows)}")

    # Normalize
    all_rows = [normalize_row(r) for r in all_rows]

    # Dedupe
    final_rows, dupes = dedupe(all_rows)
    print(f"🧹 Duplicates removed: {dupes}")
    print(f"🎯 Final detailed rows: {len(final_rows)}")

    # Sort
    final_rows.sort(
        key=lambda x: (
            x["movie"],
            x["city"],
            x["venue"],
            x["time"],
        )
    )

    # Save finaldetailed.json
    save_json(
        FINAL_DETAILED,
        {
            "last_updated": LAST_UPDATED,
            "data": final_rows
        }
    )
    print("🎉 finaldetailed.json saved")

    # Build summary
    summary = {}

    for r in final_rows:
        movie = r["movie"]
        city = r["city"]
        state = r["state"]
        venue = r["venue"]
        chain = r["chain"]

        total = r["totalSeats"]
        sold = r["sold"]
        gross = r["gross"]
        occ = (sold / total * 100) if total else 0

        if movie not in summary:
            summary[movie] = {
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "venues": set(),
                "cities": set(),
                "fastfilling": 0,
                "housefull": 0,
                "details": {},
                "Chain_details": {}
            }

        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)

        if occ >= 98:
            m["housefull"] += 1
        elif occ >= 50:
            m["fastfilling"] += 1

        # City details
        ck = (city, state)
        if ck not in m["details"]:
            m["details"][ck] = {
                "city": city,
                "state": state,
                "venues": set(),
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0,
                "housefull": 0
            }

        d = m["details"][ck]
        d["venues"].add(venue)
        d["shows"] += 1
        d["gross"] += gross
        d["sold"] += sold
        d["totalSeats"] += total
        if occ >= 98:
            d["housefull"] += 1
        elif occ >= 50:
            d["fastfilling"] += 1

        # Chain details
        if chain not in m["Chain_details"]:
            m["Chain_details"][chain] = {
                "chain": chain,
                "venues": set(),
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0,
                "housefull": 0
            }

        c = m["Chain_details"][chain]
        c["venues"].add(venue)
        c["shows"] += 1
        c["gross"] += gross
        c["sold"] += sold
        c["totalSeats"] += total
        if occ >= 98:
            c["housefull"] += 1
        elif occ >= 50:
            c["fastfilling"] += 1

    # Finalize summary
    final_summary = {}

    for movie, m in summary.items():
        final_summary[movie] = {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
            "details": [],
            "Chain_details": []
        }

        for d in m["details"].values():
            final_summary[movie]["details"].append({
                "city": d["city"],
                "state": d["state"],
                "venues": len(d["venues"]),
                "shows": d["shows"],
                "gross": round(d["gross"], 2),
                "sold": d["sold"],
                "totalSeats": d["totalSeats"],
                "fastfilling": d["fastfilling"],
                "housefull": d["housefull"],
                "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0
            })

        for c in m["Chain_details"].values():
            final_summary[movie]["Chain_details"].append({
                "chain": c["chain"],
                "venues": len(c["venues"]),
                "shows": c["shows"],
                "gross": round(c["gross"], 2),
                "sold": c["sold"],
                "totalSeats": c["totalSeats"],
                "fastfilling": c["fastfilling"],
                "housefull": c["housefull"],
                "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0
            })

    # Prepare final output
    final_output = {
        "last_updated": LAST_UPDATED,
        "movies": final_summary
    }

    # Save finalsummary.json
    save_json(FINAL_SUMMARY, final_output)
    print("🎉 finalsummary.json created successfully")

    # Sync to MongoDB
    sync_to_mongodb(final_output)

    print("📄 Files ready:")
    print(f"   • {FINAL_DETAILED}")
    print(f"   • {FINAL_SUMMARY}")


if __name__ == "__main__":
    main()
