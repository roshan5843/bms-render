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
combine_dailyshards.py (REPLACE YOUR EXISTING FILE)
====================================================
Combines shards + syncs to MongoDB automatically
"""

import json
import os
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient, UpdateOne

# =====================================================
# CONFIG
# =====================================================
MONGO_URI = os.environ.get(
    "MONGO_URI", "mongodb+srv://admin-roshan:test123@cluster0.vmh1vrq.mongodb.net/movie-blog?retryWrites=true&w=majority&appName=Cluster0")
DB_NAME = os.environ.get("DB_NAME", "movie-blog")
SYNC_TO_MONGO = bool(MONGO_URI)  # Only sync if MONGO_URI is set

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)
DATE_CODE = NOW_IST.strftime("%Y%m%d")
LAST_UPDATED = NOW_IST.strftime("%Y-%m-%d %H:%M IST")

BASE_DIR = f"daily/data/{DATE_CODE}"
FINAL_DETAILED = os.path.join(BASE_DIR, "finaldetailed.json")
FINAL_SUMMARY = os.path.join(BASE_DIR, "finalsummary.json")


# =====================================================
# HELPERS
# =====================================================
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except:
        return []


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize_row(r):
    return {
        "movie": r.get("movie") or "Unknown",
        "city": r.get("city") or "Unknown",
        "state": r.get("state") or "Unknown",
        "venue": r.get("venue") or "Unknown",
        "address": r.get("address") or "",
        "time": r.get("time") or "",
        "audi": r.get("audi") or "",
        "session_id": str(r.get("session_id") or ""),
        "chain": r.get("chain") or "Unknown",
        "source": r.get("source") or "Unknown",
        "date": r.get("date") or DATE_CODE,
        "totalSeats": int(r.get("totalSeats") or 0),
        "available": int(r.get("available") or 0),
        "sold": int(r.get("sold") or 0),
        "gross": float(r.get("gross") or 0.0),
        "occupancy": f"{round(float(r.get('occupancy', 0)), 2)}%" if isinstance(r.get('occupancy'), (int, float)) else r.get('occupancy', '0%')
    }


def dedupe(rows):
    seen = set()
    out = []
    for r in rows:
        key = (r.get("venue", ""), r.get("time", ""),
               r.get("session_id", ""), r.get("audi", ""))
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


# =====================================================
# MONGODB SYNC
# =====================================================
def sync_to_mongodb(movies: dict):
    if not SYNC_TO_MONGO:
        print("⏭ MongoDB sync skipped (MONGO_URI not set)")
        return

    print(f"🔄 Syncing {len(movies)} movies to MongoDB...")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db["movies"]

    collection.create_index([("movie_name", 1), ("date_code", 1)], unique=True)

    operations = []
    now = NOW_IST

    for movie_name, m in movies.items():
        doc = {
            "movie_name": movie_name,
            "date_code": DATE_CODE,
            "last_updated": LAST_UPDATED,
            "shows": m.get("shows", 0),
            "gross": m.get("gross", 0.0),
            "sold": m.get("sold", 0),
            "total_seats": m.get("totalSeats", 0),
            "venues": m.get("venues", 0),
            "cities": m.get("cities", 0),
            "fastfilling": m.get("fastfilling", 0),
            "housefull": m.get("housefull", 0),
            "occupancy": m.get("occupancy", 0.0),
            "city_details": m.get("details", []),
            "chain_details": m.get("Chain_details", []),
            "updated_at": now,
        }
        operations.append(UpdateOne(
            {"movie_name": movie_name, "date_code": DATE_CODE},
            {"$set": doc},
            upsert=True
        ))

    result = collection.bulk_write(operations, ordered=False)
    print(
        f"✅ MongoDB: {result.upserted_count} inserted, {result.modified_count} updated")
    client.close()


# =====================================================
# MAIN
# =====================================================
def main():
    print(f"📁 Processing: {BASE_DIR}")

    # 1. Load all shards
    all_rows = []
    for i in range(1, 10):
        path = os.path.join(BASE_DIR, f"detailed{i}.json")
        data = load_json(path)
        if data:
            print(f"  ✅ detailed{i}.json → {len(data)} rows")
            all_rows.extend(data)

    # 2. Normalize & dedupe
    all_rows = [normalize_row(r) for r in all_rows]
    final_rows = dedupe(all_rows)
    final_rows.sort(key=lambda x: (
        x["movie"], x["city"], x["venue"], x["time"]))
    print(f"📊 Total: {len(final_rows)} shows")

    # 3. Save finaldetailed.json
    save_json(FINAL_DETAILED, {
              "last_updated": LAST_UPDATED, "data": final_rows})

    # 4. Build summary
    summary = {}
    for r in final_rows:
        movie = r["movie"]
        if movie not in summary:
            summary[movie] = {
                "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                "venues": set(), "cities": set(), "fastfilling": 0, "housefull": 0,
                "details": {}, "Chain_details": {}
            }

        m = summary[movie]
        total, sold, gross = r["totalSeats"], r["sold"], r["gross"]
        occ = (sold / total * 100) if total else 0

        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        m["venues"].add(r["venue"])
        m["cities"].add(r["city"])
        if occ >= 98:
            m["housefull"] += 1
        elif occ >= 50:
            m["fastfilling"] += 1

        # City details
        ck = (r["city"], r["state"])
        if ck not in m["details"]:
            m["details"][ck] = {"city": r["city"], "state": r["state"], "venues": set(
            ), "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0, "fastfilling": 0, "housefull": 0}
        d = m["details"][ck]
        d["venues"].add(r["venue"])
        d["shows"] += 1
        d["gross"] += gross
        d["sold"] += sold
        d["totalSeats"] += total
        if occ >= 98:
            d["housefull"] += 1
        elif occ >= 50:
            d["fastfilling"] += 1

        # Chain details
        chain = r["chain"]
        if chain not in m["Chain_details"]:
            m["Chain_details"][chain] = {"chain": chain, "venues": set(
            ), "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0, "fastfilling": 0, "housefull": 0}
        c = m["Chain_details"][chain]
        c["venues"].add(r["venue"])
        c["shows"] += 1
        c["gross"] += gross
        c["sold"] += sold
        c["totalSeats"] += total
        if occ >= 98:
            c["housefull"] += 1
        elif occ >= 50:
            c["fastfilling"] += 1

    # 5. Finalize summary
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
            "details": [
                {**{k: (len(v) if k == "venues" else v) for k, v in d.items()}, "occupancy": round(
                    (d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0}
                for d in m["details"].values()
            ],
            "Chain_details": [
                {**{k: (len(v) if k == "venues" else v) for k, v in c.items()}, "occupancy": round(
                    (c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0}
                for c in m["Chain_details"].values()
            ]
        }

    # 6. Save finalsummary.json
    save_json(FINAL_SUMMARY, {
              "last_updated": LAST_UPDATED, "movies": final_summary})
    print(f"🎉 Saved: {FINAL_SUMMARY}")

    # 7. Sync to MongoDB
    sync_to_mongodb(final_summary)


if __name__ == "__main__":
    main()
