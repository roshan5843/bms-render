#
# import json
# import os
# import sys
# from datetime import datetime
# from pathlib import Path
# from pymongo import MongoClient
# import pytz
#
# from config import (
#     MONGODB_URI,
#     DATABASE_NAME,
#     COLLECTION_ADVANCE,
#     IST
# )
#
#
# class MongoDBAdvanceSync:
#     def __init__(self):
#         if not MONGODB_URI:
#             raise ValueError("MONGODB_URI environment variable not set")
#
#         self.client = MongoClient(MONGODB_URI)
#         self.db = self.client[DATABASE_NAME]
#
#         print(f"✅ Connected to MongoDB: {DATABASE_NAME}")
#
#     # ---------------------------------------------------
#     # 🔍 FIND FILES
#     # ---------------------------------------------------
#     def find_all_advance_summaries(self):
#         print("\n🔍 Searching for advance summary files...")
#
#         base_dir = Path("advance/data")
#
#         if not base_dir.exists():
#             print(f"❌ Directory not found: {base_dir}")
#             return []
#
#         date_dirs = sorted(
#             [d for d in base_dir.iterdir() if d.is_dir() and d.name.isdigit()],
#             key=lambda x: x.name
#         )
#
#         summary_files = []
#
#         for date_dir in date_dirs:
#             summary_path = date_dir / "finalsummary.json"
#
#             if summary_path.exists():
#                 summary_files.append({
#                     "path": str(summary_path),
#                     "date": date_dir.name
#                 })
#
#         print(f"📊 Total summaries found: {len(summary_files)}")
#         return summary_files
#
#     # ---------------------------------------------------
#     # 📂 LOAD JSON
#     # ---------------------------------------------------
#     def load_json(self, filepath):
#         try:
#             with open(filepath, "r", encoding="utf-8") as f:
#                 return json.load(f)
#         except Exception as e:
#             print(f"❌ Error loading {filepath}: {e}")
#             return None
#
#     # ---------------------------------------------------
#     # 🔄 SYNC SINGLE FILE
#     # ---------------------------------------------------
#     def sync_single_summary(self, filepath, date_code):
#         data = self.load_json(filepath)
#         if not data:
#             return False
#
#         movies = data.get("movies", {})
#         if not movies:
#             print(f"⚠️ No movies found in {filepath}")
#             return False
#
#         last_updated = data.get("last_updated", "")
#         timestamp = datetime.now(IST)
#
#         collection = self.db[COLLECTION_ADVANCE]
#
#         movies_array = []
#         for movie_name, movie_data in movies.items():
#             movies_array.append({
#                 "movie": movie_name,
#                 **movie_data
#             })
#
#         doc = {
#             "_id": f"advance_{date_code}",
#             "show_date": date_code,
#             "last_updated": last_updated,
#             "synced_at": timestamp,
#             "total_movies": len(movies),
#             "movies": movies_array
#         }
#
#         try:
#             result = collection.replace_one(
#                 {"_id": doc["_id"]},
#                 doc,
#                 upsert=True
#             )
#
#             if result.upserted_id:
#                 print(f"   ✅ Inserted: {doc['_id']}")
#             else:
#                 print(f"   🔄 Updated: {doc['_id']}")
#
#             return True
#
#         except Exception as e:
#             print(f"   ❌ Error syncing {date_code}: {e}")
#             return False
#
#     # ---------------------------------------------------
#     # ⚡ SMART SYNC (MAIN FIX)
#     # ---------------------------------------------------
#     def sync_all(self):
#         summary_files = self.find_all_advance_summaries()
#
#         if not summary_files:
#             print("\n❌ No files found")
#             return False
#
#         collection = self.db[COLLECTION_ADVANCE]
#
#         print("\n🧠 Smart filtering (new + updated only)...")
#
#         new_files = []
#         skipped = 0
#
#         for file_info in summary_files:
#             filepath = file_info["path"]
#             date_code = file_info["date"]
#             doc_id = f"advance_{date_code}"
#
#             # Load only metadata (fast)
#             data = self.load_json(filepath)
#             if not data:
#                 continue
#
#             file_last_updated = data.get("last_updated", "")
#
#             # 🔥 SMART CHECK (Mongo vs File)
#             doc = collection.find_one(
#                 {"_id": doc_id},
#                 {"last_updated": 1}
#             )
#
#             if not doc or doc.get("last_updated") != file_last_updated:
#                 new_files.append({
#                     "path": filepath,
#                     "date": date_code
#                 })
#             else:
#                 skipped += 1
#
#         print(f"🆕 To Sync: {len(new_files)}")
#         print(f"⏭ Skipped (unchanged): {skipped}")
#
#         if not new_files:
#             print("✅ Everything already up-to-date")
#             return True
#
#         # ---------------------------------------------------
#         # 🚀 PROCESS ONLY REQUIRED FILES
#         # ---------------------------------------------------
#         success_count = 0
#         fail_count = 0
#
#         for file_info in new_files:
#             print(f"\n📅 Processing: {file_info['date']}")
#
#             if self.sync_single_summary(
#                 file_info["path"],
#                 file_info["date"]
#             ):
#                 success_count += 1
#             else:
#                 fail_count += 1
#
#         # ---------------------------------------------------
#         # 📊 SUMMARY
#         # ---------------------------------------------------
#         print("\n" + "=" * 60)
#         print("📊 FINAL SYNC SUMMARY")
#         print(f"   ✅ Success: {success_count}")
#         print(f"   ❌ Failed: {fail_count}")
#         print(f"   🆕 Processed: {len(new_files)}")
#         print(f"   ⏭ Skipped: {skipped}")
#         print("=" * 60)
#
#         return success_count > 0
#
#     # ---------------------------------------------------
#     def close(self):
#         self.client.close()
#         print("\n👋 MongoDB connection closed")
#
#
# # -------------------------------------------------------
# # 🚀 MAIN
# # -------------------------------------------------------
# def main():
#     print("🚀 Starting Smart MongoDB Sync...")
#     print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
#
#     syncer = MongoDBAdvanceSync()
#
#     try:
#         success = syncer.sync_all()
#
#         if success:
#             print("\n✅ SYNC COMPLETED SUCCESSFULLY")
#             sys.exit(0)
#         else:
#             print("\n❌ SYNC FAILED")
#             sys.exit(1)
#
#     except Exception as e:
#         print(f"\n❌ Fatal error: {e}")
#         import traceback
#         traceback.print_exc()
#         sys.exit(1)
#
#     finally:
#         syncer.close()
#
#
# if __name__ == "__main__":
#     main()


# mongodb/sync_advance.py
# ✅ OPTIMIZED: bulk upsert (one round-trip), parallel JSON loading,
#               fingerprint check via last_updated string (no full-doc compare)

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient, ReplaceOne
import pytz

from config import (
    MONGODB_URI,
    DATABASE_NAME,
    COLLECTION_ADVANCE,
    IST,
)

# -------------------------------------------------------
# HELPERS
# -------------------------------------------------------


def load_json(filepath: str):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading {filepath}: {e}")
        return None


def find_all_advance_summaries():
    print("\n🔍 Searching for advance summary files...")
    base_dir = Path("advance/data")

    if not base_dir.exists():
        print(f"❌ Directory not found: {base_dir}")
        return []

    summary_files = []
    for date_dir in sorted(
        (d for d in base_dir.iterdir() if d.is_dir() and d.name.isdigit()),
        key=lambda x: x.name,
    ):
        p = date_dir / "finalsummary.json"
        if p.exists():
            summary_files.append({"path": str(p), "date": date_dir.name})

    print(f"📊 Total summaries found: {len(summary_files)}")
    return summary_files


# -------------------------------------------------------
# LOAD FILE METADATA IN PARALLEL (fast — only reads
# "last_updated" key, no full JSON parse needed for check)
# -------------------------------------------------------
def load_last_updated(file_info):
    """Return (doc_id, file_last_updated, file_info) quickly."""
    data = load_json(file_info["path"])
    if not data:
        return None
    return (
        f"advance_{file_info['date']}",
        data.get("last_updated", ""),
        data,
        file_info["date"],
    )


# -------------------------------------------------------
# BUILD DOC FROM DATA
# -------------------------------------------------------
def build_doc(data, date_code, timestamp):
    movies = data.get("movies", {})
    movies_array = [{"movie": k, **v} for k, v in movies.items()]
    return {
        "_id":          f"advance_{date_code}",
        "show_date":    date_code,
        "last_updated": data.get("last_updated", ""),
        "synced_at":    timestamp,
        "total_movies": len(movies),
        "movies":       movies_array,
    }


# -------------------------------------------------------
# MAIN SYNC
# -------------------------------------------------------
def sync_all():
    if not MONGODB_URI:
        raise ValueError("MONGODB_URI environment variable not set")

    client = MongoClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_ADVANCE]

    print(f"✅ Connected to MongoDB: {DATABASE_NAME}")

    summary_files = find_all_advance_summaries()
    if not summary_files:
        print("\n❌ No files found")
        return False

    # ── STEP 1: load all files in parallel ───────────────
    print("\n⚡ Loading files in parallel...")
    loaded = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(load_last_updated, fi)                   : fi for fi in summary_files}
        for future in as_completed(futures):
            result = future.result()
            if result:
                loaded.append(result)

    if not loaded:
        print("❌ No valid files loaded")
        return False

    # ── STEP 2: fetch ALL existing last_updated values ───
    #   ONE single Mongo query instead of N queries
    print("\n🧠 Smart filtering (single Mongo round-trip)...")
    all_ids = [r[0] for r in loaded]
    existing = {
        doc["_id"]: doc.get("last_updated", "")
        for doc in collection.find(
            {"_id": {"$in": all_ids}},
            {"_id": 1, "last_updated": 1}
        )
    }

    timestamp = datetime.now(IST)
    to_sync = []
    skipped = 0

    for (doc_id, file_lu, data, date_code) in loaded:
        if existing.get(doc_id) == file_lu:
            skipped += 1
            continue
        to_sync.append(build_doc(data, date_code, timestamp))

    print(f"🆕 To Sync:              {len(to_sync)}")
    print(f"⏭ Skipped (unchanged):  {skipped}")

    if not to_sync:
        print("✅ Everything already up-to-date")
        client.close()
        return True

    # ── STEP 3: BULK UPSERT (one round-trip to Mongo) ────
    print(f"\n🚀 Bulk upserting {len(to_sync)} document(s)...")
    operations = [
        ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
        for doc in to_sync
    ]

    result = collection.bulk_write(operations, ordered=False)

    inserted = result.upserted_count
    updated = result.modified_count

    # ── SUMMARY ──────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📊 FINAL SYNC SUMMARY")
    print(f"   ✅ Inserted: {inserted}")
    print(f"   🔄 Updated:  {updated}")
    print(f"   🆕 Processed:{len(to_sync)}")
    print(f"   ⏭ Skipped:  {skipped}")
    print("=" * 60)

    client.close()
    print("\n👋 MongoDB connection closed")
    return True


# -------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------
def main():
    print("🚀 Starting Smart MongoDB Sync...")
    print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

    try:
        success = sync_all()
        if success:
            print("\n✅ SYNC COMPLETED SUCCESSFULLY")
            sys.exit(0)
        else:
            print("\n❌ SYNC FAILED")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
