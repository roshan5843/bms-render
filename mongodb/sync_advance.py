
# # mongodb/sync_advance.py — R2 VERSION
# import json
# import os
# import sys
# from datetime import datetime
# from pymongo import MongoClient
# import pytz

# from config import MONGODB_URI, DATABASE_NAME, COLLECTION_ADVANCE, IST
# from r2_client import r2_download_json, get_r2, BUCKET


# class MongoDBAdvanceSync:
#     def __init__(self):
#         if not MONGODB_URI:
#             raise ValueError("MONGODB_URI environment variable not set")
#         self.client = MongoClient(MONGODB_URI)
#         self.db = self.client[DATABASE_NAME]
#         print(f"✅ Connected to MongoDB: {DATABASE_NAME}")

#     def find_all_advance_dates(self):
#         """List all advance date folders in R2."""
#         print("\n🔍 Scanning R2 for advance summary files...")
#         r2 = get_r2()
#         try:
#             paginator = r2.get_paginator("list_objects_v2")
#             pages = paginator.paginate(
#                 Bucket=BUCKET, Prefix="advance/", Delimiter="/")

#             date_codes = []
#             for page in pages:
#                 for prefix in page.get("CommonPrefixes", []):
#                     # e.g. "advance/20260502/"
#                     folder = prefix["Prefix"]
#                     date_part = folder.rstrip("/").split("/")[-1]
#                     if date_part.isdigit() and len(date_part) == 8:
#                         date_codes.append(date_part)

#             date_codes.sort()
#             print(f"📊 Total advance dates found in R2: {len(date_codes)}")
#             return date_codes

#         except Exception as e:
#             print(f"❌ R2 listing error: {e}")
#             return []

#     def r2_key_exists(self, key):
#         r2 = get_r2()
#         try:
#             r2.head_object(Bucket=BUCKET, Key=key)
#             return True
#         except:
#             return False

#     def sync_single_summary(self, r2_key, date_code):
#         data = r2_download_json(r2_key)
#         if not data:
#             return False

#         movies = data.get("movies", {})
#         if not movies:
#             print(f"⚠️  No movies in {r2_key}")
#             return False

#         last_updated = data.get("last_updated", "")
#         timestamp = datetime.now(IST)
#         collection = self.db[COLLECTION_ADVANCE]

#         movies_array = [{"movie": name, **mdata}
#                         for name, mdata in movies.items()]

#         doc = {
#             "_id":          f"advance_{date_code}",
#             "show_date":    date_code,
#             "last_updated": last_updated,
#             "synced_at":    timestamp,
#             "total_movies": len(movies),
#             "movies":       movies_array
#         }

#         try:
#             result = collection.replace_one(
#                 {"_id": doc["_id"]}, doc, upsert=True)
#             action = "Inserted" if result.upserted_id else "Updated"
#             print(f"   ✅ {action}: advance_{date_code} ({len(movies)} movies)")
#             return True
#         except Exception as e:
#             print(f"   ❌ Error syncing {date_code}: {e}")
#             return False

#     # def sync_all(self):
#     #     date_codes = self.find_all_advance_dates()
#     #     if not date_codes:
#     #         print("\n❌ No advance dates found in R2")
#     #         return False

#     #     collection = self.db[COLLECTION_ADVANCE]
#     #     print("\n🧠 Smart filtering (new + updated only)...")

#     #     to_sync = []
#     #     skipped = 0

#     #     for date_code in date_codes:
#     #         r2_key = f"advance/{date_code}/finalsummary.json"

#     #         # Check if R2 file exists
#     #         if not self.r2_key_exists(r2_key):
#     #             print(f"⚠️  No finalsummary.json for {date_code}, skipping")
#     #             continue

#     #         # Download just to check last_updated (smart check)
#     #         data = r2_download_json(r2_key)
#     #         if not data:
#     #             continue

#     #         file_last_updated = data.get("last_updated", "")
#     #         doc_id = f"advance_{date_code}"

#     #         # Compare with MongoDB
#     #         existing = collection.find_one(
#     #             {"_id": doc_id}, {"last_updated": 1})

#     #         if not existing or existing.get("last_updated") != file_last_updated:
#     #             to_sync.append(
#     #                 {"r2_key": r2_key, "date_code": date_code, "data": data})
#     #         else:
#     #             skipped += 1

#     #     print(f"🆕 To sync: {len(to_sync)}")
#     #     print(f"⏭  Skipped (unchanged): {skipped}")

#     #     if not to_sync:
#     #         print("✅ Everything already up-to-date")
#     #         return True

#     #     success_count = 0
#     #     fail_count = 0

#     #     for item in to_sync:
#     #         print(f"\n📅 Processing: {item['date_code']}")
#     #         # Pass already-downloaded data directly
#     #         movies = item["data"].get("movies", {})
#     #         if not movies:
#     #             fail_count += 1
#     #             continue

#     #         last_updated = item["data"].get("last_updated", "")
#     #         date_code = item["date_code"]
#     #         movies_array = [{"movie": name, **mdata}
#     #                         for name, mdata in movies.items()]
#     #         doc = {
#     #             "_id":          f"advance_{date_code}",
#     #             "show_date":    date_code,
#     #             "last_updated": last_updated,
#     #             "synced_at":    datetime.now(IST),
#     #             "total_movies": len(movies),
#     #             "movies":       movies_array
#     #         }
#     #         try:
#     #             result = collection.replace_one(
#     #                 {"_id": doc["_id"]}, doc, upsert=True)
#     #             action = "Inserted" if result.upserted_id else "Updated"
#     #             print(
#     #                 f"   ✅ {action}: advance_{date_code} ({len(movies)} movies)")
#     #             success_count += 1
#     #         except Exception as e:
#     #             print(f"   ❌ Error: {e}")
#     #             fail_count += 1

#     #     print("\n" + "="*60)
#     #     print(
#     #         f"✅ Success: {success_count} | ❌ Failed: {fail_count} | ⏭ Skipped: {skipped}")
#     #     print("="*60)
#     #     return success_count > 0

#     def sync_all(self):
#         date_codes = self.find_all_advance_dates()
#         if not date_codes:
#             print("\n❌ No advance dates found in R2")
#             return False

#         collection = self.db[COLLECTION_ADVANCE]

#     # ✅ FIX: Find the oldest date still present in MongoDB.
#     existing_dates = collection.distinct("show_date")
#     if existing_dates:
#         min_existing = min(existing_dates)
#         print(f"\n📅 Oldest date in MongoDB: {min_existing} — skipping R2 dates before this")
#         date_codes = [d for d in date_codes if d >= min_existing]
#     else:
#         # MongoDB empty → only sync latest
#         if date_codes:
#             date_codes = [date_codes[-1]]
#             print(f"\n⚠️  MongoDB collection is empty. Only syncing latest R2 date: {date_codes[0]}")

#     print("\n🧠 Smart filtering (new + updated only)...")

#     to_sync = []
#     skipped = 0

#     for date_code in date_codes:
#         r2_key = f"advance/{date_code}/finalsummary.json"

#         if not self.r2_key_exists(r2_key):
#             print(f"⚠️  No finalsummary.json for {date_code}, skipping")
#             continue

#         data = r2_download_json(r2_key)
#         if not data:
#             continue

#         file_last_updated = data.get("last_updated", "")
#         doc_id = f"advance_{date_code}"

#         existing = collection.find_one({"_id": doc_id}, {"last_updated": 1})

#         if not existing or existing.get("last_updated") != file_last_updated:
#             to_sync.append({
#                 "r2_key": r2_key,
#                 "date_code": date_code,
#                 "data": data
#             })
#         else:
#             skipped += 1

#     print(f"🆕 To sync: {len(to_sync)}")
#     print(f"⏭  Skipped (unchanged): {skipped}")

#     if not to_sync:
#         print("✅ Everything already up-to-date")
#         return True

#     success_count = 0
#     fail_count = 0

#     for item in to_sync:
#         print(f"\n📅 Processing: {item['date_code']}")

#         movies = item["data"].get("movies", {})
#         if not movies:
#             fail_count += 1
#             continue

#         last_updated = item["data"].get("last_updated", "")
#         date_code = item["date_code"]

#         movies_array = [
#             {"movie": name, **mdata}
#             for name, mdata in movies.items()
#         ]

#         doc = {
#             "_id": f"advance_{date_code}",
#             "show_date": date_code,
#             "last_updated": last_updated,
#             "synced_at": datetime.now(IST),
#             "total_movies": len(movies),
#             "movies": movies_array
#         }

#         try:
#             result = collection.replace_one(
#                 {"_id": doc["_id"]}, doc, upsert=True
#             )
#             action = "Inserted" if result.upserted_id else "Updated"
#             print(f"   ✅ {action}: advance_{date_code} ({len(movies)} movies)")
#             success_count += 1
#         except Exception as e:
#             print(f"   ❌ Error: {e}")
#             fail_count += 1

#     print("\n" + "=" * 60)
#     print(f"✅ Success: {success_count} | ❌ Failed: {fail_count} | ⏭ Skipped: {skipped}")
#     print("=" * 60)

#     return success_count > 0

#     def close(self):
#         self.client.close()
#         print("\n👋 MongoDB connection closed")


# def main():
#     print("🚀 Starting Smart MongoDB Advance Sync (R2)...")
#     print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

#     syncer = MongoDBAdvanceSync()
#     try:
#         success = syncer.sync_all()
#         print("\n✅ SYNC COMPLETED" if success else "\n❌ SYNC FAILED")
#         sys.exit(0 if success else 1)
#     except Exception as e:
#         print(f"\n❌ Fatal error: {e}")
#         import traceback
#         traceback.print_exc()
#         sys.exit(1)
#     finally:
#         syncer.close()


# if __name__ == "__main__":
#     main()



# mongodb/sync_advance.py — R2 VERSION

import json
import os
import sys
from datetime import datetime
from pymongo import MongoClient
import pytz

from config import MONGODB_URI, DATABASE_NAME, COLLECTION_ADVANCE, IST
from r2_client import r2_download_json, get_r2, BUCKET


class MongoDBAdvanceSync:
    def __init__(self):
        if not MONGODB_URI:
            raise ValueError("MONGODB_URI environment variable not set")
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]
        print(f"✅ Connected to MongoDB: {DATABASE_NAME}")

    def find_all_advance_dates(self):
        """List all advance date folders in R2."""
        print("\n🔍 Scanning R2 for advance summary files...")
        r2 = get_r2()
        try:
            paginator = r2.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=BUCKET,
                Prefix="advance/",
                Delimiter="/"
            )

            date_codes = []
            for page in pages:
                for prefix in page.get("CommonPrefixes", []):
                    folder = prefix["Prefix"]
                    date_part = folder.rstrip("/").split("/")[-1]

                    if date_part.isdigit() and len(date_part) == 8:
                        date_codes.append(date_part)

            date_codes.sort()
            print(f"📊 Total advance dates found in R2: {len(date_codes)}")
            return date_codes

        except Exception as e:
            print(f"❌ R2 listing error: {e}")
            return []

    def r2_key_exists(self, key):
        r2 = get_r2()
        try:
            r2.head_object(Bucket=BUCKET, Key=key)
            return True
        except Exception:
            return False

    def sync_single_summary(self, r2_key, date_code):
        data = r2_download_json(r2_key)
        if not data:
            return False

        movies = data.get("movies", {})
        if not movies:
            print(f"⚠️  No movies in {r2_key}")
            return False

        last_updated = data.get("last_updated", "")
        timestamp = datetime.now(IST)
        collection = self.db[COLLECTION_ADVANCE]

        movies_array = [
            {"movie": name, **mdata}
            for name, mdata in movies.items()
        ]

        doc = {
            "_id": f"advance_{date_code}",
            "show_date": date_code,
            "last_updated": last_updated,
            "synced_at": timestamp,
            "total_movies": len(movies),
            "movies": movies_array
        }

        try:
            result = collection.replace_one(
                {"_id": doc["_id"]},
                doc,
                upsert=True
            )
            action = "Inserted" if result.upserted_id else "Updated"
            print(f"   ✅ {action}: advance_{date_code} ({len(movies)} movies)")
            return True
        except Exception as e:
            print(f"   ❌ Error syncing {date_code}: {e}")
            return False

    def sync_all(self):
        date_codes = self.find_all_advance_dates()
        if not date_codes:
            print("\n❌ No advance dates found in R2")
            return False

        collection = self.db[COLLECTION_ADVANCE]

        # 🔥 Smart cutoff logic
        existing_dates = collection.distinct("show_date")
        if existing_dates:
            min_existing = min(existing_dates)
            print(f"\n📅 Oldest date in MongoDB: {min_existing} — skipping older R2 data")
            date_codes = [d for d in date_codes if d >= min_existing]
        else:
            if date_codes:
                date_codes = [date_codes[-1]]
                print(f"\n⚠️ MongoDB empty → syncing only latest date: {date_codes[0]}")

        print("\n🧠 Smart filtering (new + updated only)...")

        to_sync = []
        skipped = 0

        for date_code in date_codes:
            r2_key = f"advance/{date_code}/finalsummary.json"

            if not self.r2_key_exists(r2_key):
                print(f"⚠️ No file for {date_code}, skipping")
                continue

            data = r2_download_json(r2_key)
            if not data:
                continue

            file_last_updated = data.get("last_updated", "")
            doc_id = f"advance_{date_code}"

            existing = collection.find_one(
                {"_id": doc_id},
                {"last_updated": 1}
            )

            if not existing or existing.get("last_updated") != file_last_updated:
                to_sync.append({
                    "r2_key": r2_key,
                    "date_code": date_code,
                    "data": data
                })
            else:
                skipped += 1

        print(f"🆕 To sync: {len(to_sync)}")
        print(f"⏭ Skipped: {skipped}")

        if not to_sync:
            print("✅ Already up-to-date")
            return True

        success_count = 0
        fail_count = 0

        for item in to_sync:
            print(f"\n📅 Processing: {item['date_code']}")

            movies = item["data"].get("movies", {})
            if not movies:
                fail_count += 1
                continue

            last_updated = item["data"].get("last_updated", "")
            date_code = item["date_code"]

            movies_array = [
                {"movie": name, **mdata}
                for name, mdata in movies.items()
            ]

            doc = {
                "_id": f"advance_{date_code}",
                "show_date": date_code,
                "last_updated": last_updated,
                "synced_at": datetime.now(IST),
                "total_movies": len(movies),
                "movies": movies_array
            }

            try:
                result = collection.replace_one(
                    {"_id": doc["_id"]},
                    doc,
                    upsert=True
                )
                action = "Inserted" if result.upserted_id else "Updated"
                print(f"   ✅ {action}: advance_{date_code}")
                success_count += 1
            except Exception as e:
                print(f"   ❌ Error: {e}")
                fail_count += 1

        print("\n" + "=" * 60)
        print(f"✅ Success: {success_count} | ❌ Failed: {fail_count} | ⏭ Skipped: {skipped}")
        print("=" * 60)

        return success_count > 0

    def close(self):
        self.client.close()
        print("\n👋 MongoDB connection closed")


def main():
    print("🚀 Starting Smart MongoDB Advance Sync (R2)...")
    print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

    syncer = MongoDBAdvanceSync()
    try:
        success = syncer.sync_all()
        print("\n✅ SYNC COMPLETED" if success else "\n❌ SYNC FAILED")
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        syncer.close()


if __name__ == "__main__":
    main()
