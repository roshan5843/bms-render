# import json
# import os
# import sys
# from datetime import datetime
# from pathlib import Path
# from pymongo import MongoClient
# from pymongo.errors import DuplicateKeyError
# import pytz
#
# from config import (
#     MONGODB_URI,
#     DATABASE_NAME,
#     COLLECTION_SUMMARY,
#     IST
# )
#
#
# class MongoDBSync:
#     def __init__(self):
#         if not MONGODB_URI:
#             raise ValueError("MONGODB_URI environment variable not set")
#
#         self.client = MongoClient(MONGODB_URI)
#         self.db = self.client[DATABASE_NAME]
#
#         print(f"✅ Connected to MongoDB: {DATABASE_NAME}")
#
#     def find_latest_summary(self):
#         """Find the most recent summary file"""
#         print("\n🔍 Searching for latest summary file...")
#
#         base_dir = Path("daily/data")
#
#         if not base_dir.exists():
#             print(f"❌ Directory not found: {base_dir}")
#             return None
#
#         # Get all date directories, sorted newest first
#         date_dirs = sorted(
#             [d for d in base_dir.iterdir() if d.is_dir() and d.name.isdigit()],
#             key=lambda x: x.name,
#             reverse=True
#         )
#
#         if not date_dirs:
#             print("❌ No date directories found in daily/data/")
#             return None
#
#         # Search for summary file in newest directories first
#         for date_dir in date_dirs[:3]:  # Check last 3 days
#             summary_path = date_dir / "finalsummary.json"
#
#             if summary_path.exists():
#                 print(f"✅ Found summary in: {date_dir.name}")
#                 print(f"   • Summary: {summary_path}")
#                 return str(summary_path)
#
#         print("❌ No finalsummary.json found in recent directories")
#         return None
#
#     def load_json(self, filepath):
#         """Load JSON file safely"""
#         try:
#             with open(filepath, 'r', encoding='utf-8') as f:
#                 data = json.load(f)
#                 print(
#                     f"✅ Loaded: {filepath} ({os.path.getsize(filepath) / 1024:.1f} KB)")
#                 return data
#         except FileNotFoundError:
#             print(f"❌ File not found: {filepath}")
#             return None
#         except json.JSONDecodeError as e:
#             print(f"❌ Invalid JSON in {filepath}: {e}")
#             return None
#
#     def extract_date_from_data(self, data):
#         """Extract date from data or filename"""
#         # Try last_updated first
#         last_updated = data.get("last_updated", "")
#         if last_updated:
#             try:
#                 # Parse "2025-01-01 22:16 IST" format
#                 date_str = last_updated.split()[0]
#                 return date_str.replace("-", "")
#             except:
#                 pass
#
#         # Fallback to current date
#         return datetime.now(IST).strftime("%Y%m%d")
#
#     def sync_summary(self, filepath):
#         """Sync summary data as nested document structure"""
#         print(f"\n📊 Syncing summary from: {filepath}")
#
#         data = self.load_json(filepath)
#         if not data:
#             return False
#
#         movies = data.get("movies", {})
#         if not movies:
#             print("⚠️  No movies found in summary")
#             return False
#
#         date_code = self.extract_date_from_data(data)
#         last_updated = data.get("last_updated")
#         timestamp = datetime.now(IST)
#
#         collection = self.db[COLLECTION_SUMMARY]
#
#         print(f"📅 Date code: {date_code}")
#         print(f"📁 Collection: {COLLECTION_SUMMARY}")
#         print(f"🎬 Movies found: {len(movies)}")
#
#         # Convert movies dict to array format
#         movies_array = []
#         for movie_name, movie_data in movies.items():
#             movies_array.append({
#                 "movie": movie_name,
#                 **movie_data
#             })
#
#         # Create document structure
#         doc = {
#             "_id": f"summary_{date_code}",  # e.g., "summary_20250101"
#             "date": date_code,
#             "last_updated": last_updated,
#             "synced_at": timestamp,
#             "total_movies": len(movies),
#             "movies": movies_array
#         }
#
#         # Upsert the entire day's data
#         try:
#             result = collection.replace_one(
#                 {"_id": f"summary_{date_code}"},
#                 doc,
#                 upsert=True
#             )
#
#             if result.upserted_id:
#                 print(f"✅ Summary inserted:")
#                 print(f"   • Document ID: summary_{date_code}")
#                 print(f"   • Total movies: {len(movies)}")
#             else:
#                 print(f"✅ Summary updated:")
#                 print(f"   • Document ID: summary_{date_code}")
#                 print(f"   • Total movies: {len(movies)}")
#                 print(f"   • Modified: {result.modified_count}")
#
#             return True
#
#         except Exception as e:
#             print(f"❌ Sync error: {e}")
#             return False
#
#     def create_indexes(self):
#         """Create indexes for better query performance"""
#         print("\n🔍 Creating indexes...")
#
#         try:
#             collection = self.db[COLLECTION_SUMMARY]
#
#             # Date index
#             collection.create_index([("date", -1)])
#
#             # Movie name index (for searching within movies array)
#             collection.create_index([("movies.movie", 1)])
#
#             # Gross index (for top grossing queries)
#             collection.create_index([("movies.gross", -1)])
#
#             # Occupancy index
#             collection.create_index([("movies.occupancy", -1)])
#
#             print(f"✅ Indexes created for {COLLECTION_SUMMARY}")
#         except Exception as e:
#             print(f"⚠️  Index creation warning: {e}")
#
#     def sync_all(self):
#         """Sync summary data only"""
#         # Find latest summary file
#         summary_path = self.find_latest_summary()
#
#         if not summary_path:
#             print("\n❌ Could not find summary file to sync")
#             return False
#
#         # Sync summary
#         success = self.sync_summary(summary_path)
#
#         if not success:
#             print("\n❌ Summary sync failed")
#             return False
#
#         # Create indexes
#         self.create_indexes()
#
#         return True
#
#     def close(self):
#         """Close MongoDB connection"""
#         self.client.close()
#         print("\n👋 MongoDB connection closed")
#
#
# def main():
#     print("🚀 Starting MongoDB sync...")
#     print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
#
#     syncer = MongoDBSync()
#
#     try:
#         success = syncer.sync_all()
#
#         if success:
#             print("\n" + "="*50)
#             print("✅ SUMMARY SYNC COMPLETED SUCCESSFULLY")
#             print("="*50)
#             sys.exit(0)
#         else:
#             print("\n" + "="*50)
#             print("❌ SYNC FAILED")
#             print("="*50)
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


# sync.py — R2 VERSION
import json
import os
import sys
from datetime import datetime
from pymongo import MongoClient
import pytz

from config import MONGODB_URI, DATABASE_NAME, COLLECTION_SUMMARY, IST
from r2_client import r2_download_json, get_r2, BUCKET


class MongoDBSync:
    def __init__(self):
        if not MONGODB_URI:
            raise ValueError("MONGODB_URI environment variable not set")
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]
        print(f"✅ Connected to MongoDB: {DATABASE_NAME}")

    def find_latest_summary(self):
        """Find the most recent finalsummary.json in R2."""
        print("\n🔍 Searching for latest summary in R2...")

        r2 = get_r2()

        # List all objects under daily/ prefix
        try:
            paginator = r2.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=BUCKET, Prefix="daily/", Delimiter="/")

            date_prefixes = []
            for page in pages:
                for prefix in page.get("CommonPrefixes", []):
                    # prefix looks like "daily/20250501/"
                    folder = prefix["Prefix"]
                    date_part = folder.rstrip("/").split("/")[-1]
                    if date_part.isdigit() and len(date_part) == 8:
                        date_prefixes.append(date_part)

            if not date_prefixes:
                print("❌ No date folders found in R2")
                return None

            date_prefixes.sort(reverse=True)

            # Find the latest date that has a finalsummary.json
            for date_code in date_prefixes[:5]:
                key = f"daily/{date_code}/finalsummary.json"
                try:
                    r2.head_object(Bucket=BUCKET, Key=key)
                    print(f"✅ Found: r2://{BUCKET}/{key}")
                    return key
                except:
                    continue

            print("❌ No finalsummary.json found in recent R2 dates")
            return None

        except Exception as e:
            print(f"❌ R2 listing error: {e}")
            return None

    def sync_summary(self, r2_key):
        """Sync summary data from R2 key into MongoDB."""
        print(f"\n📊 Syncing summary from R2: {r2_key}")

        data = r2_download_json(r2_key)
        if not data:
            return False

        movies = data.get("movies", {})
        if not movies:
            print("⚠️  No movies found in summary")
            return False

        # Extract date from key path: daily/20250501/finalsummary.json
        date_code = r2_key.split("/")[1]
        last_updated = data.get("last_updated")
        timestamp = datetime.now(IST)

        collection = self.db[COLLECTION_SUMMARY]

        print(f"📅 Date code: {date_code}")
        print(f"🎬 Movies found: {len(movies)}")

        movies_array = [{"movie": name, **mdata}
                        for name, mdata in movies.items()]

        doc = {
            "_id": f"summary_{date_code}",
            "date": date_code,
            "last_updated": last_updated,
            "synced_at": timestamp,
            "total_movies": len(movies),
            "movies": movies_array
        }

        try:
            result = collection.replace_one(
                {"_id": f"summary_{date_code}"}, doc, upsert=True)
            action = "inserted" if result.upserted_id else "updated"
            print(
                f"✅ Summary {action}: summary_{date_code} ({len(movies)} movies)")
            return True
        except Exception as e:
            print(f"❌ Sync error: {e}")
            return False

    def create_indexes(self):
        print("\n🔍 Creating indexes...")
        try:
            col = self.db[COLLECTION_SUMMARY]
            col.create_index([("date", -1)])
            col.create_index([("movies.movie", 1)])
            col.create_index([("movies.gross", -1)])
            col.create_index([("movies.occupancy", -1)])
            print(f"✅ Indexes created")
        except Exception as e:
            print(f"⚠️  Index warning: {e}")

    def sync_all(self):
        r2_key = self.find_latest_summary()
        if not r2_key:
            print("\n❌ Could not find summary file in R2")
            return False
        success = self.sync_summary(r2_key)
        if success:
            self.create_indexes()
        return success

    def close(self):
        self.client.close()
        print("\n👋 MongoDB connection closed")


def main():
    print("🚀 Starting MongoDB sync...")
    print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

    syncer = MongoDBSync()
    try:
        success = syncer.sync_all()
        print("\n" + "="*50)
        print("✅ SYNC COMPLETED" if success else "❌ SYNC FAILED")
        print("="*50)
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
