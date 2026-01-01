# import json
# import os
# import sys
# from datetime import datetime
# from pathlib import Path
# from pymongo import MongoClient, UpdateOne
# from pymongo.errors import BulkWriteError
# import pytz
#
# from config import (
#     MONGODB_URI,
#     DATABASE_NAME,
#     COLLECTION_SUMMARY,
#     COLLECTION_DETAILED,
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
#     def find_latest_files(self):
#         """Find the most recent data files"""
#         print("\n🔍 Searching for latest data files...")
#
#         base_dir = Path("daily/data")
#
#         if not base_dir.exists():
#             print(f"❌ Directory not found: {base_dir}")
#             return None, None
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
#             return None, None
#
#         # Search for files in newest directories first
#         for date_dir in date_dirs[:3]:  # Check last 3 days
#             summary_path = date_dir / "finalsummary.json"
#             detailed_path = date_dir / "finaldetailed.json"
#
#             if summary_path.exists() and detailed_path.exists():
#                 print(f"✅ Found files in: {date_dir.name}")
#                 print(f"   • Summary: {summary_path}")
#                 print(f"   • Detailed: {detailed_path}")
#                 return str(summary_path), str(detailed_path)
#
#         print("❌ No finalsummary.json or finaldetailed.json found in recent directories")
#         return None, None
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
#                 # Parse "2025-12-31 22:16 IST" format
#                 date_str = last_updated.split()[0]
#                 return date_str.replace("-", "")
#             except:
#                 pass
#
#         # Fallback to current date
#         return datetime.now(IST).strftime("%Y%m%d")
#
#     def sync_summary(self, filepath):
#         """Sync summary data with bulk upsert"""
#         print(f"\n📊 Syncing summary from: {filepath}")
#
#         data = self.load_json(filepath)
#         if not data:
#             return False, None
#
#         movies = data.get("movies", {})
#         if not movies:
#             print("⚠️  No movies found in summary")
#             return False, None
#
#         date_code = self.extract_date_from_data(data)
#         last_updated = data.get("last_updated")
#         timestamp = datetime.now(IST)
#
#         print(f"📅 Date code: {date_code}")
#         print(f"🎬 Movies found: {len(movies)}")
#
#         # Prepare bulk operations
#         operations = []
#
#         for movie_name, movie_data in movies.items():
#             # Add metadata
#             doc = {
#                 **movie_data,
#                 "movie": movie_name,
#                 "date": date_code,
#                 "last_updated": last_updated,
#                 "synced_at": timestamp
#             }
#
#             # Upsert operation
#             operations.append(
#                 UpdateOne(
#                     {"movie": movie_name, "date": date_code},
#                     {"$set": doc},
#                     upsert=True
#                 )
#             )
#
#         # Execute bulk write
#         try:
#             result = self.db[COLLECTION_SUMMARY].bulk_write(
#                 operations,
#                 ordered=False
#             )
#
#             print(f"✅ Summary sync complete:")
#             print(f"   • Matched: {result.matched_count}")
#             print(f"   • Modified: {result.modified_count}")
#             print(f"   • Upserted: {result.upserted_count}")
#             print(f"   • Total movies: {len(movies)}")
#
#             return True, date_code
#
#         except BulkWriteError as e:
#             print(
#                 f"⚠️  Bulk write errors: {len(e.details.get('writeErrors', []))}")
#             processed = e.details.get(
#                 'nInserted', 0) + e.details.get('nModified', 0)
#             print(f"✅ Successfully processed: {processed}")
#             return True, date_code
#
#     def sync_detailed(self, filepath, date_code=None):
#         """Sync detailed data with bulk upsert"""
#         print(f"\n📋 Syncing detailed from: {filepath}")
#
#         data = self.load_json(filepath)
#         if not data:
#             return False
#
#         shows = data.get("data", [])
#         if not shows:
#             print("⚠️  No shows found in detailed")
#             return False
#
#         if not date_code:
#             date_code = self.extract_date_from_data(data)
#
#         last_updated = data.get("last_updated")
#         timestamp = datetime.now(IST)
#
#         print(f"📅 Date code: {date_code}")
#         print(f"🎫 Shows found: {len(shows)}")
#
#         # Prepare bulk operations
#         operations = []
#
#         for show in shows:
#             # Create unique key
#             show_key = f"{show.get('venue', '')}_{show.get('time', '')}_{show.get('session_id', '')}_{show.get('audi', '')}"
#
#             # Add metadata
#             doc = {
#                 **show,
#                 "_key": show_key,
#                 "date": date_code,
#                 "last_updated": last_updated,
#                 "synced_at": timestamp
#             }
#
#             # Upsert operation
#             operations.append(
#                 UpdateOne(
#                     {"_key": show_key, "date": date_code},
#                     {"$set": doc},
#                     upsert=True
#                 )
#             )
#
#         # Batch process for large datasets
#         batch_size = 1000
#         total_matched = 0
#         total_modified = 0
#         total_upserted = 0
#
#         num_batches = (len(operations) + batch_size - 1) // batch_size
#         print(f"📦 Processing {num_batches} batches...")
#
#         for i in range(0, len(operations), batch_size):
#             batch = operations[i:i + batch_size]
#             batch_num = i // batch_size + 1
#
#             try:
#                 result = self.db[COLLECTION_DETAILED].bulk_write(
#                     batch,
#                     ordered=False
#                 )
#
#                 total_matched += result.matched_count
#                 total_modified += result.modified_count
#                 total_upserted += result.upserted_count
#
#                 print(
#                     f"   ✓ Batch {batch_num}/{num_batches}: {len(batch)} operations")
#
#             except BulkWriteError as e:
#                 errors = len(e.details.get('writeErrors', []))
#                 print(
#                     f"   ⚠️ Batch {batch_num}/{num_batches}: {errors} errors")
#                 total_modified += e.details.get('nModified', 0)
#                 total_upserted += e.details.get('nInserted', 0)
#
#         print(f"✅ Detailed sync complete:")
#         print(f"   • Matched: {total_matched}")
#         print(f"   • Modified: {total_modified}")
#         print(f"   • Upserted: {total_upserted}")
#         print(f"   • Total shows: {len(shows)}")
#
#         return True
#
#     def create_indexes(self):
#         """Create indexes for better query performance"""
#         print("\n🔍 Creating indexes...")
#
#         try:
#             # Summary indexes
#             self.db[COLLECTION_SUMMARY].create_index(
#                 [("movie", 1), ("date", -1)], unique=True)
#             self.db[COLLECTION_SUMMARY].create_index([("date", -1)])
#             self.db[COLLECTION_SUMMARY].create_index([("gross", -1)])
#             self.db[COLLECTION_SUMMARY].create_index([("occupancy", -1)])
#
#             # Detailed indexes
#             self.db[COLLECTION_DETAILED].create_index(
#                 [("_key", 1), ("date", -1)], unique=True)
#             self.db[COLLECTION_DETAILED].create_index([("date", -1)])
#             self.db[COLLECTION_DETAILED].create_index([("movie", 1)])
#             self.db[COLLECTION_DETAILED].create_index([("venue", 1)])
#             self.db[COLLECTION_DETAILED].create_index([("city", 1)])
#             self.db[COLLECTION_DETAILED].create_index([("time", 1)])
#
#             print("✅ Indexes created")
#         except Exception as e:
#             print(f"⚠️  Index creation warning: {e}")
#
#     def sync_all(self):
#         """Sync both summary and detailed data"""
#         # Find latest files
#         summary_path, detailed_path = self.find_latest_files()
#
#         if not summary_path or not detailed_path:
#             print("\n❌ Could not find data files to sync")
#             return False
#
#         # Sync summary first (to get date_code)
#         success_summary, date_code = self.sync_summary(summary_path)
#
#         if not success_summary:
#             print("\n❌ Summary sync failed")
#             return False
#
#         # Sync detailed
#         success_detailed = self.sync_detailed(detailed_path, date_code)
#
#         if not success_detailed:
#             print("\n⚠️  Detailed sync failed")
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
#             print("✅ ALL SYNCS COMPLETED SUCCESSFULLY")
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


import json
import os
import sys
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pytz

from config import (
    MONGODB_URI,
    DATABASE_NAME,
    COLLECTION_SUMMARY,
    IST
)


class MongoDBSync:
    def __init__(self):
        if not MONGODB_URI:
            raise ValueError("MONGODB_URI environment variable not set")

        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]

        print(f"✅ Connected to MongoDB: {DATABASE_NAME}")

    def find_latest_summary(self):
        """Find the most recent summary file"""
        print("\n🔍 Searching for latest summary file...")

        base_dir = Path("daily/data")

        if not base_dir.exists():
            print(f"❌ Directory not found: {base_dir}")
            return None

        # Get all date directories, sorted newest first
        date_dirs = sorted(
            [d for d in base_dir.iterdir() if d.is_dir() and d.name.isdigit()],
            key=lambda x: x.name,
            reverse=True
        )

        if not date_dirs:
            print("❌ No date directories found in daily/data/")
            return None

        # Search for summary file in newest directories first
        for date_dir in date_dirs[:3]:  # Check last 3 days
            summary_path = date_dir / "finalsummary.json"

            if summary_path.exists():
                print(f"✅ Found summary in: {date_dir.name}")
                print(f"   • Summary: {summary_path}")
                return str(summary_path)

        print("❌ No finalsummary.json found in recent directories")
        return None

    def load_json(self, filepath):
        """Load JSON file safely"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(
                    f"✅ Loaded: {filepath} ({os.path.getsize(filepath) / 1024:.1f} KB)")
                return data
        except FileNotFoundError:
            print(f"❌ File not found: {filepath}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in {filepath}: {e}")
            return None

    def extract_date_from_data(self, data):
        """Extract date from data or filename"""
        # Try last_updated first
        last_updated = data.get("last_updated", "")
        if last_updated:
            try:
                # Parse "2025-01-01 22:16 IST" format
                date_str = last_updated.split()[0]
                return date_str.replace("-", "")
            except:
                pass

        # Fallback to current date
        return datetime.now(IST).strftime("%Y%m%d")

    def sync_summary(self, filepath):
        """Sync summary data as nested document structure"""
        print(f"\n📊 Syncing summary from: {filepath}")

        data = self.load_json(filepath)
        if not data:
            return False

        movies = data.get("movies", {})
        if not movies:
            print("⚠️  No movies found in summary")
            return False

        date_code = self.extract_date_from_data(data)
        last_updated = data.get("last_updated")
        timestamp = datetime.now(IST)

        collection = self.db[COLLECTION_SUMMARY]

        print(f"📅 Date code: {date_code}")
        print(f"📁 Collection: {COLLECTION_SUMMARY}")
        print(f"🎬 Movies found: {len(movies)}")

        # Convert movies dict to array format
        movies_array = []
        for movie_name, movie_data in movies.items():
            movies_array.append({
                "movie": movie_name,
                **movie_data
            })

        # Create document structure
        doc = {
            "_id": f"summary_{date_code}",  # e.g., "summary_20250101"
            "date": date_code,
            "last_updated": last_updated,
            "synced_at": timestamp,
            "total_movies": len(movies),
            "movies": movies_array
        }

        # Upsert the entire day's data
        try:
            result = collection.replace_one(
                {"_id": f"summary_{date_code}"},
                doc,
                upsert=True
            )

            if result.upserted_id:
                print(f"✅ Summary inserted:")
                print(f"   • Document ID: summary_{date_code}")
                print(f"   • Total movies: {len(movies)}")
            else:
                print(f"✅ Summary updated:")
                print(f"   • Document ID: summary_{date_code}")
                print(f"   • Total movies: {len(movies)}")
                print(f"   • Modified: {result.modified_count}")

            return True

        except Exception as e:
            print(f"❌ Sync error: {e}")
            return False

    def create_indexes(self):
        """Create indexes for better query performance"""
        print("\n🔍 Creating indexes...")

        try:
            collection = self.db[COLLECTION_SUMMARY]

            # Date index
            collection.create_index([("date", -1)])

            # Movie name index (for searching within movies array)
            collection.create_index([("movies.movie", 1)])

            # Gross index (for top grossing queries)
            collection.create_index([("movies.gross", -1)])

            # Occupancy index
            collection.create_index([("movies.occupancy", -1)])

            print(f"✅ Indexes created for {COLLECTION_SUMMARY}")
        except Exception as e:
            print(f"⚠️  Index creation warning: {e}")

    def sync_all(self):
        """Sync summary data only"""
        # Find latest summary file
        summary_path = self.find_latest_summary()

        if not summary_path:
            print("\n❌ Could not find summary file to sync")
            return False

        # Sync summary
        success = self.sync_summary(summary_path)

        if not success:
            print("\n❌ Summary sync failed")
            return False

        # Create indexes
        self.create_indexes()

        return True

    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        print("\n👋 MongoDB connection closed")


def main():
    print("🚀 Starting MongoDB sync...")
    print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

    syncer = MongoDBSync()

    try:
        success = syncer.sync_all()

        if success:
            print("\n" + "="*50)
            print("✅ SUMMARY SYNC COMPLETED SUCCESSFULLY")
            print("="*50)
            sys.exit(0)
        else:
            print("\n" + "="*50)
            print("❌ SYNC FAILED")
            print("="*50)
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        syncer.close()


if __name__ == "__main__":
    main()
