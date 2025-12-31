import json
import os
import sys
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import pytz

from config import (
    MONGODB_URI,
    DATABASE_NAME,
    COLLECTION_SUMMARY,
    COLLECTION_DETAILED,
    IST,
    get_file_paths,
    get_date_code
)


class MongoDBSync:
    def __init__(self):
        if not MONGODB_URI:
            raise ValueError("MONGODB_URI environment variable not set")

        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]
        self.date_code = get_date_code()

        print(f"✅ Connected to MongoDB: {DATABASE_NAME}")
        print(f"📅 Date: {self.date_code}")

    def load_json(self, filepath):
        """Load JSON file safely"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ File not found: {filepath}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in {filepath}: {e}")
            return None

    def sync_summary(self, filepath):
        """Sync summary data with bulk upsert"""
        print(f"\n📊 Syncing summary from: {filepath}")

        data = self.load_json(filepath)
        if not data:
            return False

        movies = data.get("movies", {})
        if not movies:
            print("⚠️  No movies found in summary")
            return False

        last_updated = data.get("last_updated")
        timestamp = datetime.now(IST)

        # Prepare bulk operations
        operations = []

        for movie_name, movie_data in movies.items():
            # Add metadata
            doc = {
                **movie_data,
                "movie": movie_name,
                "date": self.date_code,
                "last_updated": last_updated,
                "synced_at": timestamp
            }

            # Upsert operation
            operations.append(
                UpdateOne(
                    {"movie": movie_name, "date": self.date_code},
                    {"$set": doc},
                    upsert=True
                )
            )

        # Execute bulk write
        try:
            result = self.db[COLLECTION_SUMMARY].bulk_write(
                operations,
                ordered=False
            )

            print(f"✅ Summary sync complete:")
            print(f"   • Matched: {result.matched_count}")
            print(f"   • Modified: {result.modified_count}")
            print(f"   • Upserted: {result.upserted_count}")
            print(f"   • Total movies: {len(movies)}")

            return True

        except BulkWriteError as e:
            print(
                f"⚠️  Bulk write errors: {len(e.details.get('writeErrors', []))}")
            print(
                f"✅ Successfully processed: {e.details.get('nInserted', 0) + e.details.get('nModified', 0)}")
            return True

    def sync_detailed(self, filepath):
        """Sync detailed data with bulk upsert"""
        print(f"\n📋 Syncing detailed from: {filepath}")

        data = self.load_json(filepath)
        if not data:
            return False

        shows = data.get("data", [])
        if not shows:
            print("⚠️  No shows found in detailed")
            return False

        last_updated = data.get("last_updated")
        timestamp = datetime.now(IST)

        # Prepare bulk operations
        operations = []

        for show in shows:
            # Create unique key
            show_key = f"{show.get('venue')}_{show.get('time')}_{show.get('session_id')}_{show.get('audi')}"

            # Add metadata
            doc = {
                **show,
                "_key": show_key,
                "date": self.date_code,
                "last_updated": last_updated,
                "synced_at": timestamp
            }

            # Upsert operation
            operations.append(
                UpdateOne(
                    {"_key": show_key, "date": self.date_code},
                    {"$set": doc},
                    upsert=True
                )
            )

        # Batch process for large datasets (split into chunks of 1000)
        batch_size = 1000
        total_matched = 0
        total_modified = 0
        total_upserted = 0

        for i in range(0, len(operations), batch_size):
            batch = operations[i:i + batch_size]

            try:
                result = self.db[COLLECTION_DETAILED].bulk_write(
                    batch,
                    ordered=False
                )

                total_matched += result.matched_count
                total_modified += result.modified_count
                total_upserted += result.upserted_count

                print(f"   Batch {i//batch_size + 1}: {len(batch)} operations")

            except BulkWriteError as e:
                print(
                    f"⚠️  Batch {i//batch_size + 1} errors: {len(e.details.get('writeErrors', []))}")
                total_modified += e.details.get('nModified', 0)
                total_upserted += e.details.get('nInserted', 0)

        print(f"✅ Detailed sync complete:")
        print(f"   • Matched: {total_matched}")
        print(f"   • Modified: {total_modified}")
        print(f"   • Upserted: {total_upserted}")
        print(f"   • Total shows: {len(shows)}")

        return True

    def create_indexes(self):
        """Create indexes for better query performance"""
        print("\n🔍 Creating indexes...")

        # Summary indexes
        self.db[COLLECTION_SUMMARY].create_index(
            [("movie", 1), ("date", -1)], unique=True)
        self.db[COLLECTION_SUMMARY].create_index([("date", -1)])
        self.db[COLLECTION_SUMMARY].create_index([("gross", -1)])
        self.db[COLLECTION_SUMMARY].create_index([("occupancy", -1)])

        # Detailed indexes
        self.db[COLLECTION_DETAILED].create_index(
            [("_key", 1), ("date", -1)], unique=True)
        self.db[COLLECTION_DETAILED].create_index([("date", -1)])
        self.db[COLLECTION_DETAILED].create_index([("movie", 1)])
        self.db[COLLECTION_DETAILED].create_index([("venue", 1)])
        self.db[COLLECTION_DETAILED].create_index([("city", 1)])
        self.db[COLLECTION_DETAILED].create_index([("time", 1)])

        print("✅ Indexes created")

    def sync_all(self):
        """Sync both summary and detailed data"""
        paths = get_file_paths()

        success = True

        # Sync summary
        if os.path.exists(paths["summary"]):
            success &= self.sync_summary(paths["summary"])
        else:
            print(f"⚠️  Summary file not found: {paths['summary']}")
            success = False

        # Sync detailed
        if os.path.exists(paths["detailed"]):
            success &= self.sync_detailed(paths["detailed"])
        else:
            print(f"⚠️  Detailed file not found: {paths['detailed']}")
            success = False

        # Create indexes on first run
        self.create_indexes()

        return success

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
            print("\n✅ All syncs completed successfully")
            sys.exit(0)
        else:
            print("\n⚠️  Some syncs failed")
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
