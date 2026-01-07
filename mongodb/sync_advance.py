import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pytz

from config import (
    MONGODB_URI,
    DATABASE_NAME,
    COLLECTION_ADVANCE,
    IST
)


class MongoDBAdvanceSync:
    def __init__(self):
        if not MONGODB_URI:
            raise ValueError("MONGODB_URI environment variable not set")

        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]

        print(f"✅ Connected to MongoDB: {DATABASE_NAME}")

    def find_all_advance_summaries(self):
        """Find ALL advance booking summary files (multiple dates)"""
        print("\n🔍 Searching for all advance summary files...")

        base_dir = Path("advance/data")

        if not base_dir.exists():
            print(f"❌ Directory not found: {base_dir}")
            return []

        # Get all date directories
        date_dirs = sorted(
            [d for d in base_dir.iterdir() if d.is_dir() and d.name.isdigit()],
            key=lambda x: x.name,
            reverse=False  # Oldest to newest
        )

        if not date_dirs:
            print("❌ No date directories found in advance/data/")
            return []

        # Find all finalsummary.json files
        summary_files = []
        for date_dir in date_dirs:
            summary_path = date_dir / "finalsummary.json"

            if summary_path.exists():
                print(f"✅ Found summary: {date_dir.name}")
                summary_files.append({
                    'path': str(summary_path),
                    'date': date_dir.name
                })

        print(f"📊 Total summaries found: {len(summary_files)}")
        return summary_files

    def load_json(self, filepath):
        """Load JSON file safely"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except FileNotFoundError:
            print(f"❌ File not found: {filepath}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in {filepath}: {e}")
            return None

    def sync_single_summary(self, filepath, date_code):
        """Sync a single advance booking summary"""
        data = self.load_json(filepath)
        if not data:
            return False

        movies = data.get("movies", {})
        if not movies:
            print(f"⚠️  No movies found in {filepath}")
            return False

        last_updated = data.get("last_updated", "")
        timestamp = datetime.now(IST)

        collection = self.db[COLLECTION_ADVANCE]

        # Convert movies dict to array format
        movies_array = []
        for movie_name, movie_data in movies.items():
            movies_array.append({
                "movie": movie_name,
                **movie_data
            })

        # Create document structure
        doc = {
            "_id": f"advance_{date_code}",
            "show_date": date_code,
            "last_updated": last_updated,
            "synced_at": timestamp,
            "total_movies": len(movies),
            "movies": movies_array
        }

        # Upsert the day's advance data
        try:
            result = collection.replace_one(
                {"_id": f"advance_{date_code}"},
                doc,
                upsert=True
            )

            if result.upserted_id:
                print(
                    f"   ✅ Inserted: advance_{date_code} ({len(movies)} movies)")
            else:
                print(
                    f"   ✅ Updated: advance_{date_code} ({len(movies)} movies)")

            return True

        except Exception as e:
            print(f"   ❌ Error syncing {date_code}: {e}")
            return False

    def create_indexes(self):
        """Create indexes for better query performance"""
        print("\n🔍 Creating indexes...")

        try:
            collection = self.db[COLLECTION_ADVANCE]

            # Show date index
            collection.create_index([("show_date", -1)])

            # Movie name index
            collection.create_index([("movies.movie", 1)])

            # Gross index
            collection.create_index([("movies.gross", -1)])

            # Occupancy index
            collection.create_index([("movies.occupancy", -1)])

            # Last updated index
            collection.create_index([("last_updated", -1)])

            # Synced at index
            collection.create_index([("synced_at", -1)])

            print(f"✅ Indexes created for {COLLECTION_ADVANCE}")
        except Exception as e:
            print(f"⚠️  Index creation warning: {e}")

    def sync_all(self):
        """Sync all advance booking summaries (multiple dates)"""
        # Find all advance summary files
        summary_files = self.find_all_advance_summaries()

        if not summary_files:
            print("\n❌ No advance summary files found")
            return False

        print(f"\n📊 Syncing {len(summary_files)} advance summaries...")

        success_count = 0
        fail_count = 0

        # Sync each summary
        for file_info in summary_files:
            filepath = file_info['path']
            date_code = file_info['date']

            print(f"\n📅 Processing: {date_code}")
            if self.sync_single_summary(filepath, date_code):
                success_count += 1
            else:
                fail_count += 1

        # Create indexes
        self.create_indexes()

        # Summary
        print("\n" + "="*60)
        print(f"📊 SYNC SUMMARY:")
        print(f"   ✅ Success: {success_count}")
        print(f"   ❌ Failed: {fail_count}")
        print(f"   📁 Total: {len(summary_files)}")
        print("="*60)

        return success_count > 0

    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        print("\n👋 MongoDB connection closed")


def main():
    print("🚀 Starting MongoDB Advance Booking Sync (Multi-Date)...")
    print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

    syncer = MongoDBAdvanceSync()

    try:
        success = syncer.sync_all()

        if success:
            print("\n" + "="*60)
            print("✅ ADVANCE SUMMARY SYNC COMPLETED")
            print("="*60)
            sys.exit(0)
        else:
            print("\n" + "="*60)
            print("❌ SYNC FAILED")
            print("="*60)
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
