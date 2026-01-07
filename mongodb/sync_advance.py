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

    def find_latest_advance_summary(self):
        """Find the most recent advance booking summary file"""
        print("\n🔍 Searching for latest advance summary file...")

        base_dir = Path("advance/data")

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
            print("❌ No date directories found in advance/data/")
            return None

        # Search for summary file in newest directories first (check last 3 days)
        for date_dir in date_dirs[:3]:
            summary_path = date_dir / "finalsummary.json"

            if summary_path.exists():
                print(f"✅ Found advance summary in: {date_dir.name}")
                print(f"   • Summary: {summary_path}")
                return str(summary_path)

        print("❌ No finalsummary.json found in recent advance directories")
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

        # Fallback to tomorrow's date (advance bookings are for tomorrow)
        tomorrow = datetime.now(IST) + timedelta(days=1)
        return tomorrow.strftime("%Y%m%d")

    def sync_advance_summary(self, filepath):
        """Sync advance booking summary data as nested document structure"""
        print(f"\n📊 Syncing advance summary from: {filepath}")

        data = self.load_json(filepath)
        if not data:
            return False

        movies = data.get("movies", {})
        if not movies:
            print("⚠️  No movies found in advance summary")
            return False

        date_code = self.extract_date_from_data(data)
        last_updated = data.get("last_updated")
        timestamp = datetime.now(IST)

        collection = self.db[COLLECTION_ADVANCE]

        print(f"📅 Date code (Show Date): {date_code}")
        print(f"📁 Collection: {COLLECTION_ADVANCE}")
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
            "_id": f"advance_{date_code}",  # e.g., "advance_20250103"
            "show_date": date_code,          # Date for which shows are booked
            "last_updated": last_updated,
            "synced_at": timestamp,
            "total_movies": len(movies),
            "movies": movies_array
        }

        # Upsert the entire day's advance data
        try:
            result = collection.replace_one(
                {"_id": f"advance_{date_code}"},
                doc,
                upsert=True
            )

            if result.upserted_id:
                print(f"✅ Advance summary inserted:")
                print(f"   • Document ID: advance_{date_code}")
                print(f"   • Show Date: {date_code}")
                print(f"   • Total movies: {len(movies)}")
            else:
                print(f"✅ Advance summary updated:")
                print(f"   • Document ID: advance_{date_code}")
                print(f"   • Show Date: {date_code}")
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
            collection = self.db[COLLECTION_ADVANCE]

            # Show date index
            collection.create_index([("show_date", -1)])

            # Movie name index (for searching within movies array)
            collection.create_index([("movies.movie", 1)])

            # Gross index (for top grossing queries)
            collection.create_index([("movies.gross", -1)])

            # Occupancy index
            collection.create_index([("movies.occupancy", -1)])

            # Last updated index
            collection.create_index([("last_updated", -1)])

            print(f"✅ Indexes created for {COLLECTION_ADVANCE}")
        except Exception as e:
            print(f"⚠️  Index creation warning: {e}")

    def sync_all(self):
        """Sync advance booking summary data"""
        # Find latest advance summary file
        summary_path = self.find_latest_advance_summary()

        if not summary_path:
            print("\n❌ Could not find advance summary file to sync")
            return False

        # Sync advance summary
        success = self.sync_advance_summary(summary_path)

        if not success:
            print("\n❌ Advance summary sync failed")
            return False

        # Create indexes
        self.create_indexes()

        return True

    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        print("\n👋 MongoDB connection closed")


def main():
    print("🚀 Starting MongoDB Advance Booking Sync...")
    print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

    syncer = MongoDBAdvanceSync()

    try:
        success = syncer.sync_all()

        if success:
            print("\n" + "="*50)
            print("✅ ADVANCE SUMMARY SYNC COMPLETED SUCCESSFULLY")
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
