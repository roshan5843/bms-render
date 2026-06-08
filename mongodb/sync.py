
# mongodb/sync.py — R2 VERSION
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

