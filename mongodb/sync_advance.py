
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient
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

    # ---------------------------------------------------
    # 🔍 FIND FILES
    # ---------------------------------------------------
    def find_all_advance_summaries(self):
        print("\n🔍 Searching for advance summary files...")

        base_dir = Path("advance/data")

        if not base_dir.exists():
            print(f"❌ Directory not found: {base_dir}")
            return []

        date_dirs = sorted(
            [d for d in base_dir.iterdir() if d.is_dir() and d.name.isdigit()],
            key=lambda x: x.name
        )

        summary_files = []

        for date_dir in date_dirs:
            summary_path = date_dir / "finalsummary.json"

            if summary_path.exists():
                summary_files.append({
                    "path": str(summary_path),
                    "date": date_dir.name
                })

        print(f"📊 Total summaries found: {len(summary_files)}")
        return summary_files

    # ---------------------------------------------------
    # 📂 LOAD JSON
    # ---------------------------------------------------
    def load_json(self, filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Error loading {filepath}: {e}")
            return None

    # ---------------------------------------------------
    # 🔄 SYNC SINGLE FILE
    # ---------------------------------------------------
    def sync_single_summary(self, filepath, date_code):
        data = self.load_json(filepath)
        if not data:
            return False

        movies = data.get("movies", {})
        if not movies:
            print(f"⚠️ No movies found in {filepath}")
            return False

        last_updated = data.get("last_updated", "")
        timestamp = datetime.now(IST)

        collection = self.db[COLLECTION_ADVANCE]

        movies_array = []
        for movie_name, movie_data in movies.items():
            movies_array.append({
                "movie": movie_name,
                **movie_data
            })

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

            if result.upserted_id:
                print(f"   ✅ Inserted: {doc['_id']}")
            else:
                print(f"   🔄 Updated: {doc['_id']}")

            return True

        except Exception as e:
            print(f"   ❌ Error syncing {date_code}: {e}")
            return False

    # ---------------------------------------------------
    # ⚡ SMART SYNC (MAIN FIX)
    # ---------------------------------------------------
    def sync_all(self):
        summary_files = self.find_all_advance_summaries()

        if not summary_files:
            print("\n❌ No files found")
            return False

        collection = self.db[COLLECTION_ADVANCE]

        print("\n🧠 Smart filtering (new + updated only)...")

        new_files = []
        skipped = 0

        for file_info in summary_files:
            filepath = file_info["path"]
            date_code = file_info["date"]
            doc_id = f"advance_{date_code}"

            # Load only metadata (fast)
            data = self.load_json(filepath)
            if not data:
                continue

            file_last_updated = data.get("last_updated", "")

            # 🔥 SMART CHECK (Mongo vs File)
            doc = collection.find_one(
                {"_id": doc_id},
                {"last_updated": 1}
            )

            if not doc or doc.get("last_updated") != file_last_updated:
                new_files.append({
                    "path": filepath,
                    "date": date_code
                })
            else:
                skipped += 1

        print(f"🆕 To Sync: {len(new_files)}")
        print(f"⏭ Skipped (unchanged): {skipped}")

        if not new_files:
            print("✅ Everything already up-to-date")
            return True

        # ---------------------------------------------------
        # 🚀 PROCESS ONLY REQUIRED FILES
        # ---------------------------------------------------
        success_count = 0
        fail_count = 0

        for file_info in new_files:
            print(f"\n📅 Processing: {file_info['date']}")

            if self.sync_single_summary(
                file_info["path"],
                file_info["date"]
            ):
                success_count += 1
            else:
                fail_count += 1

        # ---------------------------------------------------
        # 📊 SUMMARY
        # ---------------------------------------------------
        print("\n" + "=" * 60)
        print("📊 FINAL SYNC SUMMARY")
        print(f"   ✅ Success: {success_count}")
        print(f"   ❌ Failed: {fail_count}")
        print(f"   🆕 Processed: {len(new_files)}")
        print(f"   ⏭ Skipped: {skipped}")
        print("=" * 60)

        return success_count > 0

    # ---------------------------------------------------
    def close(self):
        self.client.close()
        print("\n👋 MongoDB connection closed")


# -------------------------------------------------------
# 🚀 MAIN
# -------------------------------------------------------
def main():
    print("🚀 Starting Smart MongoDB Sync...")
    print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

    syncer = MongoDBAdvanceSync()

    try:
        success = syncer.sync_all()

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

    finally:
        syncer.close()


if __name__ == "__main__":
    main()

