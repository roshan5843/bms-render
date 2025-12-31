import os
from datetime import datetime
import pytz

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("MONGODB_DATABASE", "movie-blog")

# Collections
COLLECTION_SUMMARY = "daily_summary"
COLLECTION_DETAILED = "daily_detailed"

# Timezone
IST = pytz.timezone("Asia/Kolkata")

# File paths


def get_date_code():
    return datetime.now(IST).strftime("%Y%m%d")


def get_file_paths():
    date_code = get_date_code()
    base_dir = f"daily/data/{date_code}"
    return {
        "summary": f"{base_dir}/finalsummary.json",
        "detailed": f"{base_dir}/finaldetailed.json"
    }
