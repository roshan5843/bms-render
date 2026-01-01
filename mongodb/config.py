import os
from datetime import datetime
import pytz

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("MONGODB_DATABASE", "movie-blog")

# Collections
COLLECTION_SUMMARY = "daily_summary"

# Timezone
IST = pytz.timezone("Asia/Kolkata")
