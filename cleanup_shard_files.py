# import os
# from datetime import datetime, timedelta
# import pytz
#
# # ================= CONFIG =================
# BASE_PATHS = [
#     "advance/data",
#     "daily/data"
# ]
#
# IST = pytz.timezone("Asia/Kolkata")
#
# # 📅 Date range:
# # Start = 5 days ago (IST)
# # End   = yesterday (IST)
# START_DATE = (datetime.now(IST) - timedelta(days=5)).strftime("%Y%m%d")
# END_DATE   = (datetime.now(IST) - timedelta(days=1)).strftime("%Y%m%d")
#
# FILES_TO_DELETE = [
#     *(f"detailed{i}.json" for i in range(1, 10)),
#     *(f"movie_summary{i}.json" for i in range(1, 10)),
# ]
#
# # ================= HELPERS =================
# def daterange(start, end):
#     cur = datetime.strptime(start, "%Y%m%d")
#     end = datetime.strptime(end, "%Y%m%d")
#     while cur <= end:
#         yield cur.strftime("%Y%m%d")
#         cur += timedelta(days=1)
#
# # ================= CLEANUP =================
# deleted = 0
#
# print(f"🗓 Cleaning shard files from {START_DATE} → {END_DATE} (IST)\n")
#
# for date in daterange(START_DATE, END_DATE):
#     for base in BASE_PATHS:
#         folder = os.path.join(base, date)
#         if not os.path.isdir(folder):
#             continue
#
#         for fname in FILES_TO_DELETE:
#             path = os.path.join(folder, fname)
#             if os.path.exists(path):
#                 os.remove(path)
#                 deleted += 1
#                 print(f"🗑 Deleted: {path}")
#
# print(f"\n✅ Cleanup complete. Files removed: {deleted}")


# cleanup_shard_files.py — R2 VERSION
import os
import boto3
from datetime import datetime, timedelta
import pytz

IST = pytz.timezone("Asia/Kolkata")

# ================= CONFIG =================
# Delete shard files older than yesterday, going back 10 days
START_DATE = (datetime.now(IST) - timedelta(days=10)).strftime("%Y%m%d")
END_DATE = (datetime.now(IST) - timedelta(days=1)).strftime("%Y%m%d")

FILES_TO_DELETE = [
    *(f"detailed{i}.json" for i in range(1, 10)),
    *(f"movie_summary{i}.json" for i in range(1, 10)),
]

R2_BUCKETS = {
    "daily":   os.environ["R2_BUCKET"],
    "advance": os.environ["R2_BUCKET_ADVANCE"],
}

# ================= R2 CLIENT =================


def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY"],
        aws_secret_access_key=os.environ["R2_SECRET_KEY"],
        region_name="auto",
    )

# ================= HELPERS =================


def daterange(start, end):
    cur = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    while cur <= end_dt:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)


# ================= CLEANUP =================
r2 = get_r2()
deleted = 0
not_found = 0

print(f"🗓 Cleaning shard files from {START_DATE} → {END_DATE} (IST)\n")

for date in daterange(START_DATE, END_DATE):
    for folder_prefix, bucket in R2_BUCKETS.items():
        for fname in FILES_TO_DELETE:
            key = f"{folder_prefix}/{date}/{fname}"
            try:
                r2.head_object(Bucket=bucket, Key=key)
                r2.delete_object(Bucket=bucket, Key=key)
                print(f"🗑 Deleted: r2://{bucket}/{key}")
                deleted += 1
            except Exception:
                not_found += 1

print(f"\n✅ Cleanup complete.")
print(f"   🗑 Deleted  : {deleted}")
print(f"   ⏭ Not found: {not_found}")
