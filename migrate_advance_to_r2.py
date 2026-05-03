# migrate_advance_to_r2.py — run ONCE locally
import boto3
import os
from pathlib import Path

R2_ENDPOINT = input("R2 Endpoint URL: ").strip()
R2_ACCESS_KEY = input("R2 Access Key: ").strip()
R2_SECRET_KEY = input("R2 Secret Key: ").strip()
R2_BUCKET = input("R2 Bucket [bms-data]: ").strip() or "bms-data"

r2 = boto3.client("s3",
                  endpoint_url=R2_ENDPOINT,
                  aws_access_key_id=R2_ACCESS_KEY,
                  aws_secret_access_key=R2_SECRET_KEY,
                  region_name="auto",
                  )

BASE = Path("advance/data")
all_files = list(BASE.rglob("*.json"))
print(f"📦 Found {len(all_files)} files to migrate")

success = failed = 0
for local_path in all_files:
    # advance/data/20260502/finalsummary.json → advance/20260502/finalsummary.json
    parts = local_path.parts
    r2_key = "/".join(["advance"] + list(parts[2:]))  # skip 'data'
    try:
        r2.upload_file(str(local_path), R2_BUCKET, r2_key,
                       ExtraArgs={"ContentType": "application/json"})
        print(f"✅ {r2_key}")
        success += 1
    except Exception as e:
        print(f"❌ {r2_key} → {e}")
        failed += 1

print(f"\n🎉 Done: {success} uploaded, {failed} failed")
