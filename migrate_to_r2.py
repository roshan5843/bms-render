# migrate_to_r2.py  — run ONCE locally
import boto3
import os
import json
from pathlib import Path

R2_ENDPOINT = input("R2 Endpoint URL: ").strip()
R2_ACCESS_KEY = input("R2 Access Key: ").strip()
R2_SECRET_KEY = input("R2 Secret Key: ").strip()
R2_BUCKET = input("R2 Bucket name [bms-data]: ").strip() or "bms-data"

r2 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
)

BASE = Path("daily/data")

if not BASE.exists():
    print("❌ daily/data directory not found. Run from repo root.")
    exit(1)

all_files = list(BASE.rglob("*.json"))
print(f"📦 Found {len(all_files)} JSON files to migrate\n")

success = 0
failed = 0

for local_path in all_files:
    # Convert: daily/data/20250401/detailed1.json
    # To R2:   daily/20250401/detailed1.json
    parts = local_path.parts  # ('daily', 'data', '20250401', 'detailed1.json')
    r2_key = "/".join(["daily"] + list(parts[2:]))  # skip 'data'

    try:
        r2.upload_file(
            str(local_path),
            R2_BUCKET,
            r2_key,
            ExtraArgs={"ContentType": "application/json"}
        )
        print(f"✅ {r2_key}")
        success += 1
    except Exception as e:
        print(f"❌ {r2_key} → {e}")
        failed += 1

print(f"\n🎉 Migration complete: {success} uploaded, {failed} failed")
print(f"\nNext step: add daily/ and advance/ to .gitignore")
print("  echo 'daily/' >> .gitignore")
print("  echo 'advance/' >> .gitignore")
print("  git rm -r --cached daily/ advance/ 2>/dev/null || true")
print("  git commit -m 'chore: move data to R2, remove from git tracking'")
print("  git push")
