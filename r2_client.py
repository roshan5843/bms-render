# r2_client.py
from r2_client import r2_download_json, get_r2, BUCKET
import boto3
import json
import os

# mongodb/sync.py — add this at the very top
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# NOW this will work:
from r2_client import r2_download_json, get_r2, BUCKET

def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY"],
        aws_secret_access_key=os.environ["R2_SECRET_KEY"],
        region_name="auto",
    )


BUCKET = os.environ.get("R2_BUCKET", "bms-data")


def r2_upload_json(key, data):
    """Upload a Python object as JSON to R2."""
    r2 = get_r2()
    r2.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )
    print(f"☁️  Uploaded → r2://{BUCKET}/{key}")


def r2_download_json(key, default=None):
    """Download and parse JSON from R2. Returns default if key doesn't exist."""
    r2 = get_r2()
    try:
        resp = r2.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"⬇️  Downloaded ← r2://{BUCKET}/{key}")
        return data
    except r2.exceptions.NoSuchKey:
        print(f"⚠️  Not found in R2: {key}")
        return default
    except Exception as e:
        print(f"⚠️  R2 download failed for {key}: {e}")
        return default


def r2_key_exists(key):
    r2 = get_r2()
    try:
        r2.head_object(Bucket=BUCKET, Key=key)
        return True
    except:
        return False
