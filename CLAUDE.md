# BMS Render — Agentic Coding Reference

## What This Repo Does

Real-time Indian box office scraper for **BookMyShow (BMS)**. It runs parallel scraping jobs across 9 shards, aggregates results, and stores everything in **Cloudflare R2** (S3-compatible) with optional sync to **MongoDB Atlas**. Powers a movie analytics/blog platform.

---

## Architecture Overview

```
[GitHub Actions / Render runners]
   │
   ├─ 9 parallel scrapers (bms*.py / bmsdaily*.py / bmsrotate*.py)
   │       └─ scrape BMS API → upload shards to R2
   │
   ├─ Combiner (combine_shards.py / combine_dailyshards.py / combine_shards_rotate.py)
   │       └─ merge 9 shards → finaldetailed.json + finalsummary.json → R2
   │
   └─ MongoDB sync (mongodb/sync.py + sync_advance.py) [optional]
           └─ reads final R2 files → upserts to MongoDB collections
```

### R2 Bucket Structure

```
bms-data/
├─ advance/{DATE_CODE}/
│   ├─ detailed{1-9}.json       ← individual shard output (deleted after combine)
│   ├─ movie_summary{1-9}.json  ← individual shard output (deleted after combine)
│   ├─ finaldetailed.json       ← RETAINED: all show records merged
│   └─ finalsummary.json        ← RETAINED: per-movie aggregated stats
│
└─ daily/{DATE_CODE}/
    ├─ detailed{1-9}.json
│   ├─ movie_summary{1-9}.json
    ├─ finaldetailed.json
    └─ finalsummary.json
```

`DATE_CODE` format: `YYYYMMDD` (e.g. `20261103`)

---

## File Map

### Scraper Shards

| File pattern | Prefix | Date source | Venue file |
|---|---|---|---|
| `bms{N}.py` | `advance/` | tomorrow (auto) | `venues{N}.json` |
| `bmsdaily{N}.py` | `daily/` | today (auto) | `venues{N}.json` |
| `bmsrotate{N}.py` | `advance/` | `DATE_CODE` env var | `venues{N}.json` |

N = 1–9. Each file has `SHARD_ID = N` hard-coded at the top. The 9 venue JSON files (`venues1.json`–`venues8.json`) split the venue list so shards run independently in parallel.

### Combiners

| File | Reads from | Writes to |
|---|---|---|
| `combine_shards.py` | `advance/{DATE}/detailed{1-9}.json` | `advance/{DATE}/finaldetailed.json` + `finalsummary.json` |
| `combine_dailyshards.py` | `daily/{DATE}/detailed{1-9}.json` | `daily/{DATE}/finaldetailed.json` + `finalsummary.json` |
| `combine_shards_rotate.py` | `advance/{DATE_CODE}/detailed{1-9}.json` | same prefix, final files |
| `simplecombiner.py` | local disk (legacy) | local disk (legacy) |

### Utilities

| File | Purpose |
|---|---|
| `r2_client.py` | Shared R2 helpers: `get_r2()`, `r2_upload_json()`, `r2_download_json()`, `r2_key_exists()` |
| `cleanup_shard_files.py` | Deletes 5–10-day-old shard files from R2 to save storage |
| `migrate_to_r2.py` | One-off: bulk-upload historical `daily/` data from disk to R2 |
| `migrate_advance_to_r2.py` | One-off: same for `advance/` data |

### MongoDB Layer (`mongodb/`)

| File | Purpose |
|---|---|
| `config.py` | Connection config, collection names |
| `sync.py` | Reads `daily/*/finalsummary.json` → upserts to `daily_summary` collection |
| `sync_advance.py` | Reads `advance/*/finalsummary.json` → upserts to `advance_summary` collection |
| `r2_client.py` | Duplicate R2 client (used standalone by MongoDB scripts) |

---

## Environment Variables

All secrets are injected via environment variables (GitHub Actions secrets or Render env vars).

| Variable | Used by | Purpose |
|---|---|---|
| `R2_ENDPOINT` | all scrapers, combiners, r2_client.py | Cloudflare R2 S3 API URL |
| `R2_ACCESS_KEY` | r2_client.py | R2 auth |
| `R2_SECRET_KEY` | r2_client.py | R2 auth |
| `R2_BUCKET` | r2_client.py | Bucket name (default: `bms-data`) |
| `BMS_PROXY` | bms*.py scrapers | Optional HTTP proxy (`http://user:pass@host:port`) |
| `MONGODB_URI` | mongodb/config.py | MongoDB Atlas connection string |
| `MONGODB_DATABASE` | mongodb/config.py | Database name (default: `movie-blog`) |
| `DATE_CODE` | bmsrotate*.py, combine_shards_rotate.py | Custom date for rotated runs (YYYYMMDD) |

---

## Data Model

### Detailed Record (one per show)

```python
{
    "movie": "Pushpa 2: The Rule [2D | Hindi]",
    "venue": "PVR Cinemas, Phoenix Market City",
    "address": "Viman Nagar, Pune",
    "city": "Pune",
    "state": "Maharashtra",
    "chain": "PVR Cinemas",
    "time": "6:30 PM",
    "audi": "Screen 1",
    "session_id": "1234567890",
    "totalSeats": 150,
    "available": 45,
    "sold": 105,
    "gross": 210000.00,
    "source": "BMS",
    "date": "20261103"
}
```

### Summary Record (aggregated per movie, in `finalsummary.json`)

```python
{
    "Pushpa 2: The Rule [2D | Hindi]": {
        "shows": 120,
        "gross": 15000000.00,
        "sold": 9500,
        "totalSeats": 12000,
        "venues": 45,
        "cities": 25,
        "occupancy": 79.17,
        "fastfilling": 50,    # shows where occupancy > threshold
        "housefull": 12,      # shows where sold == totalSeats
        "details": [          # per-city breakdown
            {"city": "Pune", "state": "Maharashtra", "venues": 8, ...}
        ],
        "Chain_details": [    # per-chain breakdown
            {"chain": "PVR Cinemas", "venues": 5, ...}
        ]
    }
}
```

---

## Coding Conventions

### Adding a new shard script
- Copy an existing `bms{N}.py`, change `SHARD_ID = N` and venue file to `venues{N}.json`
- Mirror any logic changes across all 9 (or all 9 daily / all 9 rotate) files
- The pattern is intentional — 9 identical scripts with only SHARD_ID differing

### Anti-bot / HTTP client
- Primary: `curl_cffi` with Chrome TLS fingerprint (real browser impersonation)
- Fallback: `cloudscraper`
- Never use plain `requests` — BMS blocks it

### Timeouts & retries
- Hard API timeout: 20s
- Inter-request delay: random 0.45–1.75s, adaptive on failures
- Retry rounds: 3 attempts per venue, 2 final retry passes
- IP block detection: 5 consecutive failures → 60s cooldown → abort after 3 cooldowns

### R2 uploads
- Always use `r2_client.py` helpers — never raw boto3 calls inline
- No local disk writes in production; write directly to R2

### Deduplication key in combiners
- `venue + time + session_id + auditorium` — any change here affects data integrity

### Timezone
- Always IST (`Asia/Kolkata`, UTC+5:30) — use `pytz.timezone("Asia/Kolkata")`

---

## How to Make Common Changes

### Add a new field to scraped data
1. Update the scraping logic in all 9 `bms{N}.py` files (and daily/rotate variants if needed)
2. Update the normalization/defaults in `combine_shards.py` (`normalize_row()` function)
3. Update `combine_dailyshards.py` and `combine_shards_rotate.py` equivalently
4. If the field goes into the summary, update the aggregation logic too
5. Update MongoDB schema/upsert logic in `mongodb/sync*.py` if needed

### Add a new venue set
1. Create `venues{N}.json` with the new venue list
2. Create `bms{N}.py` (copy existing, set `SHARD_ID = N`)
3. Create `bmsdaily{N}.py` and `bmsrotate{N}.py` equivalently
4. Update the combiners to also pull `detailed{N}.json` in their merge loops

### Change date logic (advance booking window)
- Advance scraper: look for `timedelta(days=1)` in `bms{N}.py`
- Daily scraper: look for `CUTOFF_MINUTES = 210` in `bmsdaily{N}.py`

### Add a new storage destination
- Add helpers to `r2_client.py` (or `mongodb/r2_client.py` for MongoDB scripts)
- Call from the combiner or sync script, not from individual shards

---

## Dependencies

Key packages from `requirements.txt`:

| Package | Why |
|---|---|
| `curl_cffi` | Real Chrome TLS fingerprint — primary HTTP client |
| `cloudscraper` | CloudFlare bypass — fallback HTTP client |
| `boto3==1.34.162` | R2/S3 client |
| `pymongo` + `dnspython` | MongoDB Atlas driver |
| `pytz` | IST timezone handling |
| `selenium` + `selenium-wire` | Headless browser (fallback, rarely used) |
| `webdriver-manager` | Auto-downloads ChromeDriver |

---

## What to Never Do

- Don't use plain `requests` for BMS API calls — it gets blocked
- Don't write shard output to local disk in production — upload directly to R2
- Don't change the deduplication key without migrating existing R2 data
- Don't hard-code dates — always derive from `datetime.now(IST)` or read `DATE_CODE` env var
- Don't skip updating all 9 shard files when changing shared scraping logic
