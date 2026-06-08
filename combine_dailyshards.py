# combine_dailyshards.py — R2 VERSION

import json
from datetime import datetime
import pytz
from r2_client import r2_upload_json, r2_download_json

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)
DATE_CODE = NOW_IST.strftime("%Y%m%d")
LAST_UPDATED = NOW_IST.strftime("%Y-%m-%d %H:%M IST")

R2_BASE = f"daily/{DATE_CODE}"

print(f"📁 R2 base path: r2://{R2_BASE}")
print(f"⏱ Last updated: {LAST_UPDATED}")

# =====================================================
# NORMALIZE ROW
# =====================================================


def normalize_row(r):
    r["movie"] = r.get("movie") or "Unknown"
    r["city"] = r.get("city") or "Unknown"
    r["state"] = r.get("state") or "Unknown"
    r["venue"] = r.get("venue") or "Unknown"
    r["address"] = r.get("address") or ""
    r["time"] = r.get("time") or ""
    r["audi"] = r.get("audi") or ""
    r["session_id"] = str(r.get("session_id") or "")
    r["chain"] = r.get("chain") or "Unknown"
    r["source"] = r.get("source") or "Unknown"
    r["date"] = r.get("date") or DATE_CODE
    r["totalSeats"] = int(r.get("totalSeats") or 0)
    r["available"] = int(r.get("available") or 0)
    r["sold"] = int(r.get("sold") or 0)
    r["gross"] = float(r.get("gross") or 0.0)

    occ = r.get("occupancy", "")
    if isinstance(occ, (int, float)):
        r["occupancy"] = f"{round(float(occ), 2)}%"
    elif isinstance(occ, str):
        if not occ.endswith("%"):
            try:
                r["occupancy"] = f"{round(float(occ), 2)}%"
            except:
                r["occupancy"] = "0%"
    else:
        r["occupancy"] = "0%"
    return r

# =====================================================
# DEDUPE
# =====================================================


def dedupe(rows):
    seen = set()
    out = []
    dupes = 0
    for r in rows:
        key = (r.get("venue", ""), r.get("time", ""),
               r.get("session_id", ""), r.get("audi", ""))
        if key in seen:
            dupes += 1
            continue
        seen.add(key)
        out.append(r)
    return out, dupes


# =====================================================
# LOAD SHARDS FROM R2
# =====================================================
all_rows = []

for i in range(1, 10):
    key = f"{R2_BASE}/detailed{i}.json"
    data = r2_download_json(key, default=[])
    if data:
        # handle case where shard saved as {last_updated, data} wrapper
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        print(f"✅ detailed{i}.json → {len(data)} rows")
        all_rows.extend(data)
    else:
        print(f"⚠️  detailed{i}.json not found in R2, skipping")

print(f"📊 Raw rows: {len(all_rows)}")

# =====================================================
# NORMALIZE + DEDUPE + SORT
# =====================================================
all_rows = [normalize_row(r) for r in all_rows]
final_rows, dupes = dedupe(all_rows)
print(f"🧹 Duplicates removed: {dupes}")
print(f"🎯 Final detailed rows: {len(final_rows)}")

final_rows.sort(key=lambda x: (x["movie"], x["city"], x["venue"], x["time"]))

# =====================================================
# UPLOAD finaldetailed.json TO R2
# =====================================================
r2_upload_json(
    f"{R2_BASE}/finaldetailed.json",
    {"last_updated": LAST_UPDATED, "data": final_rows}
)
print("🎉 finaldetailed.json uploaded to R2")

# =====================================================
# BUILD SUMMARY
# =====================================================
summary = {}

for r in final_rows:
    movie = r["movie"]
    city = r["city"]
    venue = r["venue"]
    chain = r["chain"]
    state = r["state"]

    total = r["totalSeats"]
    sold = r["sold"]
    gross = r["gross"]
    occ = (sold / total * 100) if total else 0

    if movie not in summary:
        summary[movie] = {
            "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
            "venues": set(), "cities": set(),
            "fastfilling": 0, "housefull": 0,
            "details": {}, "Chain_details": {}
        }

    m = summary[movie]
    m["shows"] += 1
    m["gross"] += gross
    m["sold"] += sold
    m["totalSeats"] += total
    m["venues"].add(venue)
    m["cities"].add(city)
    if occ >= 98:
        m["housefull"] += 1
    elif occ >= 50:
        m["fastfilling"] += 1

    ck = (city, state)
    if ck not in m["details"]:
        m["details"][ck] = {"city": city, "state": state, "venues": set(),
                            "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                            "fastfilling": 0, "housefull": 0}
    d = m["details"][ck]
    d["venues"].add(venue)
    d["shows"] += 1
    d["gross"] += gross
    d["sold"] += sold
    d["totalSeats"] += total
    if occ >= 98:
        d["housefull"] += 1
    elif occ >= 50:
        d["fastfilling"] += 1

    if chain not in m["Chain_details"]:
        m["Chain_details"][chain] = {"chain": chain, "venues": set(),
                                     "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                                     "fastfilling": 0, "housefull": 0}
    c = m["Chain_details"][chain]
    c["venues"].add(venue)
    c["shows"] += 1
    c["gross"] += gross
    c["sold"] += sold
    c["totalSeats"] += total
    if occ >= 98:
        c["housefull"] += 1
    elif occ >= 50:
        c["fastfilling"] += 1

# =====================================================
# FINALIZE + UPLOAD finalsummary.json TO R2
# =====================================================
final_summary = {}
for movie, m in summary.items():
    final_summary[movie] = {
        "shows": m["shows"],
        "gross": round(m["gross"], 2),
        "sold": m["sold"],
        "totalSeats": m["totalSeats"],
        "venues": len(m["venues"]),
        "cities": len(m["cities"]),
        "fastfilling": m["fastfilling"],
        "housefull": m["housefull"],
        "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
        "details": [
            {**{k: v for k, v in d.items() if k != "venues"},
             "venues": len(d["venues"]),
             "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0}
            for d in m["details"].values()
        ],
        "Chain_details": [
            {**{k: v for k, v in c.items() if k != "venues"},
             "venues": len(c["venues"]),
             "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0}
            for c in m["Chain_details"].values()
        ]
    }

r2_upload_json(
    f"{R2_BASE}/finalsummary.json",
    {"last_updated": LAST_UPDATED, "movies": final_summary}
)

print("🎉 finalsummary.json uploaded to R2")
print(f"   • r2://{R2_BASE}/finaldetailed.json")
print(f"   • r2://{R2_BASE}/finalsummary.json")
