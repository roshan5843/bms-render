# bms-render/bms9.py
import json
import os
import asyncio
import aiohttp
from datetime import datetime, timedelta
import pytz
from r2_client import r2_upload_json
# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 9
CONCURRENCY = 20
TIMEOUT = aiohttp.ClientTimeout(total=25)

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)

DATE_CODE = (NOW_IST + timedelta(days=1)).strftime("%Y%m%d")
DATE_DISTRICT = (NOW_IST + timedelta(days=1)).strftime("%Y-%m-%d")

# REPLACE with simple log to stdout only (no disk)
BASE_DIR = f"advance/{DATE_CODE}"  # used only for R2 key naming

R2_DETAILED_KEY = f"advance/{DATE_CODE}/detailed{SHARD_ID}.json"
R2_SUMMARY_KEY = f"advance/{DATE_CODE}/movie_summary{SHARD_ID}.json"

API_URL = os.getenv("MY_API_URL")

if not API_URL:
    raise RuntimeError("❌ MY_API_URL env variable not set")

# =====================================================
# LOGGING
# =====================================================


def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)


# =====================================================
# LOAD DISTRICT VENUES
# =====================================================
with open("districtvenues.json", "r", encoding="utf-8") as f:
    DIST_VENUES = json.load(f)

log(f"📍 Loaded {len(DIST_VENUES)} district venues")

# =====================================================
# HELPERS
# =====================================================


def format_state(s):
    if not s:
        return "Unknown"
    return " ".join(w.capitalize() for w in s.replace("-", " ").split())


def format_chain(s):
    if not s:
        return "Unknown"
    return " ".join(w.capitalize() for w in s.replace("-", " ").split())


def dedupe(rows):
    seen = set()
    out = []

    for r in rows:
        key = (
            r.get("venue", ""),
            r.get("time", ""),
            r.get("session_id", ""),
            r.get("audi", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out

# =====================================================
# FETCH SINGLE VENUE
# =====================================================


async def fetch_one(session, venue):
    cid = venue.get("id")
    url = API_URL.format(cid=cid, date=DATE_DISTRICT)

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                log(f"⚠ {cid} status {resp.status}")
                return None

            data = await resp.json()
            session_dates = data.get("data", {}).get("sessionDates", [])

            # ---- DATE SKIP ----
            if DATE_DISTRICT not in session_dates:
                return None

            return {"venue": venue, "data": data}

    except Exception as e:
        log(f"❌ {cid} {type(e).__name__}")
        return None

# =====================================================
# FETCH ALL (ASYNC)
# =====================================================


async def fetch_all():
    sem = asyncio.Semaphore(CONCURRENCY)
    results = []

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:

        async def bound(v):
            async with sem:
                return await fetch_one(session, v)

        tasks = [bound(v) for v in DIST_VENUES]
        raw = await asyncio.gather(*tasks)

    for r in raw:
        if r:
            results.append(r)

    log(f"✅ Fetched {len(results)} venues with shows")
    return results

# =====================================================
# PARSE DATA
# =====================================================


def parse(results):
    detailed = []

    for res in results:
        venue_meta = res["venue"]
        data = res["data"]

        city = venue_meta.get("city") or "Unknown"
        state = format_state(venue_meta.get("state"))

        cinema = data.get("meta", {}).get("cinema", {})

        venue_name = (
            cinema.get("name")
            or venue_meta.get("name")
            or venue_meta.get("district_name")
            or "Unknown"
        )

        venue_addr = (
            cinema.get("address")
            or venue_meta.get("address")
            or ""
        )

        chain = format_chain(
            venue_meta.get("chainKey")
            or venue_meta.get("chain")
            or venue_name
        )

        movies = data.get("meta", {}).get("movies", []) or []
        movie_map = {}
        for m in movies:
            movie_map[m.get("id")] = m
            movie_map[str(m.get("id"))] = m

        for s in data.get("pageData", {}).get("sessions", []) or []:
            mid = s.get("mid")
            movie = movie_map.get(mid) or movie_map.get(str(mid))
            if not movie:
                continue

            name = movie.get("name", "Unknown")
            lang = s.get("lang") or movie.get("lang") or ""
            fmt = s.get("scrnFmt") or ""
            fmt = fmt.replace("-", " | ") if fmt else ""

            movie_key = (
                f"{name} [{fmt} | {lang}]"
                if fmt else f"{name} | {lang}"
            )

            total = int(s.get("total", 0))
            avail = int(s.get("avail", 0))
            sold = total - avail

            gross = sum(
                (a.get("sTotal", 0) - a.get("sAvail", 0)) * a.get("price", 0)
                for a in s.get("areas", []) or []
            )

            occ = (sold / total * 100) if total else 0

            detailed.append({
                "movie": movie_key,
                "city": city,
                "state": state,
                "venue": venue_name,
                "address": venue_addr,
                "time": (
                    datetime.strptime(s.get("showTime"), "%Y-%m-%dT%H:%M")
                    .replace(tzinfo=pytz.UTC)
                    .astimezone(IST)
                    .strftime("%I:%M %p")
                    if s.get("showTime") else ""
                ),
                "audi": s.get("audi", ""),
                "session_id": str(s.get("id", "")),
                "totalSeats": total,
                "available": avail,
                "sold": sold,
                "gross": round(gross, 2),
                "occupancy": f"{round(occ, 2)}%",
                "source": "District",
                "date": DATE_CODE,
                "chain": chain
            })

    return dedupe(detailed)

# =====================================================
# BUILD SUMMARY
# =====================================================


def build_summary(detailed):
    summary = {}

    for r in detailed:
        movie = r["movie"]
        city = r["city"]
        state = r["state"]
        venue = r["venue"]
        chain = r["chain"]

        total = r["totalSeats"]
        sold = r["sold"]
        gross = r["gross"]
        occ = (sold / total * 100) if total else 0

        if movie not in summary:
            summary[movie] = {
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "venues": set(),
                "cities": set(),
                "fastfilling": 0,
                "housefull": 0,
                "details": {},
                "Chain_details": {}
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
            m["details"][ck] = {
                "city": city,
                "state": state,
                "venues": set(),
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0,
                "housefull": 0
            }

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
            m["Chain_details"][chain] = {
                "chain": chain,
                "venues": set(),
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0,
                "housefull": 0
            }

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

    final = {}
    for movie, m in summary.items():
        final[movie] = {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
            "details": [],
            "Chain_details": []
        }

        for d in m["details"].values():
            final[movie]["details"].append({
                "city": d["city"],
                "state": d["state"],
                "venues": len(d["venues"]),
                "shows": d["shows"],
                "gross": round(d["gross"], 2),
                "sold": d["sold"],
                "totalSeats": d["totalSeats"],
                "fastfilling": d["fastfilling"],
                "housefull": d["housefull"],
                "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0
            })

        for c in m["Chain_details"].values():
            final[movie]["Chain_details"].append({
                "chain": c["chain"],
                "venues": len(c["venues"]),
                "shows": c["shows"],
                "gross": round(c["gross"], 2),
                "sold": c["sold"],
                "totalSeats": c["totalSeats"],
                "fastfilling": c["fastfilling"],
                "housefull": c["housefull"],
                "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0
            })

    return final

# =====================================================
# ENTRY
# =====================================================


async def main():
    log("🚀 DISTRICT SCRAPER STARTED")
    results = await fetch_all()
    detailed = parse(results)
    summary = build_summary(detailed)

    # SAVE TO R2
    r2_upload_json(R2_DETAILED_KEY, detailed)
    r2_upload_json(R2_SUMMARY_KEY, summary)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(summary)}")

if __name__ == "__main__":
    asyncio.run(main())
