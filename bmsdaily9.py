

# # bms-render/bmsdaily9.py
# import json
# import os
# import asyncio
# import aiohttp
# from datetime import datetime, timedelta
# import pytz

# from r2_client import r2_upload_json, r2_download_json

# # =====================================================
# # CONFIG
# # =====================================================
# SHARD_ID = 9
# CONCURRENCY = 20
# TIMEOUT = aiohttp.ClientTimeout(total=25)

# CUTOFF_MINUTES = 200

# IST = pytz.timezone("Asia/Kolkata")
# NOW_IST = datetime.now(IST)

# DATE_CODE = NOW_IST.strftime("%Y%m%d")
# DATE_DISTRICT = NOW_IST.strftime("%Y-%m-%d")

# BASE_DIR = os.path.join("daily", "data", DATE_CODE)
# LOG_DIR = os.path.join(BASE_DIR, "logs")
# os.makedirs(LOG_DIR, exist_ok=True)

# R2_DETAILED_KEY = f"daily/{DATE_CODE}/detailed{SHARD_ID}.json"
# R2_SUMMARY_KEY = f"daily/{DATE_CODE}/movie_summary{SHARD_ID}.json"

# DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
# SUMMARY_FILE = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
# LOG_FILE = f"{LOG_DIR}/districtdaily{SHARD_ID}.log"

# API_URL = os.getenv("MY_API_URL")
# if not API_URL:
#     raise RuntimeError("❌ MY_API_URL env variable not set")

# # =====================================================
# # LOGGING
# # =====================================================


# def log(msg):
#     ts = datetime.now(IST).strftime("%H:%M:%S")
#     line = f"[{ts}] {msg}"
#     print(line, flush=True)
#     with open(LOG_FILE, "a", encoding="utf-8") as f:
#         f.write(line + "\n")


# # =====================================================
# # LOAD DISTRICT VENUES
# # =====================================================
# with open("districtvenues.json", "r", encoding="utf-8") as f:
#     DIST_VENUES = json.load(f)

# log(f"📍 Loaded {len(DIST_VENUES)} district venues")

# # =====================================================
# # HELPERS
# # =====================================================


# def format_state(s):
#     if not s:
#         return "Unknown"
#     return " ".join(w.capitalize() for w in s.replace("-", " ").split())


# def format_chain(s):
#     if not s:
#         return "Unknown"
#     return " ".join(w.capitalize() for w in s.replace("-", " ").split())


# def minutes_left(show_time_str):
#     """
#     Calculate minutes left from now (IST).
#     Handles post-midnight rollover.
#     """
#     try:
#         now = NOW_IST
#         t = datetime.strptime(show_time_str, "%I:%M %p").replace(
#             year=now.year, month=now.month, day=now.day, tzinfo=IST
#         )
#         if t < now - timedelta(hours=6):
#             t += timedelta(days=1)
#         return (t - now).total_seconds() / 60
#     except Exception:
#         return 9999


# # ─── NEW: classify show start time into one of four periods ──────────────────
# def get_period(show_time_str):
#     """
#     Returns 'morning' | 'afternoon' | 'evening' | 'night' based on show start hour.

#     Boundaries (same as shard 1-8 and the front-end):
#       Morning   00:00 – 11:59   (hour < 12)
#       Afternoon 12:00 – 15:59   (12 ≤ hour < 16)
#       Evening   16:00 – 19:59   (16 ≤ hour < 20)
#       Night     20:00 – 23:59   (hour ≥ 20)
#     """
#     try:
#         hour = datetime.strptime(show_time_str, "%I:%M %p").hour
#         if hour < 12:
#             return "morning"
#         if hour < 16:
#             return "afternoon"
#         if hour < 20:
#             return "evening"
#         return "night"
#     except Exception:
#         return None


# def show_key(r):
#     return (r.get("venue"), r.get("time"), r.get("session_id"), r.get("audi"))

# # =====================================================
# # FETCH SINGLE VENUE
# # =====================================================


# async def fetch_one(session, venue):
#     cid = venue.get("id")
#     url = API_URL.format(cid=cid, date=DATE_DISTRICT)

#     try:
#         async with session.get(url) as resp:
#             if resp.status != 200:
#                 log(f"⚠ {cid} status {resp.status}")
#                 return None
#             data = await resp.json()
#             if DATE_DISTRICT not in data.get("data", {}).get("sessionDates", []):
#                 return None
#             return {"venue": venue, "data": data}
#     except Exception as e:
#         log(f"❌ {cid} {type(e).__name__}")
#         return None

# # =====================================================
# # FETCH ALL (ASYNC)
# # =====================================================


# async def fetch_all():
#     sem = asyncio.Semaphore(CONCURRENCY)
#     results = []

#     async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
#         async def bound(v):
#             async with sem:
#                 return await fetch_one(session, v)
#         raw = await asyncio.gather(*[bound(v) for v in DIST_VENUES])

#     for r in raw:
#         if r:
#             results.append(r)

#     log(f"✅ Fetched {len(results)} venues with shows")
#     return results

# # =====================================================
# # PARSE + APPLY CUTOFF
# # =====================================================


# def parse(results):
#     detailed = []

#     for res in results:
#         venue_meta = res["venue"]
#         data = res["data"]

#         city = venue_meta.get("city") or "Unknown"
#         state = format_state(venue_meta.get("state"))

#         cinema = data.get("meta", {}).get("cinema", {})
#         venue_name = (
#             cinema.get("name")
#             or venue_meta.get("name")
#             or venue_meta.get("district_name")
#             or "Unknown"
#         )
#         venue_addr = cinema.get("address") or venue_meta.get("address") or ""
#         chain = format_chain(
#             venue_meta.get("chainKey") or venue_meta.get("chain") or venue_name
#         )

#         movies = data.get("meta", {}).get("movies", []) or []
#         movie_map = {str(m.get("id")): m for m in movies}

#         for s in data.get("pageData", {}).get("sessions", []) or []:
#             movie = movie_map.get(str(s.get("mid")))
#             if not movie:
#                 continue

#             name = movie.get("name", "Unknown")
#             lang = s.get("lang") or movie.get("lang") or ""
#             fmt = (s.get("scrnFmt") or "").replace("-", " | ")

#             movie_key = f"{name} [{fmt} | {lang}]" if fmt else f"{name} | {lang}"

#             show_time = (
#                 datetime.strptime(s.get("showTime"), "%Y-%m-%dT%H:%M")
#                 .replace(tzinfo=pytz.UTC)
#                 .astimezone(IST)
#                 .strftime("%I:%M %p")
#             )

#             mins = minutes_left(show_time)
#             if mins > CUTOFF_MINUTES:
#                 continue

#             total = int(s.get("total", 0))
#             avail = int(s.get("avail", 0))
#             sold = total - avail
#             gross = sum(
#                 (a.get("sTotal", 0) - a.get("sAvail", 0)) * a.get("price", 0)
#                 for a in s.get("areas", []) or []
#             )

#             detailed.append({
#                 "movie":      movie_key,
#                 "city":       city,
#                 "state":      state,
#                 "venue":      venue_name,
#                 "address":    venue_addr,
#                 "time":       show_time,
#                 "audi":       s.get("audi", ""),
#                 "session_id": str(s.get("id", "")),
#                 "totalSeats": total,
#                 "available":  avail,
#                 "sold":       sold,
#                 "gross":      round(gross, 2),
#                 "minsLeft":   round(mins, 1),
#                 "source":     "District",
#                 "date":       DATE_CODE,
#                 "chain":      chain,
#             })

#     return detailed

# # =====================================================
# # BUILD SUMMARY (FROM FINAL DETAILED)
# # =====================================================


# def build_summary(detailed):
#     # Period slot template
#     def _empty_periods():
#         return {
#             p: {"shows": 0, "sold": 0, "totalSeats": 0, "gross": 0.0}
#             for p in ("morning", "afternoon", "evening", "night")
#         }

#     summary = {}

#     for r in detailed:
#         movie = r["movie"]
#         venue = r["venue"]
#         city = r["city"]

#         total = r["totalSeats"]
#         sold = r["sold"]
#         gross = r["gross"]
#         occ = (sold / total * 100) if total else 0

#         if movie not in summary:
#             summary[movie] = {
#                 "shows":       0,
#                 "gross":       0.0,
#                 "sold":        0,
#                 "totalSeats":  0,
#                 "venues":      set(),
#                 "cities":      set(),
#                 "fastfilling": 0,
#                 "housefull":   0,
#                 # ── NEW: per-period accumulators ──────────────────────────
#                 "periods":     _empty_periods(),
#             }

#         m = summary[movie]
#         m["shows"] += 1
#         m["gross"] += gross
#         m["sold"] += sold
#         m["totalSeats"] += total
#         m["venues"].add(venue)
#         m["cities"].add(city)

#         if occ >= 98:
#             m["housefull"] += 1
#         elif occ >= 50:
#             m["fastfilling"] += 1

#         # ── NEW: accumulate into the correct time-slot bucket ─────────────
#         period = get_period(r["time"])
#         if period:
#             pd = m["periods"][period]
#             pd["shows"] += 1
#             pd["sold"] += sold
#             pd["totalSeats"] += total
#             pd["gross"] += gross

#     # ─── Finalise: compute per-period occupancy ───────────────────────────
#     final = {}
#     for movie, m in summary.items():
#         periods_out = {
#             slot: {
#                 "shows":      pd["shows"],
#                 "sold":       pd["sold"],
#                 "totalSeats": pd["totalSeats"],
#                 "gross":      round(pd["gross"], 2),
#                 "occupancy":  round((pd["sold"] / pd["totalSeats"]) * 100, 2)
#                 if pd["totalSeats"] else 0.0,
#             }
#             for slot, pd in m["periods"].items()
#         }

#         final[movie] = {
#             "shows":       m["shows"],
#             "gross":       round(m["gross"], 2),
#             "sold":        m["sold"],
#             "totalSeats":  m["totalSeats"],
#             "venues":      len(m["venues"]),
#             "cities":      len(m["cities"]),
#             "fastfilling": m["fastfilling"],
#             "housefull":   m["housefull"],
#             "occupancy":   round((m["sold"] / m["totalSeats"]) * 100, 2)
#             if m["totalSeats"] else 0.0,
#             # ── NEW ──────────────────────────────────────────────────────
#             "periods":     periods_out,
#         }

#     return final

# # =====================================================
# # ENTRY
# # =====================================================


# async def main():
#     log("🚀 DISTRICT DAILY SCRAPER STARTED")

#     results = await fetch_all()
#     fresh = parse(results)

#     old_rows = r2_download_json(R2_DETAILED_KEY, default=[])
#     log(f"📥 Loaded {len(old_rows)} existing rows from R2")

#     old_map = {show_key(r): r for r in old_rows}
#     new_map = {}

#     for r in fresh:
#         key = show_key(r)
#         if key in old_map:
#             old_map[key].update({
#                 "totalSeats": r["totalSeats"],
#                 "available":  r["available"],
#                 "sold":       r["sold"],
#                 "gross":      r["gross"],
#                 "minsLeft":   r["minsLeft"],
#             })
#             new_map[key] = old_map[key]
#         else:
#             new_map[key] = r

#     for key, r in old_map.items():
#         if key not in new_map:
#             new_map[key] = r

#     detailed = list(new_map.values())
#     summary = build_summary(detailed)

#     r2_upload_json(R2_DETAILED_KEY, detailed)
#     r2_upload_json(R2_SUMMARY_KEY,  summary)

#     log(f"✅ DONE | Shows={len(detailed)} | Movies={len(summary)}")


# if __name__ == "__main__":
#     asyncio.run(main())




# # bms-render/bmsdaily9.py  (FIXED: 403/429 handling, retries, throttling, reporting)
# import json
# import os
# import sys
# import asyncio
# import random
# from collections import Counter
# import aiohttp
# from datetime import datetime, timedelta
# import pytz

# from r2_client import r2_upload_json, r2_download_json

# # =====================================================
# # CONFIG
# # =====================================================
# SHARD_ID = 9

# # --- request pacing (the main fix for 403s: stop bursting) ---
# CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))          # was 20
# RETRY_CONCURRENCY = 3                                      # used for the cool-down passes
# REQUEST_DELAY = (0.05, 0.25)                               # random sleep before each request
# MAX_RETRIES = 4                                            # attempts per venue per pass
# EXTRA_PASSES = 2                                           # extra passes over failed venues
# PASS_COOLDOWN = 20                                         # seconds between passes
# TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)

# RETRYABLE = {403, 408, 425, 429, 500, 502, 503, 504}       # 403 here is usually WAF / rate limit
# FATAL = {400, 401}                                         # bad request / auth: retrying won't help

# # Exit non-zero if too many venues still failed (0 = disabled). e.g. 0.1 = 10%
# FAIL_EXIT_RATIO = float(os.getenv("FAIL_EXIT_RATIO", "0"))

# CUTOFF_MINUTES = 200

# IST = pytz.timezone("Asia/Kolkata")
# NOW_IST = datetime.now(IST)

# DATE_CODE = NOW_IST.strftime("%Y%m%d")
# DATE_DISTRICT = NOW_IST.strftime("%Y-%m-%d")

# BASE_DIR = os.path.join("daily", "data", DATE_CODE)
# LOG_DIR = os.path.join(BASE_DIR, "logs")
# os.makedirs(LOG_DIR, exist_ok=True)

# R2_DETAILED_KEY = f"daily/{DATE_CODE}/detailed{SHARD_ID}.json"
# R2_SUMMARY_KEY = f"daily/{DATE_CODE}/movie_summary{SHARD_ID}.json"

# DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
# SUMMARY_FILE = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
# LOG_FILE = f"{LOG_DIR}/districtdaily{SHARD_ID}.log"

# API_URL = os.getenv("MY_API_URL")
# if not API_URL:
#     raise RuntimeError("❌ MY_API_URL env variable not set")

# # Normal browser-like request headers. If the API needs cookies/tokens, put them
# # in a secret MY_API_HEADERS, e.g. {"Cookie": "...", "Authorization": "Bearer ..."}
# BASE_HEADERS = {
#     "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
#     "Accept": "application/json, text/plain, */*",
#     "Accept-Language": "en-IN,en;q=0.9",
# }
# try:
#     BASE_HEADERS.update(json.loads(os.getenv("MY_API_HEADERS", "{}")))
# except json.JSONDecodeError:
#     print("⚠ MY_API_HEADERS is not valid JSON – ignored", flush=True)

# STATUS_COUNTS = Counter()   # every HTTP status seen (including retries)

# # =====================================================
# # LOGGING
# # =====================================================


# def log(msg):
#     ts = datetime.now(IST).strftime("%H:%M:%S")
#     line = f"[{ts}] {msg}"
#     print(line, flush=True)
#     with open(LOG_FILE, "a", encoding="utf-8") as f:
#         f.write(line + "\n")


# # =====================================================
# # LOAD DISTRICT VENUES
# # =====================================================
# with open("districtvenues.json", "r", encoding="utf-8") as f:
#     DIST_VENUES = json.load(f)

# log(f"📍 Loaded {len(DIST_VENUES)} district venues")

# # =====================================================
# # HELPERS
# # =====================================================


# def format_state(s):
#     if not s:
#         return "Unknown"
#     return " ".join(w.capitalize() for w in s.replace("-", " ").split())


# def format_chain(s):
#     if not s:
#         return "Unknown"
#     return " ".join(w.capitalize() for w in s.replace("-", " ").split())


# def minutes_left(show_time_str):
#     """
#     Calculate minutes left from now (IST).
#     Handles post-midnight rollover.
#     """
#     try:
#         now = NOW_IST
#         t = datetime.strptime(show_time_str, "%I:%M %p").replace(
#             year=now.year, month=now.month, day=now.day, tzinfo=IST
#         )
#         if t < now - timedelta(hours=6):
#             t += timedelta(days=1)
#         return (t - now).total_seconds() / 60
#     except Exception:
#         return 9999


# def get_period(show_time_str):
#     """
#     Returns 'morning' | 'afternoon' | 'evening' | 'night' based on show start hour.
#       Morning   00:00 – 11:59
#       Afternoon 12:00 – 15:59
#       Evening   16:00 – 19:59
#       Night     20:00 – 23:59
#     """
#     try:
#         hour = datetime.strptime(show_time_str, "%I:%M %p").hour
#         if hour < 12:
#             return "morning"
#         if hour < 16:
#             return "afternoon"
#         if hour < 20:
#             return "evening"
#         return "night"
#     except Exception:
#         return None


# def show_key(r):
#     return (r.get("venue"), r.get("time"), r.get("session_id"), r.get("audi"))

# # =====================================================
# # FETCH SINGLE VENUE (retry + backoff)
# # =====================================================
# # Returns (status, payload):
# #   "ok"      -> payload = {"venue":..., "data":...}
# #   "noshows" -> nothing for today (not an error)
# #   "fatal"   -> 400/401, retrying will not help
# #   "failed"  -> still failing after retries (403/429/5xx/timeouts)


# async def fetch_one(session, venue):
#     cid = venue.get("id")
#     url = API_URL.format(cid=cid, date=DATE_DISTRICT)
#     last = ""

#     for attempt in range(1, MAX_RETRIES + 1):
#         await asyncio.sleep(random.uniform(*REQUEST_DELAY))
#         wait = None
#         try:
#             async with session.get(url) as resp:
#                 STATUS_COUNTS[resp.status] += 1

#                 if resp.status == 200:
#                     data = await resp.json(content_type=None)
#                     dates = (data.get("data") or {}).get("sessionDates") or []
#                     if DATE_DISTRICT not in dates:
#                         return "noshows", None
#                     return "ok", {"venue": venue, "data": data}

#                 if resp.status == 404:
#                     return "noshows", None

#                 if resp.status in FATAL:
#                     log(f"🚫 {cid} status {resp.status} (not retryable)")
#                     return "fatal", venue

#                 last = f"status {resp.status}"
#                 if resp.status in RETRYABLE:
#                     ra = resp.headers.get("Retry-After")
#                     wait = float(ra) if ra and ra.isdigit() else None
#                 else:
#                     log(f"⚠ {cid} status {resp.status} (not retryable)")
#                     return "fatal", venue

#         except (asyncio.TimeoutError, aiohttp.ClientError, ValueError) as e:
#             last = type(e).__name__

#         if attempt < MAX_RETRIES:
#             # exponential backoff with jitter: ~3s, 6s, 12s ...
#             delay = wait if wait is not None else min(3 * 2 ** (attempt - 1), 30)
#             await asyncio.sleep(delay + random.random() * 2)

#     return "failed", venue

# # =====================================================
# # FETCH ALL (ASYNC) with cool-down passes
# # =====================================================


# async def run_pass(session, venues, concurrency):
#     sem = asyncio.Semaphore(concurrency)

#     async def bound(v):
#         async with sem:
#             return await fetch_one(session, v)

#     return await asyncio.gather(*[bound(v) for v in venues])


# async def fetch_all():
#     results = []
#     noshows = 0
#     fatal = []

#     async with aiohttp.ClientSession(timeout=TIMEOUT, headers=BASE_HEADERS) as session:
#         pending = list(DIST_VENUES)
#         conc = CONCURRENCY

#         for pass_no in range(EXTRA_PASSES + 1):
#             if not pending:
#                 break
#             if pass_no:
#                 log(f"🔁 Pass {pass_no + 1}: retrying {len(pending)} venues "
#                     f"after {PASS_COOLDOWN}s cool-down (concurrency={RETRY_CONCURRENCY})")
#                 await asyncio.sleep(PASS_COOLDOWN)
#                 conc = RETRY_CONCURRENCY

#             raw = await run_pass(session, pending, conc)

#             pending = []
#             for status, payload in raw:
#                 if status == "ok":
#                     results.append(payload)
#                 elif status == "noshows":
#                     noshows += 1
#                 elif status == "fatal":
#                     fatal.append(payload)
#                 else:
#                     pending.append(payload)

#     failed = pending
#     log(f"📊 HTTP status counts (incl. retries): {dict(STATUS_COUNTS)}")
#     log(f"✅ Fetched {len(results)} venues with shows | no-shows={noshows} "
#         f"| fatal(400/401)={len(fatal)} | still-failed={len(failed)} "
#         f"| total={len(DIST_VENUES)}")

#     if failed:
#         ids = [str(v.get("id")) for v in failed]
#         log(f"❗ Failed venue IDs ({len(ids)}): {', '.join(ids[:50])}"
#             f"{' ...' if len(ids) > 50 else ''}")
#     if fatal:
#         ids = [str(v.get("id")) for v in fatal]
#         log(f"❗ Fatal venue IDs ({len(ids)}): {', '.join(ids[:50])}")

#     return results, len(failed) + len(fatal)

# # =====================================================
# # PARSE + APPLY CUTOFF
# # =====================================================


# def parse(results):
#     detailed = []

#     for res in results:
#         venue_meta = res["venue"]
#         data = res["data"]

#         city = venue_meta.get("city") or "Unknown"
#         state = format_state(venue_meta.get("state"))

#         cinema = data.get("meta", {}).get("cinema", {})
#         venue_name = (
#             cinema.get("name")
#             or venue_meta.get("name")
#             or venue_meta.get("district_name")
#             or "Unknown"
#         )
#         venue_addr = cinema.get("address") or venue_meta.get("address") or ""
#         chain = format_chain(
#             venue_meta.get("chainKey") or venue_meta.get("chain") or venue_name
#         )

#         movies = data.get("meta", {}).get("movies", []) or []
#         movie_map = {str(m.get("id")): m for m in movies}

#         for s in data.get("pageData", {}).get("sessions", []) or []:
#             movie = movie_map.get(str(s.get("mid")))
#             if not movie:
#                 continue

#             name = movie.get("name", "Unknown")
#             lang = s.get("lang") or movie.get("lang") or ""
#             fmt = (s.get("scrnFmt") or "").replace("-", " | ")

#             movie_key = f"{name} [{fmt} | {lang}]" if fmt else f"{name} | {lang}"

#             show_time = (
#                 datetime.strptime(s.get("showTime"), "%Y-%m-%dT%H:%M")
#                 .replace(tzinfo=pytz.UTC)
#                 .astimezone(IST)
#                 .strftime("%I:%M %p")
#             )

#             mins = minutes_left(show_time)
#             if mins > CUTOFF_MINUTES:
#                 continue

#             total = int(s.get("total", 0))
#             avail = int(s.get("avail", 0))
#             sold = total - avail
#             gross = sum(
#                 (a.get("sTotal", 0) - a.get("sAvail", 0)) * a.get("price", 0)
#                 for a in s.get("areas", []) or []
#             )

#             detailed.append({
#                 "movie":      movie_key,
#                 "city":       city,
#                 "state":      state,
#                 "venue":      venue_name,
#                 "address":    venue_addr,
#                 "time":       show_time,
#                 "audi":       s.get("audi", ""),
#                 "session_id": str(s.get("id", "")),
#                 "totalSeats": total,
#                 "available":  avail,
#                 "sold":       sold,
#                 "gross":      round(gross, 2),
#                 "minsLeft":   round(mins, 1),
#                 "source":     "District",
#                 "date":       DATE_CODE,
#                 "chain":      chain,
#             })

#     return detailed

# # =====================================================
# # BUILD SUMMARY (FROM FINAL DETAILED)
# # =====================================================


# def build_summary(detailed):
#     def _empty_periods():
#         return {
#             p: {"shows": 0, "sold": 0, "totalSeats": 0, "gross": 0.0}
#             for p in ("morning", "afternoon", "evening", "night")
#         }

#     summary = {}

#     for r in detailed:
#         movie = r["movie"]
#         venue = r["venue"]
#         city = r["city"]

#         total = r["totalSeats"]
#         sold = r["sold"]
#         gross = r["gross"]
#         occ = (sold / total * 100) if total else 0

#         if movie not in summary:
#             summary[movie] = {
#                 "shows":       0,
#                 "gross":       0.0,
#                 "sold":        0,
#                 "totalSeats":  0,
#                 "venues":      set(),
#                 "cities":      set(),
#                 "fastfilling": 0,
#                 "housefull":   0,
#                 "periods":     _empty_periods(),
#             }

#         m = summary[movie]
#         m["shows"] += 1
#         m["gross"] += gross
#         m["sold"] += sold
#         m["totalSeats"] += total
#         m["venues"].add(venue)
#         m["cities"].add(city)

#         if occ >= 98:
#             m["housefull"] += 1
#         elif occ >= 50:
#             m["fastfilling"] += 1

#         period = get_period(r["time"])
#         if period:
#             pd = m["periods"][period]
#             pd["shows"] += 1
#             pd["sold"] += sold
#             pd["totalSeats"] += total
#             pd["gross"] += gross

#     final = {}
#     for movie, m in summary.items():
#         periods_out = {
#             slot: {
#                 "shows":      pd["shows"],
#                 "sold":       pd["sold"],
#                 "totalSeats": pd["totalSeats"],
#                 "gross":      round(pd["gross"], 2),
#                 "occupancy":  round((pd["sold"] / pd["totalSeats"]) * 100, 2)
#                 if pd["totalSeats"] else 0.0,
#             }
#             for slot, pd in m["periods"].items()
#         }

#         final[movie] = {
#             "shows":       m["shows"],
#             "gross":       round(m["gross"], 2),
#             "sold":        m["sold"],
#             "totalSeats":  m["totalSeats"],
#             "venues":      len(m["venues"]),
#             "cities":      len(m["cities"]),
#             "fastfilling": m["fastfilling"],
#             "housefull":   m["housefull"],
#             "occupancy":   round((m["sold"] / m["totalSeats"]) * 100, 2)
#             if m["totalSeats"] else 0.0,
#             "periods":     periods_out,
#         }

#     return final

# # =====================================================
# # ENTRY
# # =====================================================


# async def main():
#     log("🚀 DISTRICT DAILY SCRAPER STARTED")
#     log(f"⚙️ concurrency={CONCURRENCY} retries={MAX_RETRIES} extra_passes={EXTRA_PASSES}")

#     results, bad_count = await fetch_all()
#     fresh = parse(results)

#     old_rows = r2_download_json(R2_DETAILED_KEY, default=[])
#     log(f"📥 Loaded {len(old_rows)} existing rows from R2")

#     old_map = {show_key(r): r for r in old_rows}
#     new_map = {}

#     for r in fresh:
#         key = show_key(r)
#         if key in old_map:
#             old_map[key].update({
#                 "totalSeats": r["totalSeats"],
#                 "available":  r["available"],
#                 "sold":       r["sold"],
#                 "gross":      r["gross"],
#                 "minsLeft":   r["minsLeft"],
#             })
#             new_map[key] = old_map[key]
#         else:
#             new_map[key] = r

#     for key, r in old_map.items():
#         if key not in new_map:
#             new_map[key] = r

#     detailed = list(new_map.values())
#     summary = build_summary(detailed)

#     r2_upload_json(R2_DETAILED_KEY, detailed)
#     r2_upload_json(R2_SUMMARY_KEY,  summary)

#     log(f"✅ DONE | Shows={len(detailed)} | Movies={len(summary)} "
#         f"| VenuesNotRefreshed={bad_count}")

#     # Make the GitHub job turn red when too many venues were blocked,
#     # instead of silently publishing stale numbers for them.
#     if FAIL_EXIT_RATIO and DIST_VENUES and bad_count / len(DIST_VENUES) > FAIL_EXIT_RATIO:
#         log(f"🔴 {bad_count}/{len(DIST_VENUES)} venues not refreshed "
#             f"(> {FAIL_EXIT_RATIO:.0%}) – exiting with error")
#         sys.exit(1)


# if __name__ == "__main__":
#     asyncio.run(main())



# bms-render/bmsdaily9.py
# Source: pre-built District Detailed JSON (venue-ID matching, no per-venue API calls)
# Source URL comes from the API_URL env variable (GitHub secret/variable)
# Storage: R2 (unchanged)
import json
import os
import hashlib
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import pytz

from r2_client import r2_upload_json, r2_download_json

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 9
DISTRICT_VENUES_FILE = "districtvenues.json"
REQUEST_TIMEOUT = 30
CUTOFF_MINUTES = 20000

# Loaded from environment (GitHub Actions secret), not hard-coded
API_URL = (os.getenv("MY_API_URL") or "").strip().rstrip("/")
if not API_URL:
    raise RuntimeError("❌ MY_API_URL env variable not set")

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)

DATE_CODE = NOW_IST.strftime("%Y%m%d")
DATE_DISTRICT = NOW_IST.strftime("%Y-%m-%d")

BASE_DIR = os.path.join("daily", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# R2 keys (unchanged)
R2_DETAILED_KEY = f"daily/{DATE_CODE}/detailed{SHARD_ID}.json"
R2_SUMMARY_KEY = f"daily/{DATE_CODE}/movie_summary{SHARD_ID}.json"

LOG_FILE = os.path.join(LOG_DIR, f"districtdaily{SHARD_ID}.log")

# =====================================================
# LOGGING
# =====================================================


def log(message):
    timestamp = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# =====================================================
# JSON
# =====================================================


def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# =====================================================
# VENUE ID
# =====================================================


def get_venue_id(venue):
    if not isinstance(venue, dict):
        return None
    value = venue.get("id") if venue.get("id") is not None else venue.get("venueId")
    if value is None:
        value = venue.get("venue_id")
    if value is None:
        value = venue.get("cinema_id")
    if value is None:
        value = venue.get("cinemaId")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return str(value).strip()

# =====================================================
# LOAD SELECTED VENUES (districtvenues.json = only venue source)
# =====================================================


def load_selected_venues():
    district_venues = load_json_file(DISTRICT_VENUES_FILE)
    if not isinstance(district_venues, list):
        raise ValueError("districtvenues.json must contain an array")

    selected_by_id = {}
    missing_ids = 0
    duplicate_ids = 0

    for venue in district_venues:
        if not isinstance(venue, dict):
            continue
        venue_id = get_venue_id(venue)
        if venue_id is None:
            missing_ids += 1
            continue
        key = str(venue_id)
        if key in selected_by_id:
            duplicate_ids += 1
        selected_by_id[key] = venue

    log(f"📍 Selected venue records: {len(district_venues)}")
    log(f"🎯 Selected venue IDs: {len(selected_by_id)}")
    if missing_ids:
        log(f"⚠️ Selected venues without ID: {missing_ids}")
    if duplicate_ids:
        log(f"⚠️ Duplicate venue IDs: {duplicate_ids}")
    return selected_by_id

# =====================================================
# FORMATTING HELPERS
# =====================================================


def format_state(value):
    if not value:
        return "Unknown"
    value = str(value)
    if value.isupper():
        return value
    return " ".join(w.capitalize() for w in value.replace("-", " ").split())


def format_chain(value):
    if not value:
        return "Unknown"
    value = str(value)
    if value.isupper():
        return value
    return " ".join(w.capitalize() for w in value.replace("-", " ").split())


def normalize_movie_name(value):
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def build_movie_key(movie_name, language):
    movie_name = normalize_movie_name(movie_name)
    language = str(language or "").strip() or "Unknown"
    return f"{movie_name} [2D | {language}]"


def generate_session_id(movie, venue_id, time, audi):
    raw = "|".join([str(movie), str(venue_id), str(time), str(audi)])
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"DISTRICT_{digest}"


def calculate_minutes_left(show_time):
    try:
        now = datetime.now(IST)
        t = datetime.strptime(show_time, "%I:%M %p")
        show_dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
        if show_dt < (now - timedelta(hours=6)):
            show_dt += timedelta(days=1)
        return (show_dt - now).total_seconds() / 60
    except Exception:
        return 9999


def get_period(show_time_str):
    """morning 00-11:59 | afternoon 12-15:59 | evening 16-19:59 | night 20-23:59"""
    try:
        hour = datetime.strptime(show_time_str, "%I:%M %p").hour
        if hour < 12:
            return "morning"
        if hour < 16:
            return "afternoon"
        if hour < 20:
            return "evening"
        return "night"
    except Exception:
        return None

# =====================================================
# FETCH SOURCE
# =====================================================


def fetch_source():
    url = f"{API_URL}/{DATE_DISTRICT}_Detailed.json"
    # URL is intentionally NOT logged (it comes from a secret env variable)
    log(f"📡 Fetching Detailed JSON for {DATE_DISTRICT}")

    request = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; BMS9/1.0)"})
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            raw = response.read()
        data = json.loads(raw.decode("utf-8"))

        if not isinstance(data, dict):
            raise ValueError("Source JSON is not an object")
        if "dicts" not in data:
            raise ValueError("Missing dicts")
        if "movies" not in data:
            raise ValueError("Missing movies")

        log("✅ Source loaded")
        log(f"   Source date: {data.get('date')}")
        log(f"   Last updated: {data.get('lastUpdated')}")
        log(f"   Movie keys: {len(data.get('movies', {}))}")
        return data

    except HTTPError as e:
        if e.code == 404:
            log("❌ Source file not found (404)")
        else:
            log(f"❌ HTTP {e.code}")
        return None
    except URLError as e:
        log(f"❌ URL error: {e.reason}")
        return None
    except Exception as e:
        log(f"❌ Source error: {type(e).__name__}: {e}")
        return None

# =====================================================
# REVERSE DICTS
# =====================================================


def reverse_dictionary(dictionary):
    if not isinstance(dictionary, dict):
        return {}
    result = {}
    for key, value in dictionary.items():
        try:
            result[int(value)] = key
        except (TypeError, ValueError):
            continue
    return result


def build_reverse_dicts(source):
    dicts = source.get("dicts", {})
    return {
        "cities": reverse_dictionary(dicts.get("cities", {})),
        "states": reverse_dictionary(dicts.get("states", {})),
        "venues": reverse_dictionary(dicts.get("venues", {})),
        "chains": reverse_dictionary(dicts.get("chains", {})),
        "showtimes": reverse_dictionary(dicts.get("showtimes", {})),
        "audis": reverse_dictionary(dicts.get("audis", {})),
    }

# =====================================================
# DECOMPRESS SHOW
# [0] cityId [1] stateId [2] venueId (REAL CINEMA ID) [3] chainId
# [4] timeId [5] audiId [6] total [7] available [8] sold
# [9] gross*100 [10] occupancy*100 [11] minsLeft
# =====================================================


def decompress_show(row, reverse):
    if not isinstance(row, list) or len(row) < 12:
        return None
    try:
        return {
            "city": reverse["cities"].get(row[0], "Unknown"),
            "state": reverse["states"].get(row[1], "Unknown"),
            "venue": reverse["venues"].get(row[2], "Unknown"),
            "venue_id": row[2],
            "chain": reverse["chains"].get(row[3], "Unknown"),
            "time": reverse["showtimes"].get(row[4], ""),
            "audi": reverse["audis"].get(row[5], ""),
            "totalSeats": int(row[6] or 0),
            "available": int(row[7] or 0),
            "sold": int(row[8] or 0),
            "gross": int(row[9] or 0) / 100,
            "occupancy": int(row[10] or 0) / 100,
            "minsLeft": float(row[11] or 0),
        }
    except Exception:
        return None

# =====================================================
# PARSE SOURCE (match by venue ID only)
# =====================================================


def parse_source(source, selected_venues, reverse):
    detailed = []
    movies = source.get("movies", {})

    matched_rows = 0
    ignored_rows = 0
    cutoff_dropped = 0
    missing_source_ids = set()

    for raw_movie_key, rows in movies.items():
        if not isinstance(rows, list):
            continue

        # Source movie key format: "Movie | Language"
        if "|" in raw_movie_key:
            parts = [p.strip() for p in raw_movie_key.split("|")]
            movie_name = parts[0] if parts else raw_movie_key
            language = parts[-1] if len(parts) > 1 else "Unknown"
        else:
            movie_name = raw_movie_key.strip()
            language = "Unknown"

        movie_key = build_movie_key(movie_name, language)

        for compressed in rows:
            show = decompress_show(compressed, reverse)
            if not show:
                continue

            source_venue_id = show["venue_id"]
            selected_venue = selected_venues.get(str(source_venue_id))

            if selected_venue is None:
                ignored_rows += 1
                missing_source_ids.add(str(source_venue_id))
                continue

            matched_rows += 1

            # Venue data comes from districtvenues.json
            city = selected_venue.get("city") or "Unknown"
            state = format_state(selected_venue.get("state"))
            venue = str(
                selected_venue.get("name", show.get("venue", "Unknown Venue"))
                or "Unknown Venue"
            ).strip()
            address = str(selected_venue.get("address", "") or "")
            chain = format_chain(selected_venue.get("chainKey"))

            time = str(show.get("time", "") or "").strip()
            audi = str(show.get("audi", "") or "")

            mins_left = calculate_minutes_left(time)
            if mins_left > CUTOFF_MINUTES:
                cutoff_dropped += 1
                continue

            total = int(show.get("totalSeats", 0) or 0)
            available = int(show.get("available", 0) or 0)
            sold = max(total - available, 0)
            gross = float(show.get("gross", 0) or 0)

            session_id = generate_session_id(movie_key, source_venue_id, time, audi)

            detailed.append({
                "movie": movie_key,
                "city": city,
                "state": state,
                "venue": venue,
                "venue_id": source_venue_id,
                "address": address,
                "time": time,
                "audi": audi,
                "session_id": session_id,
                "totalSeats": total,
                "available": available,
                "sold": sold,
                "gross": round(gross, 2),
                "minsLeft": round(mins_left, 1),
                "source": "District",
                "date": DATE_CODE,
                "chain": chain,
            })

    log(f"🎬 Source movie keys: {len(movies)}")
    log(f"🎟️ Selected-venue show rows: {matched_rows}")
    log(f"🚫 Non-selected show rows ignored: {ignored_rows}")
    log(f"⏱️ Dropped by cutoff: {cutoff_dropped}")
    if missing_source_ids:
        log(f"ℹ️ Non-selected venue IDs encountered: {len(missing_source_ids)}")

    return detailed

# =====================================================
# SHOW KEY (venue-ID based)
# =====================================================


def show_key(row):
    return (
        str(row.get("venue_id", "")),
        row.get("time"),
        row.get("session_id"),
        row.get("audi"),
    )

# =====================================================
# MERGE WITHOUT DELETING (old rows come from R2)
# =====================================================


def merge_without_deleting(fresh, old_rows):
    old_map = {show_key(r): r for r in old_rows if isinstance(r, dict)}
    new_map = {}

    for row in fresh:
        key = show_key(row)
        if key in old_map:
            old_map[key].update({
                "totalSeats": row["totalSeats"],
                "available": row["available"],
                "sold": row["sold"],
                "gross": row["gross"],
                "minsLeft": row["minsLeft"],
            })
            old_map[key]["venue_id"] = row.get("venue_id")
            new_map[key] = old_map[key]
        else:
            new_map[key] = row

    for key, row in old_map.items():
        if key not in new_map:
            new_map[key] = row

    merged = list(new_map.values())
    log(f"📦 Previous shows: {len(old_rows)}")
    log(f"🆕 Fresh shows: {len(fresh)}")
    log(f"📦 Final shows: {len(merged)}")
    return merged

# =====================================================
# BUILD SUMMARY
# =====================================================


def build_summary(detailed):
    def _empty_periods():
        return {
            p: {"shows": 0, "sold": 0, "totalSeats": 0, "gross": 0.0}
            for p in ("morning", "afternoon", "evening", "night")
        }

    summary = {}

    for r in detailed:
        movie = r.get("movie", "Unknown")
        city = r.get("city", "Unknown")
        venue_id = r.get("venue_id")
        total = int(r.get("totalSeats", 0) or 0)
        sold = int(r.get("sold", 0) or 0)
        gross = float(r.get("gross", 0) or 0)
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
                "periods": _empty_periods(),
            }

        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        if venue_id is not None:
            m["venues"].add(str(venue_id))
        m["cities"].add(city)

        if occ >= 98:
            m["housefull"] += 1
        elif occ >= 50:
            m["fastfilling"] += 1

        period = get_period(r.get("time", ""))
        if period:
            pd = m["periods"][period]
            pd["shows"] += 1
            pd["sold"] += sold
            pd["totalSeats"] += total
            pd["gross"] += gross

    final = {}
    for movie, m in summary.items():
        periods_out = {
            slot: {
                "shows": pd["shows"],
                "sold": pd["sold"],
                "totalSeats": pd["totalSeats"],
                "gross": round(pd["gross"], 2),
                "occupancy": round((pd["sold"] / pd["totalSeats"]) * 100, 2)
                if pd["totalSeats"] else 0.0,
            }
            for slot, pd in m["periods"].items()
        }
        final[movie] = {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2)
            if m["totalSeats"] else 0.0,
            "periods": periods_out,
        }
    return final

# =====================================================
# MAIN
# =====================================================


def main():
    log("🚀 DISTRICT → BMS9 CONVERTER STARTED")
    log(f"📅 Date: {DATE_DISTRICT}")
    log("🔑 Venue matching: ID ONLY")

    selected_venues = load_selected_venues()

    source = fetch_source()
    if source is None:
        # Nothing fetched: leave R2 untouched so existing data is preserved
        log("⚠️ No source data – R2 files left unchanged")
        log("✅ DONE | Existing data preserved")
        return

    reverse = build_reverse_dicts(source)
    fresh = parse_source(source, selected_venues, reverse)

    # ---- R2 logic (unchanged) ----
    old_rows = r2_download_json(R2_DETAILED_KEY, default=[])
    log(f"📥 Loaded {len(old_rows)} existing rows from R2")

    detailed = merge_without_deleting(fresh, old_rows)
    summary = build_summary(detailed)

    r2_upload_json(R2_DETAILED_KEY, detailed)
    r2_upload_json(R2_SUMMARY_KEY, summary)
    # ------------------------------

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(summary)}")


if __name__ == "__main__":
    main()
