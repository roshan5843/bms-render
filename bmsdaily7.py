#
# # import json
# # import os
# # import random
# # import time
# # import threading
# # import signal
# # from datetime import datetime, timedelta, timezone
#
# # import cloudscraper
#
# # from r2_client import r2_upload_json, r2_download_json
#
# # # =====================================================
# # # CONFIG
# # # =====================================================
# # SHARD_ID = 7          # ← change this per shard file (1 through 8)
# # API_TIMEOUT = 12
# # HARD_TIMEOUT = 15
#
# # CUTOFF_MINUTES = 200  # ≈ 3h 20m
#
# # IST = timezone(timedelta(hours=5, minutes=30))
#
# # DATE_CODE = datetime.now(IST).strftime("%Y%m%d")
#
# # BASE_DIR = os.path.join("daily", "data", DATE_CODE)
# # LOG_DIR = os.path.join(BASE_DIR, "logs")
# # os.makedirs(LOG_DIR, exist_ok=True)
#
# # R2_DETAILED_KEY = f"daily/{DATE_CODE}/detailed{SHARD_ID}.json"
# # R2_SUMMARY_KEY = f"daily/{DATE_CODE}/movie_summary{SHARD_ID}.json"
#
# # DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
# # SUMMARY_FILE = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
# # LOG_FILE = f"{LOG_DIR}/bmsdaily{SHARD_ID}.log"
#
# # # =====================================================
# # # LOGGING
# # # =====================================================
#
#
# # def log(msg):
# #     ts = datetime.now(IST).strftime("%H:%M:%S")
# #     line = f"[{ts}] {msg}"
# #     print(line, flush=True)
# #     with open(LOG_FILE, "a", encoding="utf-8") as f:
# #         f.write(line + "\n")
#
# # # =====================================================
# # # HARD TIMEOUT
# # # =====================================================
#
#
# # class TimeoutError(Exception):
# #     pass
#
#
# # def _timeout_handler(signum, frame):
# #     raise TimeoutError("Hard timeout hit")
#
#
# # def hard_timeout(seconds):
# #     def deco(fn):
# #         def wrapper(*args, **kwargs):
# #             signal.signal(signal.SIGALRM, _timeout_handler)
# #             signal.alarm(seconds)
# #             try:
# #                 return fn(*args, **kwargs)
# #             finally:
# #                 signal.alarm(0)
# #         return wrapper
# #     return deco
#
#
# # # =====================================================
# # # IDENTITY / UA ROTATION
# # # =====================================================
# # USER_AGENTS = [
# #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36",
# #     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/119 Safari/537.36",
# #     "Mozilla/5.0 (X11; Linux x86_64) Chrome/118 Safari/537.36",
# # ]
#
# # thread_local = threading.local()
#
#
# # class Identity:
# #     def __init__(self):
# #         self.ua = random.choice(USER_AGENTS)
# #         self.ip = ".".join(str(random.randint(20, 230)) for _ in range(4))
# #         self.scraper = cloudscraper.create_scraper(
# #             browser={"browser": "chrome",
# #                      "platform": "windows", "desktop": True}
# #         )
#
# #     def headers(self):
# #         return {
# #             "User-Agent":      self.ua,
# #             "Accept":          "application/json, text/plain, */*",
# #             "Accept-Language": "en-IN,en;q=0.9",
# #             "Origin":          "https://in.bookmyshow.com",
# #             "Referer":         "https://in.bookmyshow.com/",
# #             "X-Forwarded-For": self.ip,
# #         }
#
#
# # def get_identity():
# #     if not hasattr(thread_local, "identity"):
# #         thread_local.identity = Identity()
# #         log("🧠 New identity")
# #     return thread_local.identity
#
#
# # def reset_identity():
# #     if hasattr(thread_local, "identity"):
# #         del thread_local.identity
# #     log("🔄 Identity reset")
#
# # # =====================================================
# # # FETCH API
# # # =====================================================
#
#
# # @hard_timeout(HARD_TIMEOUT)
# # def fetch_api_raw(venue_code):
# #     ident = get_identity()
# #     url = (
# #         "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
# #         f"?venueCode={venue_code}&dateCode={DATE_CODE}"
# #     )
# #     r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)
# #     if not r.text.strip().startswith("{"):
# #         raise RuntimeError("Blocked / HTML")
# #     return r.json()
#
# # # =====================================================
# # # TIME HELPERS
# # # =====================================================
#
#
# # def minutes_left(show_time_str):
# #     """Convert 'hh:mm AM/PM' to minutes left from now (IST)."""
# #     try:
# #         now = datetime.now(IST)
# #         t = datetime.strptime(show_time_str, "%I:%M %p")
# #         t = t.replace(year=now.year, month=now.month, day=now.day, tzinfo=IST)
# #         return (t - now).total_seconds() / 60
# #     except Exception:
# #         return 9999
#
#
# # # ─── NEW: classify show start time into one of four periods ──────────────────
# # def get_period(show_time_str):
# #     """
# #     Returns 'morning' | 'afternoon' | 'evening' | 'night' based on show start hour.
#
# #     Boundaries:
# #       Morning   00:00 – 11:59   (hour < 12)
# #       Afternoon 12:00 – 15:59   (12 ≤ hour < 16)
# #       Evening   16:00 – 19:59   (16 ≤ hour < 20)
# #       Night     20:00 – 23:59   (hour ≥ 20)
# #     """
# #     try:
# #         hour = datetime.strptime(show_time_str, "%I:%M %p").hour
# #         if hour < 12:
# #             return "morning"
# #         if hour < 16:
# #             return "afternoon"
# #         if hour < 20:
# #             return "evening"
# #         return "night"
# #     except Exception:
# #         return None
#
# # # =====================================================
# # # PARSER
# # # =====================================================
#
#
# # def parse_payload(data):
# #     out = []
#
# #     sd = data.get("ShowDetails", [])
# #     if not sd:
# #         return out
#
# #     venue = sd[0].get("Venues", {})
# #     venue_name = venue.get("VenueName", "")
# #     venue_add = venue.get("VenueAdd", "")
# #     chain = venue.get("VenueCompName", "Unknown")
#
# #     for ev in sd[0].get("Event", []):
# #         title = ev.get("EventTitle", "Unknown")
#
# #         for ch in ev.get("ChildEvents", []):
# #             dim = ch.get("EventDimension", "").strip()
# #             lang = ch.get("EventLanguage", "").strip()
# #             suffix = " | ".join(x for x in (dim, lang) if x)
# #             movie = f"{title} [{suffix}]" if suffix else title
#
# #             for sh in ch.get("ShowTimes", []):
# #                 if sh.get("ShowDateCode") != DATE_CODE:
# #                     continue
#
# #                 total = sold = avail = gross = 0
# #                 for cat in sh.get("Categories", []):
# #                     seats = int(cat.get("MaxSeats", 0))
# #                     free = int(cat.get("SeatsAvail", 0))
# #                     price = float(cat.get("CurPrice", 0))
# #                     total += seats
# #                     avail += free
# #                     sold += seats - free
# #                     gross += (seats - free) * price
#
# #                 out.append({
# #                     "movie":      movie,
# #                     "venue":      venue_name,
# #                     "address":    venue_add,
# #                     "chain":      chain,
# #                     "time":       sh.get("ShowTime", ""),
# #                     "audi":       sh.get("Attributes", "") or "",
# #                     "session_id": str(sh.get("SessionId", "")),
# #                     "totalSeats": total,
# #                     "available":  avail,
# #                     "sold":       sold,
# #                     "gross":      round(gross, 2),
# #                 })
#
# #     return out
#
# # # =====================================================
# # # STABLE SHOW KEY
# # # =====================================================
#
#
# # def show_key(r):
# #     return (r["venue"], r["time"], r["session_id"], r["audi"])
#
#
# # # =====================================================
# # # MAIN
# # # =====================================================
# # if __name__ == "__main__":
# #     log("🚀 BMS DAILY TRACKER STARTED")
#
# #     with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
# #         venues = json.load(f)
#
# #     fetched = []
#
# #     for i, vcode in enumerate(venues, 1):
# #         log(f"[{i}/{len(venues)}] {vcode}")
# #         try:
# #             raw = fetch_api_raw(vcode)
#
# #             for r in parse_payload(raw):
# #                 mins = minutes_left(r["time"])
#
# #                 if mins <= CUTOFF_MINUTES:
# #                     r["minsLeft"] = round(mins, 1)
# #                     r["city"] = venues[vcode].get("City", "Unknown")
# #                     r["state"] = venues[vcode].get("State", "Unknown")
# #                     r["source"] = "BMS"
# #                     r["date"] = DATE_CODE
# #                     fetched.append(r)
#
# #         except Exception as e:
# #             reset_identity()
# #             log(f"❌ {vcode} | {type(e).__name__}")
#
# #         time.sleep(random.uniform(0.35, 0.7))
#
# #     # =====================================================
# #     # LOAD OLD DETAILED FROM R2
# #     # =====================================================
# #     old_rows = r2_download_json(R2_DETAILED_KEY, default=[])
# #     print(f"📥 Loaded {len(old_rows)} existing rows from R2")
#
# #     old_map = {show_key(r): r for r in old_rows}
# #     new_map = {}
#
# #     for r in fetched:
# #         key = show_key(r)
# #         if key in old_map:
# #             old_map[key].update({
# #                 "totalSeats": r["totalSeats"],
# #                 "available":  r["available"],
# #                 "sold":       r["sold"],
# #                 "gross":      r["gross"],
# #                 "minsLeft":   r.get("minsLeft"),
# #             })
# #             new_map[key] = old_map[key]
# #         else:
# #             new_map[key] = r
#
# #     for key, r in old_map.items():
# #         if key not in new_map:
# #             new_map[key] = r
#
# #     detailed = list(new_map.values())
#
# #     # =====================================================
# #     # SUMMARY (REBUILT FROM DETAILED)
# #     # =====================================================
# #     # Period slot template — reused for every new movie entry
# #     def _empty_periods():
# #         return {
# #             p: {"shows": 0, "sold": 0, "totalSeats": 0, "gross": 0.0}
# #             for p in ("morning", "afternoon", "evening", "night")
# #         }
#
# #     summary = {}
#
# #     for r in detailed:
# #         movie = r["movie"]
# #         venue = r["venue"]
# #         city = r["city"]
#
# #         total = r["totalSeats"]
# #         sold = r["sold"]
# #         gross = r["gross"]
# #         occ = (sold / total * 100) if total else 0
#
# #         if movie not in summary:
# #             summary[movie] = {
# #                 "shows":       0,
# #                 "gross":       0.0,
# #                 "sold":        0,
# #                 "totalSeats":  0,
# #                 "venues":      set(),
# #                 "cities":      set(),
# #                 "fastfilling": 0,
# #                 "housefull":   0,
# #                 # ── NEW: per-period accumulators ──────────────────────────
# #                 "periods":     _empty_periods(),
# #             }
#
# #         m = summary[movie]
# #         m["shows"] += 1
# #         m["gross"] += gross
# #         m["sold"] += sold
# #         m["totalSeats"] += total
# #         m["venues"].add(venue)
# #         m["cities"].add(city)
#
# #         if occ >= 98:
# #             m["housefull"] += 1
# #         elif occ >= 50:
# #             m["fastfilling"] += 1
#
# #         # ── NEW: accumulate into the correct time-slot bucket ─────────────
# #         period = get_period(r["time"])
# #         if period:
# #             pd = m["periods"][period]
# #             pd["shows"] += 1
# #             pd["sold"] += sold
# #             pd["totalSeats"] += total
# #             pd["gross"] += gross
#
# #     # ─── Finalise summary (add computed occupancy to each period slot) ────
# #     final_summary = {}
# #     for movie, m in summary.items():
# #         # Compute per-period occupancy from accumulated totals
# #         periods_out = {
# #             slot: {
# #                 "shows":      pd["shows"],
# #                 "sold":       pd["sold"],
# #                 "totalSeats": pd["totalSeats"],
# #                 "gross":      round(pd["gross"], 2),
# #                 "occupancy":  round((pd["sold"] / pd["totalSeats"]) * 100, 2)
# #                 if pd["totalSeats"] else 0.0,
# #             }
# #             for slot, pd in m["periods"].items()
# #         }
#
# #         final_summary[movie] = {
# #             "shows":       m["shows"],
# #             "gross":       round(m["gross"], 2),
# #             "sold":        m["sold"],
# #             "totalSeats":  m["totalSeats"],
# #             "venues":      len(m["venues"]),
# #             "cities":      len(m["cities"]),
# #             "fastfilling": m["fastfilling"],
# #             "housefull":   m["housefull"],
# #             "occupancy":   round((m["sold"] / m["totalSeats"]) * 100, 2)
# #             if m["totalSeats"] else 0.0,
# #             # ── NEW ──────────────────────────────────────────────────────
# #             "periods":     periods_out,
# #         }
#
# #     # =====================================================
# #     # SAVE TO R2
# #     # =====================================================
# #     r2_upload_json(R2_DETAILED_KEY, detailed)
# #     r2_upload_json(R2_SUMMARY_KEY,  final_summary)
#
# #     log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")
#
#
# # bms-render/bmsdaily7.py
# import json
# import os
# import random
# import time
# import threading
# import signal
# from datetime import datetime, timedelta, timezone
#
# import cloudscraper
#
# from r2_client import r2_upload_json, r2_download_json
#
# # =====================================================
# # CONFIG
# # =====================================================
# SHARD_ID = 7          # ← change this per shard file (1 through 8)
# API_TIMEOUT = 12
# HARD_TIMEOUT = 15
# MAX_RECOVERY_ROUNDS = 5          # max retries per venue
# IDENTITY_ROTATE_EVERY = 10       # new identity after every N requests
# FAILURE_THRESHOLD = 5            # consecutive failures before cooldown
# COOLDOWN_SECONDS = 30            # pause when too many failures
#
# CUTOFF_MINUTES = 200  # ≈ 3h 20m
#
# IST = timezone(timedelta(hours=5, minutes=30))
#
# DATE_CODE = datetime.now(IST).strftime("%Y%m%d")
#
# BASE_DIR = os.path.join("daily", "data", DATE_CODE)
# LOG_DIR = os.path.join(BASE_DIR, "logs")
# os.makedirs(LOG_DIR, exist_ok=True)
#
# R2_DETAILED_KEY = f"daily/{DATE_CODE}/detailed{SHARD_ID}.json"
# R2_SUMMARY_KEY = f"daily/{DATE_CODE}/movie_summary{SHARD_ID}.json"
#
# DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
# SUMMARY_FILE = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
# LOG_FILE = f"{LOG_DIR}/bmsdaily{SHARD_ID}.log"
#
# # =====================================================
# # LOGGING
# # =====================================================
#
#
# def log(msg):
#     ts = datetime.now(IST).strftime("%H:%M:%S")
#     line = f"[{ts}] {msg}"
#     print(line, flush=True)
#     with open(LOG_FILE, "a", encoding="utf-8") as f:
#         f.write(line + "\n")
#
# # =====================================================
# # HARD TIMEOUT
# # =====================================================
#
#
# class TimeoutError(Exception):
#     pass
#
#
# def _timeout_handler(signum, frame):
#     raise TimeoutError("Hard timeout hit")
#
#
# def hard_timeout(seconds):
#     def deco(fn):
#         def wrapper(*args, **kwargs):
#             signal.signal(signal.SIGALRM, _timeout_handler)
#             signal.alarm(seconds)
#             try:
#                 return fn(*args, **kwargs)
#             finally:
#                 signal.alarm(0)
#         return wrapper
#     return deco
#
#
# # =====================================================
# # IDENTITY / UA ROTATION
# # =====================================================
# USER_AGENTS = [
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
#     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
#     "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
# ]
#
# thread_local = threading.local()
# request_counter = 0
#
#
# class Identity:
#     def __init__(self):
#         self.ua = random.choice(USER_AGENTS)
#         self.ip = ".".join(str(random.randint(20, 230)) for _ in range(4))
#         self.scraper = cloudscraper.create_scraper(
#             browser={"browser": "chrome",
#                      "platform": "windows", "desktop": True}
#         )
#
#     def headers(self):
#         # Full set of browser-like headers
#         return {
#             "User-Agent":      self.ua,
#             "Accept":          "application/json, text/plain, */*",
#             "Accept-Language": "en-IN,en;q=0.9",
#             "Accept-Encoding": "gzip, deflate, br",
#             "Connection":      "keep-alive",
#             "Sec-Fetch-Dest":  "empty",
#             "Sec-Fetch-Mode":  "cors",
#             "Sec-Fetch-Site":  "same-origin",
#             "Origin":          "https://in.bookmyshow.com",
#             "Referer":         "https://in.bookmyshow.com/",
#             "Cache-Control":   "no-cache",
#             "Pragma":          "no-cache",
#             "X-Forwarded-For": self.ip,
#         }
#
#
# def get_identity():
#     global request_counter
#     if not hasattr(thread_local, "identity"):
#         thread_local.identity = Identity()
#         log("🧠 New identity")
#     request_counter += 1
#     # Rotate identity proactively every N requests
#     if request_counter % IDENTITY_ROTATE_EVERY == 0:
#         reset_identity()
#         thread_local.identity = Identity()
#         log(f"🔄 Identity rotated (every {IDENTITY_ROTATE_EVERY} requests)")
#     return thread_local.identity
#
#
# def reset_identity():
#     if hasattr(thread_local, "identity"):
#         del thread_local.identity
#     log("🔄 Identity reset")
#
# # =====================================================
# # FETCH API (with retry and block detection)
# # =====================================================
#
#
# @hard_timeout(HARD_TIMEOUT)
# def fetch_api_raw(venue_code):
#     ident = get_identity()
#     url = (
#         "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
#         f"?venueCode={venue_code}&dateCode={DATE_CODE}"
#     )
#     r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)
#     if r.status_code != 200:
#         raise RuntimeError(f"HTTP {r.status_code}")
#     text = r.text.strip()
#     if not text.startswith("{"):
#         snippet = text[:200].replace("\n", " ")
#         raise RuntimeError(f"Blocked / HTML (snippet: {snippet})")
#     return r.json()
#
#
# def fetch_with_retry(venue_code, attempt=1):
#     try:
#         return fetch_api_raw(venue_code)
#     except Exception as e:
#         if attempt >= MAX_RECOVERY_ROUNDS:
#             log(f"❌ {venue_code} failed after {MAX_RECOVERY_ROUNDS} attempts")
#             raise
#         delay = (2 ** attempt) + random.uniform(0, 1)   # exponential backoff
#         log(f"🔄 Retry {attempt}/{MAX_RECOVERY_ROUNDS} for {venue_code} in {delay:.1f}s")
#         reset_identity()
#         time.sleep(delay)
#         return fetch_with_retry(venue_code, attempt + 1)
#
# # =====================================================
# # TIME HELPERS
# # =====================================================
#
#
# def minutes_left(show_time_str):
#     """Convert 'hh:mm AM/PM' to minutes left from now (IST)."""
#     try:
#         now = datetime.now(IST)
#         t = datetime.strptime(show_time_str, "%I:%M %p")
#         t = t.replace(year=now.year, month=now.month, day=now.day, tzinfo=IST)
#         return (t - now).total_seconds() / 60
#     except Exception:
#         return 9999
#
#
# # ─── NEW: classify show start time into one of four periods ──────────────────
# def get_period(show_time_str):
#     """
#     Returns 'morning' | 'afternoon' | 'evening' | 'night' based on show start hour.
#
#     Boundaries:
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
#
# # =====================================================
# # PARSER
# # =====================================================
#
#
# def parse_payload(data):
#     out = []
#
#     sd = data.get("ShowDetails", [])
#     if not sd:
#         return out
#
#     venue = sd[0].get("Venues", {})
#     venue_name = venue.get("VenueName", "")
#     venue_add = venue.get("VenueAdd", "")
#     chain = venue.get("VenueCompName", "Unknown")
#
#     for ev in sd[0].get("Event", []):
#         title = ev.get("EventTitle", "Unknown")
#
#         for ch in ev.get("ChildEvents", []):
#             dim = ch.get("EventDimension", "").strip()
#             lang = ch.get("EventLanguage", "").strip()
#             suffix = " | ".join(x for x in (dim, lang) if x)
#             movie = f"{title} [{suffix}]" if suffix else title
#
#             for sh in ch.get("ShowTimes", []):
#                 if sh.get("ShowDateCode") != DATE_CODE:
#                     continue
#
#                 total = sold = avail = gross = 0
#                 for cat in sh.get("Categories", []):
#                     seats = int(cat.get("MaxSeats", 0))
#                     free = int(cat.get("SeatsAvail", 0))
#                     price = float(cat.get("CurPrice", 0))
#                     total += seats
#                     avail += free
#                     sold += seats - free
#                     gross += (seats - free) * price
#
#                 out.append({
#                     "movie":      movie,
#                     "venue":      venue_name,
#                     "address":    venue_add,
#                     "chain":      chain,
#                     "time":       sh.get("ShowTime", ""),
#                     "audi":       sh.get("Attributes", "") or "",
#                     "session_id": str(sh.get("SessionId", "")),
#                     "totalSeats": total,
#                     "available":  avail,
#                     "sold":       sold,
#                     "gross":      round(gross, 2),
#                 })
#
#     return out
#
# # =====================================================
# # STABLE SHOW KEY
# # =====================================================
#
#
# def show_key(r):
#     return (r["venue"], r["time"], r["session_id"], r["audi"])
#
#
# # =====================================================
# # MAIN
# # =====================================================
# if __name__ == "__main__":
#     log("🚀 BMS DAILY TRACKER STARTED")
#
#     with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
#         venues = json.load(f)
#
#     fetched = []
#     failure_count = 0
#
#     for i, vcode in enumerate(venues, 1):
#         log(f"[{i}/{len(venues)}] {vcode}")
#         try:
#             raw = fetch_with_retry(vcode)    # retry wrapper
#
#             for r in parse_payload(raw):
#                 mins = minutes_left(r["time"])
#
#                 if mins <= CUTOFF_MINUTES:
#                     r["minsLeft"] = round(mins, 1)
#                     r["city"] = venues[vcode].get("City", "Unknown")
#                     r["state"] = venues[vcode].get("State", "Unknown")
#                     r["source"] = "BMS"
#                     r["date"] = DATE_CODE
#                     fetched.append(r)
#
#             failure_count = 0   # reset on success
#
#         except Exception as e:
#             failure_count += 1
#             if failure_count >= FAILURE_THRESHOLD:
#                 log(f"⛔ {failure_count} consecutive failures – cooling down {COOLDOWN_SECONDS}s")
#                 time.sleep(COOLDOWN_SECONDS)
#                 failure_count = 0
#             reset_identity()
#             log(f"❌ {vcode} final failure | {type(e).__name__} | {str(e)[:100]}")
#
#         # Increased delay with jitter
#         time.sleep(random.uniform(0.45, 0.8))
#
#     # =====================================================
#     # LOAD OLD DETAILED FROM R2
#     # =====================================================
#     old_rows = r2_download_json(R2_DETAILED_KEY, default=[])
#     print(f"📥 Loaded {len(old_rows)} existing rows from R2")
#
#     old_map = {show_key(r): r for r in old_rows}
#     new_map = {}
#
#     for r in fetched:
#         key = show_key(r)
#         if key in old_map:
#             old_map[key].update({
#                 "totalSeats": r["totalSeats"],
#                 "available":  r["available"],
#                 "sold":       r["sold"],
#                 "gross":      r["gross"],
#                 "minsLeft":   r.get("minsLeft"),
#             })
#             new_map[key] = old_map[key]
#         else:
#             new_map[key] = r
#
#     for key, r in old_map.items():
#         if key not in new_map:
#             new_map[key] = r
#
#     detailed = list(new_map.values())
#
#     # =====================================================
#     # SUMMARY (REBUILT FROM DETAILED)
#     # =====================================================
#     # Period slot template — reused for every new movie entry
#     def _empty_periods():
#         return {
#             p: {"shows": 0, "sold": 0, "totalSeats": 0, "gross": 0.0}
#             for p in ("morning", "afternoon", "evening", "night")
#         }
#
#     summary = {}
#
#     for r in detailed:
#         movie = r["movie"]
#         venue = r["venue"]
#         city = r["city"]
#
#         total = r["totalSeats"]
#         sold = r["sold"]
#         gross = r["gross"]
#         occ = (sold / total * 100) if total else 0
#
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
#
#         m = summary[movie]
#         m["shows"] += 1
#         m["gross"] += gross
#         m["sold"] += sold
#         m["totalSeats"] += total
#         m["venues"].add(venue)
#         m["cities"].add(city)
#
#         if occ >= 98:
#             m["housefull"] += 1
#         elif occ >= 50:
#             m["fastfilling"] += 1
#
#         # ── NEW: accumulate into the correct time-slot bucket ─────────────
#         period = get_period(r["time"])
#         if period:
#             pd = m["periods"][period]
#             pd["shows"] += 1
#             pd["sold"] += sold
#             pd["totalSeats"] += total
#             pd["gross"] += gross
#
#     # ─── Finalise summary (add computed occupancy to each period slot) ────
#     final_summary = {}
#     for movie, m in summary.items():
#         # Compute per-period occupancy from accumulated totals
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
#
#         final_summary[movie] = {
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
#
#     # =====================================================
#     # SAVE TO R2
#     # =====================================================
#     r2_upload_json(R2_DETAILED_KEY, detailed)
#     r2_upload_json(R2_SUMMARY_KEY,  final_summary)
#
#     log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")


# bmsdaily7.py
import json
import os
import random
import time
import threading
import signal
from datetime import datetime, timedelta, timezone

# Prefer curl_cffi (real Chrome TLS/HTTP2 fingerprint). Fall back to cloudscraper.
try:
    from curl_cffi import requests as cffi_requests
    HAVE_CFFI = True
except ImportError:
    import cloudscraper
    HAVE_CFFI = False

from r2_client import r2_upload_json, r2_download_json

SHARD_ID = 7
API_TIMEOUT = 12
HARD_TIMEOUT = 20
# attempts per venue per pass (single retry layer)
MAX_ATTEMPTS = 3
FINAL_RETRY_ROUNDS = 2            # extra passes over failed venues at the end
# rotate a warmed-up identity after N requests (per identity)
IDENTITY_ROTATE_EVERY = 40
FAILURE_THRESHOLD = 5             # consecutive failed venues before cooldown
COOLDOWN_SECONDS = 90
# still blocked after this many cooldowns -> IP is blocked, stop
MAX_COOLDOWNS_IN_A_ROW = 3
BLOCK_BACKOFF = 6                 # base backoff (s) on 403/429
BASE_DELAY = 0.45
MAX_DELAY = 1.75
CUTOFF_MINUTES = 210              # only shows within ~3h30m are kept

# Optional proxy (strongly recommended on GitHub Actions / any datacenter IP).
# e.g. BMS_PROXY="http://user:pass@host:port" (an Indian residential/ISP proxy works best)
PROXY_URL = os.environ.get("BMS_PROXY", "").strip()
PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

HOME_URL = "https://in.bookmyshow.com/"
IMPERSONATE_TARGETS = ["chrome120", "chrome124"]

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = datetime.now(IST).strftime("%Y%m%d")
BASE_DIR = os.path.join("daily", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Data lives in R2 (not in the repo). Only logs are written locally.
R2_DETAILED_KEY = f"daily/{DATE_CODE}/detailed{SHARD_ID}.json"
R2_SUMMARY_KEY = f"daily/{DATE_CODE}/movie_summary{SHARD_ID}.json"
LOG_FILE = f"{LOG_DIR}/bmsdaily{SHARD_ID}.log"


def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


class HardTimeout(Exception):
    pass


class BlockedError(RuntimeError):
    """403 / 429 / HTML challenge page: we are being blocked."""


def _timeout_handler(signum, frame):
    raise HardTimeout("Hard timeout hit")


def hard_timeout(seconds):
    def deco(fn):
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(seconds)
            try:
                return fn(*args, **kwargs)
            finally:
                signal.alarm(0)
        return wrapper
    return deco


thread_local = threading.local()


class Identity:
    """One browser-like session. UA/TLS fingerprint come from the library so they
    always match each other (a random UA on top of a Chrome TLS fingerprint is
    what gets flagged). Cookies are collected via a warm-up request."""

    def __init__(self):
        self.requests_made = 0
        if HAVE_CFFI:
            self.session = cffi_requests.Session(
                impersonate=random.choice(IMPERSONATE_TARGETS),
                proxies=PROXIES,
            )
        else:
            self.session = cloudscraper.create_scraper(
                browser={"browser": "chrome",
                         "platform": "windows", "desktop": True}
            )
            if PROXIES:
                self.session.proxies.update(PROXIES)
        self.session.headers.update({"Accept-Language": "en-IN,en;q=0.9"})
        self._warm_up()

    def _warm_up(self):
        try:
            r = self.session.get(HOME_URL, timeout=API_TIMEOUT)
            log(f"🧠 New identity created (warm-up HTTP {r.status_code})")
        except Exception as e:
            log(f"🧠 New identity created (warm-up failed: {type(e).__name__})")

    def api_headers(self):
        # Do NOT set User-Agent, Accept-Encoding, Sec-* or X-Forwarded-For here.
        return {
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://in.bookmyshow.com",
            "Referer": HOME_URL,
        }


def get_identity():
    ident = getattr(thread_local, "identity", None)
    if ident is not None and ident.requests_made >= IDENTITY_ROTATE_EVERY:
        log(f"🔄 Identity rotated (every {IDENTITY_ROTATE_EVERY} requests)")
        reset_identity()
        ident = None
    if ident is None:
        ident = Identity()
        thread_local.identity = ident
    ident.requests_made += 1
    return ident


def reset_identity():
    ident = getattr(thread_local, "identity", None)
    if ident is not None:
        try:
            ident.session.close()
        except Exception:
            pass
        del thread_local.identity


@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(venue_code):
    ident = get_identity()
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    r = ident.session.get(
        url, headers=ident.api_headers(), timeout=API_TIMEOUT)
    if r.status_code in (403, 429):
        raise BlockedError(f"HTTP {r.status_code}")
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    text = r.text.strip()
    if not text.startswith("{"):
        snippet = text[:200].replace("\n", " ")
        raise BlockedError(f"Blocked / HTML (snippet: {snippet})")
    return r.json()


def fetch_with_retry(venue_code, attempts=MAX_ATTEMPTS):
    """Single retry layer. Blocks get a longer backoff and a fresh identity."""
    for attempt in range(1, attempts + 1):
        try:
            return fetch_api_raw(venue_code)
        except Exception as e:
            blocked = isinstance(e, BlockedError)
            reset_identity()
            if attempt == attempts:
                log(f"❌ {venue_code} failed after {attempts} attempts")
                raise
            base = BLOCK_BACKOFF * attempt if blocked else 2 ** attempt
            delay = base + random.uniform(0, 2)
            log(
                f"🔄 Retry {attempt}/{attempts} for {venue_code} in {delay:.1f}s ({type(e).__name__}: {str(e)[:60]})")
            time.sleep(delay)


def minutes_left(show_time_str):
    try:
        now = datetime.now(IST)
        t = datetime.strptime(show_time_str, "%I:%M %p")
        t = t.replace(year=now.year, month=now.month, day=now.day, tzinfo=IST)
        return (t - now).total_seconds() / 60
    except Exception:
        return 9999


def parse_payload(data):
    out = []
    sd = data.get("ShowDetails", [])
    if not sd:
        return out

    venue = sd[0].get("Venues", {})
    venue_name = venue.get("VenueName", "")
    venue_add = venue.get("VenueAdd", "")
    chain = venue.get("VenueCompName", "Unknown")

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")
        for ch in ev.get("ChildEvents", []):
            dim = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                if sh.get("ShowDateCode") != DATE_CODE:
                    continue

                total = sold = avail = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += free
                    sold += seats - free
                    gross += (seats - free) * price

                out.append({
                    "movie": movie,
                    "venue": venue_name,
                    "address": venue_add,
                    "chain": chain,
                    "time": sh.get("ShowTime", ""),
                    "audi": sh.get("Attributes", "") or "",
                    "session_id": str(sh.get("SessionId", "")),
                    "totalSeats": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2)
                })
    return out


def show_key(r):
    return (r["venue"], r["time"], r["session_id"], r["audi"])


def process_venue(vcode, venues, fetched, attempts=MAX_ATTEMPTS):
    raw = fetch_with_retry(vcode, attempts=attempts)
    for r in parse_payload(raw):
        mins = minutes_left(r["time"])
        if mins <= CUTOFF_MINUTES:
            r["minsLeft"] = round(mins, 1)
            r["city"] = venues[vcode].get("City", "Unknown")
            r["state"] = venues[vcode].get("State", "Unknown")
            r["source"] = "BMS"
            r["date"] = DATE_CODE
            fetched.append(r)


if __name__ == "__main__":
    log(f"🚀 BMS DAILY TRACKER STARTED | client={'curl_cffi' if HAVE_CFFI else 'cloudscraper'} | proxy={'on' if PROXIES else 'off'}")
    if not HAVE_CFFI:
        log("⚠️ curl_cffi not installed – run `pip install -U curl_cffi` for far fewer 403s")

    with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
        venues = json.load(f)

    fetched = []
    retry = set()
    failure_count = 0
    cooldowns_in_a_row = 0
    blocked_out = False
    venue_list = list(venues)

    for i, vcode in enumerate(venue_list, 1):
        log(f"[{i}/{len(venue_list)}] {vcode}")
        delay = min(BASE_DELAY + failure_count * 0.2, MAX_DELAY)
        time.sleep(random.uniform(delay, delay + 0.3))

        try:
            process_venue(vcode, venues, fetched)
            failure_count = 0
            cooldowns_in_a_row = 0
        except Exception as e:
            retry.add(vcode)
            failure_count += 1
            log(
                f"❌ {vcode} final failure | {type(e).__name__} | {str(e)[:100]}")

            if failure_count >= FAILURE_THRESHOLD:
                cooldowns_in_a_row += 1
                if cooldowns_in_a_row >= MAX_COOLDOWNS_IN_A_ROW:
                    log("⛔ Still blocked after repeated cooldowns – this IP is blocked. "
                        "Stopping early and saving what we have. Use BMS_PROXY or a different runner.")
                    # everything not attempted yet
                    retry.update(venue_list[i:])
                    blocked_out = True
                    break
                log(f"⛔ {failure_count} consecutive failures – cooling down {COOLDOWN_SECONDS}s")
                time.sleep(COOLDOWN_SECONDS)
                failure_count = 0
                reset_identity()

    # Final retry passes (skipped if the IP is clearly blocked – it would only waste time)
    if not blocked_out:
        for rnd in range(1, FINAL_RETRY_ROUNDS + 1):
            if not retry:
                break
            log(f"🔁 RETRY ROUND {rnd} | Remaining: {len(retry)}")
            # let the block window cool off
            time.sleep(random.uniform(20, 40))
            current_retry = list(retry)
            retry.clear()
            for vcode in current_retry:
                log(f"[RETRY-{rnd}] {vcode}")
                time.sleep(random.uniform(1.0, 2.5))
                try:
                    process_venue(vcode, venues, fetched, attempts=2)
                except Exception as e:
                    retry.add(vcode)
                    log(f"❌ RETRY FAIL {vcode} | {type(e).__name__}")

    if retry:
        log(f"⚠️ FINAL FAILED VENUES: {len(retry)}")
        log(f"FAILED LIST: {sorted(retry)}")

    # ---------- merge with existing data (from R2) ----------
    old_rows = r2_download_json(R2_DETAILED_KEY, default=[])
    log(f"📥 Loaded {len(old_rows)} existing rows from R2")

    old_map = {show_key(r): r for r in old_rows}
    new_map = {}

    for r in fetched:
        key = show_key(r)
        if key in old_map:
            old_map[key].update({
                "totalSeats": r["totalSeats"],
                "available": r["available"],
                "sold": r["sold"],
                "gross": r["gross"],
                "minsLeft": r.get("minsLeft")
            })
            new_map[key] = old_map[key]
        else:
            new_map[key] = r

    for key, r in old_map.items():
        if key not in new_map:
            new_map[key] = r

    detailed = list(new_map.values())

    # ---------- summary ----------
    summary = {}
    for r in detailed:
        movie = r["movie"]
        city = r["city"]
        venue = r["venue"]
        total = r["totalSeats"]
        sold = r["sold"]
        gross = r["gross"]
        occ = (sold / total * 100) if total else 0

        if movie not in summary:
            summary[movie] = {
                "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                "venues": set(), "cities": set(), "fastfilling": 0, "housefull": 0
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

    final_summary = {
        movie: {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0
        }
        for movie, m in summary.items()
    }

    # ---------- save to R2 ----------
    r2_upload_json(R2_DETAILED_KEY, detailed)
    r2_upload_json(R2_SUMMARY_KEY, final_summary)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")
