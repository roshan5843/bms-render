# # # bms-render/bmsrotate8.py
# #
# # import json
# # import os
# # import random
# # import time
# # import threading
# # import signal
# # from collections import defaultdict
# # from datetime import datetime, timedelta, timezone
# #
# # import cloudscraper
# # from r2_client import r2_upload_json
# #
# # # =====================================================
# # # CONFIG – tune these for speed vs. safety
# # # =====================================================
# # SHARD_ID = 8
# # API_TIMEOUT = 12
# # HARD_TIMEOUT = 15
# # MAX_RECOVERY_ROUNDS = 5           # max retries per venue (initial + retry rounds)
# # IDENTITY_ROTATE_EVERY = 10        # new identity after every N requests
# # FAILURE_THRESHOLD = 5             # consecutive failures before cooldown
# # COOLDOWN_SECONDS = 30             # pause when too many failures
# # BASE_DELAY = 0.45                 # minimum seconds between requests
# # MAX_DELAY = 1.75                  # maximum seconds between requests (adaptive)
# #
# # IST = timezone(timedelta(hours=5, minutes=30))
# # DATE_CODE = os.environ["DATE_CODE"]
# #
# # # used only for R2 key naming (nothing is written to disk)
# # BASE_DIR = f"advance/{DATE_CODE}"
# #
# # R2_DETAILED_KEY = f"advance/{DATE_CODE}/detailed{SHARD_ID}.json"
# # R2_SUMMARY_KEY = f"advance/{DATE_CODE}/movie_summary{SHARD_ID}.json"
# #
# # # =====================================================
# # # LOGGING (stdout only)
# # # =====================================================
# #
# #
# # def log(msg):
# #     ts = datetime.now(IST).strftime("%H:%M:%S")
# #     line = f"[{ts}] {msg}"
# #     print(line, flush=True)
# #
# #
# # # =====================================================
# # # HARD TIMEOUT
# # # =====================================================
# # class TimeoutError(Exception):
# #     pass
# #
# #
# # def _timeout_handler(signum, frame):
# #     raise TimeoutError("Hard timeout hit")
# #
# #
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
# #
# #
# # # =====================================================
# # # USER AGENTS
# # # =====================================================
# # USER_AGENTS = [
# #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
# #     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
# #     "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
# #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
# # ]
# #
# # thread_local = threading.local()
# # request_counter = 0
# #
# #
# # class Identity:
# #     def __init__(self):
# #         self.ua = random.choice(USER_AGENTS)
# #         self.ip = ".".join(str(random.randint(20, 230)) for _ in range(4))
# #         self.scraper = cloudscraper.create_scraper(
# #             browser={"browser": "chrome",
# #                      "platform": "windows", "desktop": True}
# #         )
# #
# #     def headers(self):
# #         # Full browser-like headers
# #         return {
# #             "User-Agent": self.ua,
# #             "Accept": "application/json, text/plain, */*",
# #             "Accept-Language": "en-IN,en;q=0.9",
# #             "Accept-Encoding": "gzip, deflate, br",
# #             "Connection": "keep-alive",
# #             "Sec-Fetch-Dest": "empty",
# #             "Sec-Fetch-Mode": "cors",
# #             "Sec-Fetch-Site": "same-origin",
# #             "Origin": "https://in.bookmyshow.com",
# #             "Referer": "https://in.bookmyshow.com/",
# #             "Cache-Control": "no-cache",
# #             "Pragma": "no-cache",
# #             "X-Forwarded-For": self.ip,
# #         }
# #
# #
# # def get_identity():
# #     global request_counter
# #     if not hasattr(thread_local, "identity"):
# #         thread_local.identity = Identity()
# #         log("🧠 New identity created")
# #     request_counter += 1
# #     # Rotate proactively every N requests
# #     if request_counter % IDENTITY_ROTATE_EVERY == 0:
# #         reset_identity()
# #         thread_local.identity = Identity()
# #         log(f"🔄 Identity rotated (every {IDENTITY_ROTATE_EVERY} requests)")
# #     return thread_local.identity
# #
# #
# # def reset_identity():
# #     if hasattr(thread_local, "identity"):
# #         del thread_local.identity
# #     log("🔄 Identity reset")
# #
# # # =====================================================
# # # FETCH API (with retry and block detection)
# # # =====================================================
# #
# #
# # @hard_timeout(HARD_TIMEOUT)
# # def fetch_api_raw(venue_code):
# #     ident = get_identity()
# #     url = (
# #         "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
# #         f"?venueCode={venue_code}&dateCode={DATE_CODE}"
# #     )
# #     r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)
# #     if r.status_code != 200:
# #         raise RuntimeError(f"HTTP {r.status_code}")
# #     text = r.text.strip()
# #     if not text.startswith("{"):
# #         snippet = text[:200].replace("\n", " ")
# #         raise RuntimeError(f"Blocked / HTML (snippet: {snippet})")
# #     return r.json()
# #
# #
# # def fetch_with_retry(venue_code, attempt=1):
# #     try:
# #         return fetch_api_raw(venue_code)
# #     except Exception as e:
# #         if attempt >= MAX_RECOVERY_ROUNDS:
# #             log(f"❌ {venue_code} failed after {MAX_RECOVERY_ROUNDS} attempts")
# #             raise
# #         delay = (2 ** attempt) + random.uniform(0, 1)   # exponential backoff
# #         log(f"🔄 Retry {attempt}/{MAX_RECOVERY_ROUNDS} for {venue_code} in {delay:.1f}s")
# #         reset_identity()
# #         time.sleep(delay)
# #         return fetch_with_retry(venue_code, attempt + 1)
# #
# # # =====================================================
# # # PARSER
# # # =====================================================
# #
# #
# # def parse_payload(data):
# #     out = []
# #
# #     sd = data.get("ShowDetails", [])
# #     if not sd:
# #         return out
# #
# #     venue = sd[0].get("Venues", {})
# #     venue_name = venue.get("VenueName", "")
# #     venue_add = venue.get("VenueAdd", "")
# #     chain = venue.get("VenueCompName", "Unknown")
# #
# #     for ev in sd[0].get("Event", []):
# #         title = ev.get("EventTitle", "Unknown")
# #
# #         for ch in ev.get("ChildEvents", []):
# #             dim = ch.get("EventDimension", "").strip()
# #             lang = ch.get("EventLanguage", "").strip()
# #             suffix = " | ".join(x for x in (dim, lang) if x)
# #             movie = f"{title} [{suffix}]" if suffix else title
# #
# #             for sh in ch.get("ShowTimes", []):
# #                 show_date = sh.get("ShowDateCode")
# #                 if show_date != DATE_CODE:
# #                     continue
# #
# #                 total = sold = avail = gross = 0
# #                 for cat in sh.get("Categories", []):
# #                     seats = int(cat.get("MaxSeats", 0))
# #                     free = int(cat.get("SeatsAvail", 0))
# #                     price = float(cat.get("CurPrice", 0))
# #                     total += seats
# #                     avail += free
# #                     sold += seats - free
# #                     gross += (seats - free) * price
# #
# #                 out.append({
# #                     "movie": movie,
# #                     "venue": venue_name,
# #                     "address": venue_add,
# #                     "chain": chain,
# #                     "time": sh.get("ShowTime", ""),
# #                     "audi": sh.get("Attributes", "") or "",
# #                     "session_id": str(sh.get("SessionId", "")),
# #                     "totalSeats": total,
# #                     "available": avail,
# #                     "sold": sold,
# #                     "gross": round(gross, 2)
# #                 })
# #
# #     return out
# #
# # # =====================================================
# # # DEDUPE
# # # =====================================================
# #
# #
# # def dedupe(rows):
# #     seen = set()
# #     out = []
# #     for r in rows:
# #         key = (
# #             r["venue"],
# #             r["time"],
# #             r["session_id"],
# #             r["audi"]
# #         )
# #         if key in seen:
# #             continue
# #         seen.add(key)
# #         out.append(r)
# #     return out
# #
# #
# # # =====================================================
# # # MAIN
# # # =====================================================
# # if __name__ == "__main__":
# #     log("🚀 SCRIPT STARTED")
# #
# #     with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
# #         venues = json.load(f)
# #
# #     all_rows = []
# #     retry = set()
# #     failure_count = 0
# #
# #     # ------ INITIAL PASS ------
# #     for i, vcode in enumerate(venues, 1):
# #         log(f"[{i}/{len(venues)}] {vcode}")
# #
# #         # Adaptive delay: increase after failures
# #         delay = BASE_DELAY + (failure_count * 0.2)
# #         delay = min(delay, MAX_DELAY)
# #         time.sleep(random.uniform(delay, delay + 0.3))
# #
# #         try:
# #             raw = fetch_with_retry(vcode)   # internal retries
# #             rows = parse_payload(raw)
# #             for r in rows:
# #                 r["city"] = venues[vcode].get("City", "Unknown")
# #                 r["state"] = venues[vcode].get("State", "Unknown")
# #                 r["source"] = "BMS"
# #                 r["date"] = DATE_CODE
# #             all_rows.extend(rows)
# #             failure_count = 0   # reset on success
# #         except Exception as e:
# #             retry.add(vcode)
# #             failure_count += 1
# #             if failure_count >= FAILURE_THRESHOLD:
# #                 log(f"⛔ {failure_count} consecutive failures – cooling down {COOLDOWN_SECONDS}s")
# #                 time.sleep(COOLDOWN_SECONDS)
# #                 failure_count = 0
# #             reset_identity()
# #             log(f"❌ {vcode} final failure | {type(e).__name__} | {str(e)[:100]}")
# #
# #     # ------ RETRY ROUNDS (for failed venues) ------
# #     for attempt in range(1, MAX_RECOVERY_ROUNDS + 1):
# #         if not retry:
# #             break
# #
# #         log(f"🔁 RETRY ROUND {attempt} | Remaining: {len(retry)}")
# #         current_retry = list(retry)
# #         retry.clear()
# #
# #         for vcode in current_retry:
# #             log(f"[RETRY-{attempt}] {vcode}")
# #             # Adaptive delay inside retry (slightly longer)
# #             time.sleep(random.uniform(1.0, 2.5))
# #
# #             try:
# #                 raw = fetch_with_retry(vcode, attempt=1)  # will use its own retries
# #                 rows = parse_payload(raw)
# #                 for r in rows:
# #                     r["city"] = venues[vcode].get("City", "Unknown")
# #                     r["state"] = venues[vcode].get("State", "Unknown")
# #                     r["source"] = "BMS"
# #                     r["date"] = DATE_CODE
# #                 all_rows.extend(rows)
# #             except Exception as e:
# #                 retry.add(vcode)
# #                 reset_identity()
# #                 log(f"❌ RETRY FAIL {vcode} | {type(e).__name__}")
# #
# #             time.sleep(random.uniform(0.5, 1.0))   # small delay between retry items
# #
# #     if retry:
# #         log(f"⚠️ FINAL FAILED VENUES: {len(retry)}")
# #         log(f"FAILED LIST: {list(retry)}")
# #
# #     log("🧹 Deduping shows")
# #     detailed = dedupe(all_rows)
# #
# #     # =====================================================
# #     # SUMMARY (AFTER DEDUPE)
# #     # =====================================================
# #     summary = {}
# #
# #     for r in detailed:
# #         movie = r["movie"]
# #         city = r["city"]
# #         state = r["state"]
# #         venue = r["venue"]
# #         chain = r["chain"]
# #
# #         total = r["totalSeats"]
# #         sold = r["sold"]
# #         gross = r["gross"]
# #         occ = (sold / total * 100) if total else 0
# #
# #         if movie not in summary:
# #             summary[movie] = {
# #                 "shows": 0,
# #                 "gross": 0.0,
# #                 "sold": 0,
# #                 "totalSeats": 0,
# #                 "venues": set(),
# #                 "cities": set(),
# #                 "fastfilling": 0,
# #                 "housefull": 0,
# #                 "details": {},
# #                 "Chain_details": {}
# #             }
# #
# #         m = summary[movie]
# #         m["shows"] += 1
# #         m["gross"] += gross
# #         m["sold"] += sold
# #         m["totalSeats"] += total
# #         m["venues"].add(venue)
# #         m["cities"].add(city)
# #
# #         if occ >= 98:
# #             m["housefull"] += 1
# #         elif occ >= 50:
# #             m["fastfilling"] += 1
# #
# #         ck = (city, state)
# #         if ck not in m["details"]:
# #             m["details"][ck] = {
# #                 "city": city, "state": state,
# #                 "venues": set(),
# #                 "shows": 0, "gross": 0.0,
# #                 "sold": 0, "totalSeats": 0,
# #                 "fastfilling": 0, "housefull": 0
# #             }
# #
# #         d = m["details"][ck]
# #         d["venues"].add(venue)
# #         d["shows"] += 1
# #         d["gross"] += gross
# #         d["sold"] += sold
# #         d["totalSeats"] += total
# #         if occ >= 98:
# #             d["housefull"] += 1
# #         elif occ >= 50:
# #             d["fastfilling"] += 1
# #
# #         if chain not in m["Chain_details"]:
# #             m["Chain_details"][chain] = {
# #                 "chain": chain,
# #                 "venues": set(),
# #                 "shows": 0, "gross": 0.0,
# #                 "sold": 0, "totalSeats": 0,
# #                 "fastfilling": 0, "housefull": 0
# #             }
# #
# #         c = m["Chain_details"][chain]
# #         c["venues"].add(venue)
# #         c["shows"] += 1
# #         c["gross"] += gross
# #         c["sold"] += sold
# #         c["totalSeats"] += total
# #         if occ >= 98:
# #             c["housefull"] += 1
# #         elif occ >= 50:
# #             c["fastfilling"] += 1
# #
# #     # =====================================================
# #     # FINALIZE SUMMARY
# #     # =====================================================
# #     final_summary = {}
# #
# #     for movie, m in summary.items():
# #         final_summary[movie] = {
# #             "shows": m["shows"],
# #             "gross": round(m["gross"], 2),
# #             "sold": m["sold"],
# #             "totalSeats": m["totalSeats"],
# #             "venues": len(m["venues"]),
# #             "cities": len(m["cities"]),
# #             "fastfilling": m["fastfilling"],
# #             "housefull": m["housefull"],
# #             "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
# #             "details": [],
# #             "Chain_details": []
# #         }
# #
# #         for d in m["details"].values():
# #             final_summary[movie]["details"].append({
# #                 "city": d["city"],
# #                 "state": d["state"],
# #                 "venues": len(d["venues"]),
# #                 "shows": d["shows"],
# #                 "gross": round(d["gross"], 2),
# #                 "sold": d["sold"],
# #                 "totalSeats": d["totalSeats"],
# #                 "fastfilling": d["fastfilling"],
# #                 "housefull": d["housefull"],
# #                 "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0
# #             })
# #
# #         for c in m["Chain_details"].values():
# #             final_summary[movie]["Chain_details"].append({
# #                 "chain": c["chain"],
# #                 "venues": len(c["venues"]),
# #                 "shows": c["shows"],
# #                 "gross": round(c["gross"], 2),
# #                 "sold": c["sold"],
# #                 "totalSeats": c["totalSeats"],
# #                 "fastfilling": c["fastfilling"],
# #                 "housefull": c["housefull"],
# #                 "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0
# #             })
# #
# #     # =====================================================
# #     # SAVE TO R2 (NOT DISK)
# #     # =====================================================
# #     r2_upload_json(R2_DETAILED_KEY, detailed)
# #     r2_upload_json(R2_SUMMARY_KEY, final_summary)
# #
# #     log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")
#
#
# import json
# import os
# import random
# import sys
# import time
# import threading
# import signal
# from datetime import datetime, timedelta, timezone
#
# # Prefer curl_cffi (real Chrome TLS/HTTP2 fingerprint). Fall back to cloudscraper.
# try:
#     from curl_cffi import requests as cffi_requests
#     HAVE_CFFI = True
# except ImportError:
#     import cloudscraper
#     HAVE_CFFI = False
#
# from r2_client import r2_upload_json
#
# # =====================================================
# # CONFIG
# # =====================================================
# SHARD_ID = 8
# API_TIMEOUT = 12
# HARD_TIMEOUT = 20
# # attempts per venue per pass (single retry layer)
# MAX_ATTEMPTS = 3
# FINAL_RETRY_ROUNDS = 2            # extra passes over failed venues at the end
# IDENTITY_ROTATE_EVERY = 40        # rotate a warmed-up identity after N requests
# FAILURE_THRESHOLD = 5             # consecutive failed venues before cooldown
# COOLDOWN_SECONDS = 60
# MAX_COOLDOWNS_IN_A_ROW = 3        # still blocked after this many cooldowns -> stop
# BLOCK_BACKOFF = 6                 # base backoff (s) on 403/429
# BASE_DELAY = 0.45
# MAX_DELAY = 1.75
# # random startup delay so parallel shards don't hit at the same second
# START_JITTER_MAX = 30
#
# # Optional proxy, e.g. BMS_PROXY="http://user:pass@host:port" (Indian residential/ISP works best)
# PROXY_URL = os.environ.get("BMS_PROXY", "").strip()
# PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
#
# HOME_URL = "https://in.bookmyshow.com/"
# IMPERSONATE_TARGETS = ["chrome120", "chrome124"]
#
# IST = timezone(timedelta(hours=5, minutes=30))
# DATE_CODE = os.environ["DATE_CODE"]
#
# R2_DETAILED_KEY = f"advance/{DATE_CODE}/detailed{SHARD_ID}.json"
# R2_SUMMARY_KEY = f"advance/{DATE_CODE}/movie_summary{SHARD_ID}.json"
#
#
# def log(msg):
#     ts = datetime.now(IST).strftime("%H:%M:%S")
#     print(f"[{ts}] {msg}", flush=True)
#
#
# class HardTimeout(Exception):
#     pass
#
#
# class BlockedError(RuntimeError):
#     """403 / 429 / HTML challenge page: we are being blocked."""
#
#
# def _timeout_handler(signum, frame):
#     raise HardTimeout("Hard timeout hit")
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
# thread_local = threading.local()
# _diag_logged = False
#
#
# class Identity:
#     """One browser-like session. UA/TLS fingerprint come from the library so they
#     always match each other. Cookies are collected via a warm-up request."""
#
#     def __init__(self):
#         self.requests_made = 0
#         if HAVE_CFFI:
#             self.session = cffi_requests.Session(
#                 impersonate=random.choice(IMPERSONATE_TARGETS),
#                 proxies=PROXIES,
#             )
#         else:
#             self.session = cloudscraper.create_scraper(
#                 browser={"browser": "chrome",
#                          "platform": "windows", "desktop": True}
#             )
#             if PROXIES:
#                 self.session.proxies.update(PROXIES)
#         self.session.headers.update({"Accept-Language": "en-IN,en;q=0.9"})
#         self._warm_up()
#
#     def _warm_up(self):
#         try:
#             r = self.session.get(HOME_URL, timeout=API_TIMEOUT)
#             log(f"🧠 New identity created (warm-up HTTP {r.status_code})")
#         except Exception as e:
#             log(f"🧠 New identity created (warm-up failed: {type(e).__name__})")
#
#     def api_headers(self):
#         # No User-Agent / Accept-Encoding / Sec-* / X-Forwarded-For: the library sets
#         # a consistent set for the impersonated browser.
#         return {
#             "Accept": "application/json, text/plain, */*",
#             "Origin": "https://in.bookmyshow.com",
#             "Referer": HOME_URL,
#         }
#
#
# def get_identity():
#     ident = getattr(thread_local, "identity", None)
#     if ident is not None and ident.requests_made >= IDENTITY_ROTATE_EVERY:
#         log(f"🔄 Identity rotated (every {IDENTITY_ROTATE_EVERY} requests)")
#         reset_identity()
#         ident = None
#     if ident is None:
#         ident = Identity()
#         thread_local.identity = ident
#     ident.requests_made += 1
#     return ident
#
#
# def reset_identity():
#     ident = getattr(thread_local, "identity", None)
#     if ident is not None:
#         try:
#             ident.session.close()
#         except Exception:
#             pass
#         del thread_local.identity
#
#
# def log_egress_ip():
#     """Log the runner's public IP so you can compare working vs failing runs."""
#     try:
#         if HAVE_CFFI:
#             ip = cffi_requests.get(
#                 "https://api.ipify.org", timeout=10, proxies=PROXIES).text
#         else:
#             ip = cloudscraper.create_scraper().get("https://api.ipify.org", timeout=10).text
#         log(f"🌐 Egress IP: {ip}")
#     except Exception as e:
#         log(f"🌐 Egress IP lookup failed: {type(e).__name__}")
#
#
# def log_block_diagnostics(r):
#     """Log once who is blocking us (Cloudflare / Akamai / app)."""
#     global _diag_logged
#     if _diag_logged:
#         return
#     _diag_logged = True
#     h = {k.lower(): v for k, v in r.headers.items()}
#     interesting = {k: h[k] for k in (
#         "server", "cf-ray", "x-akamai-request-id", "x-cache", "via") if k in h}
#     snippet = r.text[:200].replace("\n", " ")
#     log(f"🔎 Block diagnostics | status={r.status_code} | headers={interesting} | body={snippet}")
#
#
# @hard_timeout(HARD_TIMEOUT)
# def fetch_api_raw(venue_code):
#     ident = get_identity()
#     url = (
#         "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
#         f"?venueCode={venue_code}&dateCode={DATE_CODE}"
#     )
#     r = ident.session.get(
#         url, headers=ident.api_headers(), timeout=API_TIMEOUT)
#     if r.status_code in (403, 429):
#         log_block_diagnostics(r)
#         raise BlockedError(f"HTTP {r.status_code}")
#     if r.status_code != 200:
#         raise RuntimeError(f"HTTP {r.status_code}")
#     text = r.text.strip()
#     if not text.startswith("{"):
#         log_block_diagnostics(r)
#         snippet = text[:200].replace("\n", " ")
#         raise BlockedError(f"Blocked / HTML (snippet: {snippet})")
#     return r.json()
#
#
# def fetch_with_retry(venue_code, attempts=MAX_ATTEMPTS):
#     """Single retry layer. Blocks get a longer backoff and a fresh identity."""
#     for attempt in range(1, attempts + 1):
#         try:
#             return fetch_api_raw(venue_code)
#         except Exception as e:
#             blocked = isinstance(e, BlockedError)
#             reset_identity()
#             if attempt == attempts:
#                 log(f"❌ {venue_code} failed after {attempts} attempts")
#                 raise
#             base = BLOCK_BACKOFF * attempt if blocked else 2 ** attempt
#             delay = base + random.uniform(0, 2)
#             log(
#                 f"🔄 Retry {attempt}/{attempts} for {venue_code} in {delay:.1f}s ({type(e).__name__}: {str(e)[:60]})")
#             time.sleep(delay)
#
#
# # =====================================================
# # PARSER
# # =====================================================
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
#                     "movie": movie,
#                     "venue": venue_name,
#                     "address": venue_add,
#                     "chain": chain,
#                     "time": sh.get("ShowTime", ""),
#                     "audi": sh.get("Attributes", "") or "",
#                     "session_id": str(sh.get("SessionId", "")),
#                     "totalSeats": total,
#                     "available": avail,
#                     "sold": sold,
#                     "gross": round(gross, 2)
#                 })
#
#     return out
#
#
# def dedupe(rows):
#     seen = set()
#     out = []
#     for r in rows:
#         key = (r["venue"], r["time"], r["session_id"], r["audi"])
#         if key in seen:
#             continue
#         seen.add(key)
#         out.append(r)
#     return out
#
#
# def process_venue(vcode, venues, all_rows, attempts=MAX_ATTEMPTS):
#     raw = fetch_with_retry(vcode, attempts=attempts)
#     rows = parse_payload(raw)
#     for r in rows:
#         r["city"] = venues[vcode].get("City", "Unknown")
#         r["state"] = venues[vcode].get("State", "Unknown")
#         r["source"] = "BMS"
#         r["date"] = DATE_CODE
#     all_rows.extend(rows)
#
#
# # =====================================================
# # MAIN
# # =====================================================
# if __name__ == "__main__":
#     log(f"🚀 SCRIPT STARTED | shard={SHARD_ID} | date={DATE_CODE} | "
#         f"client={'curl_cffi' if HAVE_CFFI else 'cloudscraper'} | proxy={'on' if PROXIES else 'off'}")
#     if not HAVE_CFFI:
#         log("⚠️ curl_cffi not installed – run `pip install -U curl_cffi` for far fewer 403s")
#
#     log_egress_ip()
#
#     # Spread out parallel shards so they don't all hit BMS in the same second
#     jitter = random.uniform(0, START_JITTER_MAX)
#     log(f"⏳ Startup jitter {jitter:.1f}s")
#     time.sleep(jitter)
#
#     with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
#         venues = json.load(f)
#
#     all_rows = []
#     retry = set()
#     failure_count = 0
#     cooldowns_in_a_row = 0
#     blocked_out = False
#     venue_list = list(venues)
#
#     # ------ INITIAL PASS ------
#     for i, vcode in enumerate(venue_list, 1):
#         log(f"[{i}/{len(venue_list)}] {vcode}")
#         delay = min(BASE_DELAY + failure_count * 0.2, MAX_DELAY)
#         time.sleep(random.uniform(delay, delay + 0.3))
#
#         try:
#             process_venue(vcode, venues, all_rows)
#             failure_count = 0
#             cooldowns_in_a_row = 0
#         except Exception as e:
#             retry.add(vcode)
#             failure_count += 1
#             log(
#                 f"❌ {vcode} final failure | {type(e).__name__} | {str(e)[:100]}")
#
#             if failure_count >= FAILURE_THRESHOLD:
#                 cooldowns_in_a_row += 1
#                 if cooldowns_in_a_row >= MAX_COOLDOWNS_IN_A_ROW:
#                     log("⛔ Still blocked after repeated cooldowns – this runner's IP is blocked. "
#                         "Stopping early. Set BMS_PROXY or stagger shards.")
#                     retry.update(venue_list[i:])
#                     blocked_out = True
#                     break
#                 log(f"⛔ {failure_count} consecutive failures – cooling down {COOLDOWN_SECONDS}s")
#                 time.sleep(COOLDOWN_SECONDS)
#                 failure_count = 0
#                 reset_identity()
#
#     # ------ FINAL RETRY PASSES (skipped if clearly IP-blocked) ------
#     if not blocked_out:
#         for rnd in range(1, FINAL_RETRY_ROUNDS + 1):
#             if not retry:
#                 break
#             log(f"🔁 RETRY ROUND {rnd} | Remaining: {len(retry)}")
#             time.sleep(random.uniform(20, 40))
#             current_retry = list(retry)
#             retry.clear()
#             for vcode in current_retry:
#                 log(f"[RETRY-{rnd}] {vcode}")
#                 time.sleep(random.uniform(1.0, 2.5))
#                 try:
#                     process_venue(vcode, venues, all_rows, attempts=2)
#                 except Exception as e:
#                     retry.add(vcode)
#                     log(f"❌ RETRY FAIL {vcode} | {type(e).__name__}")
#
#     if retry:
#         log(f"⚠️ FINAL FAILED VENUES: {len(retry)} / {len(venue_list)}")
#         log(f"FAILED LIST: {sorted(retry)}")
#
#     # Never overwrite good data in R2 with an empty result
#     if not all_rows:
#         log("🚫 No rows collected – NOT uploading to R2 (keeping existing data). Failing the job.")
#         sys.exit(1)
#
#     log("🧹 Deduping shows")
#     detailed = dedupe(all_rows)
#
#     # =====================================================
#     # SUMMARY (AFTER DEDUPE)
#     # =====================================================
#     summary = {}
#
#     for r in detailed:
#         movie = r["movie"]
#         city = r["city"]
#         state = r["state"]
#         venue = r["venue"]
#         chain = r["chain"]
#
#         total = r["totalSeats"]
#         sold = r["sold"]
#         gross = r["gross"]
#         occ = (sold / total * 100) if total else 0
#
#         if movie not in summary:
#             summary[movie] = {
#                 "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
#                 "venues": set(), "cities": set(),
#                 "fastfilling": 0, "housefull": 0,
#                 "details": {}, "Chain_details": {}
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
#         ck = (city, state)
#         if ck not in m["details"]:
#             m["details"][ck] = {
#                 "city": city, "state": state, "venues": set(),
#                 "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
#                 "fastfilling": 0, "housefull": 0
#             }
#
#         d = m["details"][ck]
#         d["venues"].add(venue)
#         d["shows"] += 1
#         d["gross"] += gross
#         d["sold"] += sold
#         d["totalSeats"] += total
#         if occ >= 98:
#             d["housefull"] += 1
#         elif occ >= 50:
#             d["fastfilling"] += 1
#
#         if chain not in m["Chain_details"]:
#             m["Chain_details"][chain] = {
#                 "chain": chain, "venues": set(),
#                 "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
#                 "fastfilling": 0, "housefull": 0
#             }
#
#         c = m["Chain_details"][chain]
#         c["venues"].add(venue)
#         c["shows"] += 1
#         c["gross"] += gross
#         c["sold"] += sold
#         c["totalSeats"] += total
#         if occ >= 98:
#             c["housefull"] += 1
#         elif occ >= 50:
#             c["fastfilling"] += 1
#
#     # =====================================================
#     # FINALIZE SUMMARY
#     # =====================================================
#     final_summary = {}
#
#     for movie, m in summary.items():
#         final_summary[movie] = {
#             "shows": m["shows"],
#             "gross": round(m["gross"], 2),
#             "sold": m["sold"],
#             "totalSeats": m["totalSeats"],
#             "venues": len(m["venues"]),
#             "cities": len(m["cities"]),
#             "fastfilling": m["fastfilling"],
#             "housefull": m["housefull"],
#             "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
#             "details": [],
#             "Chain_details": []
#         }
#
#         for d in m["details"].values():
#             final_summary[movie]["details"].append({
#                 "city": d["city"],
#                 "state": d["state"],
#                 "venues": len(d["venues"]),
#                 "shows": d["shows"],
#                 "gross": round(d["gross"], 2),
#                 "sold": d["sold"],
#                 "totalSeats": d["totalSeats"],
#                 "fastfilling": d["fastfilling"],
#                 "housefull": d["housefull"],
#                 "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0
#             })
#
#         for c in m["Chain_details"].values():
#             final_summary[movie]["Chain_details"].append({
#                 "chain": c["chain"],
#                 "venues": len(c["venues"]),
#                 "shows": c["shows"],
#                 "gross": round(c["gross"], 2),
#                 "sold": c["sold"],
#                 "totalSeats": c["totalSeats"],
#                 "fastfilling": c["fastfilling"],
#                 "housefull": c["housefull"],
#                 "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0
#             })
#
#     # =====================================================
#     # SAVE TO R2
#     # =====================================================
#     r2_upload_json(R2_DETAILED_KEY, detailed)
#     r2_upload_json(R2_SUMMARY_KEY, final_summary)
#
#     log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")


import json
import os
import random
import sys
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

from r2_client import r2_upload_json

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 8
API_TIMEOUT = 12
HARD_TIMEOUT = 20
# attempts per venue per pass (single retry layer)
MAX_ATTEMPTS = 3
FINAL_RETRY_ROUNDS = 2            # extra passes over failed venues at the end
# 0 = never rotate a working identity (only replace it when blocked)
IDENTITY_ROTATE_EVERY = 0
FAILURE_THRESHOLD = 5             # consecutive failed venues before cooldown
COOLDOWN_SECONDS = 60
MAX_COOLDOWNS_IN_A_ROW = 3        # still blocked after this many cooldowns -> stop
# base backoff (s) on 403/429 (replacement identity is pre-validated, so keep short)
BLOCK_BACKOFF = 1.5
MAX_WARMUP_TRIES = 8              # new identities to try before giving up and pausing
IDENTITY_SWAP_DELAY = (0.5, 1.5)  # short pause between rejected identities
# pause if every identity in a batch was rejected
EXHAUSTED_PAUSE = (15, 25)
BASE_DELAY = 0.45
MAX_DELAY = 1.75
# random startup delay so parallel shards don't hit at the same second
START_JITTER_MAX = 30

# Optional proxy, e.g. BMS_PROXY="http://user:pass@host:port" (Indian residential/ISP works best)
PROXY_URL = os.environ.get("BMS_PROXY", "").strip()
PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

HOME_URL = "https://in.bookmyshow.com/"
IMPERSONATE_TARGETS = ["chrome120", "chrome124"]

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = os.environ["DATE_CODE"]

R2_DETAILED_KEY = f"advance/{DATE_CODE}/detailed{SHARD_ID}.json"
R2_SUMMARY_KEY = f"advance/{DATE_CODE}/movie_summary{SHARD_ID}.json"


def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


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
_diag_logged = False


class Identity:
    """One browser-like session. UA/TLS fingerprint come from the library so they
    always match each other. A warm-up request collects cookies AND tells us
    whether this identity is accepted before we spend real requests on it."""

    def __init__(self):
        self.requests_made = 0
        self.warm_status = None
        self.target = random.choice(
            IMPERSONATE_TARGETS) if HAVE_CFFI else "cloudscraper"
        if HAVE_CFFI:
            self.session = cffi_requests.Session(
                impersonate=self.target, proxies=PROXIES)
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
            self.warm_status = r.status_code
        except Exception as e:
            self.warm_status = type(e).__name__

    @property
    def healthy(self):
        return self.warm_status == 200

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def api_headers(self):
        # No User-Agent / Accept-Encoding / Sec-* / X-Forwarded-For: the library sets
        # a consistent set for the impersonated browser.
        return {
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://in.bookmyshow.com",
            "Referer": HOME_URL,
        }


def new_healthy_identity():
    """Keep creating identities until one passes the warm-up (HTTP 200)."""
    for n in range(1, MAX_WARMUP_TRIES + 1):
        ident = Identity()
        if ident.healthy:
            log(f"🧠 Identity ready ({ident.target}, warm-up 200, try {n})")
            return ident
        log(f"🧠 Identity rejected ({ident.target}, warm-up {ident.warm_status}, try {n}/{MAX_WARMUP_TRIES})")
        ident.close()
        time.sleep(random.uniform(*IDENTITY_SWAP_DELAY))
    pause = random.uniform(*EXHAUSTED_PAUSE)
    log(f"⚠️ {MAX_WARMUP_TRIES} identities rejected in a row – pausing {pause:.0f}s")
    time.sleep(pause)
    return Identity()


def get_identity():
    ident = getattr(thread_local, "identity", None)
    if ident is not None and IDENTITY_ROTATE_EVERY and ident.requests_made >= IDENTITY_ROTATE_EVERY:
        log(f"🔄 Identity rotated (every {IDENTITY_ROTATE_EVERY} requests)")
        reset_identity()
        ident = None
    if ident is None:
        ident = new_healthy_identity()
        thread_local.identity = ident
    ident.requests_made += 1
    return ident


def reset_identity():
    ident = getattr(thread_local, "identity", None)
    if ident is not None:
        ident.close()
        del thread_local.identity


def log_egress_ip():
    """Log the runner's public IP so you can compare working vs failing runs."""
    try:
        if HAVE_CFFI:
            ip = cffi_requests.get(
                "https://api.ipify.org", timeout=10, proxies=PROXIES).text
        else:
            ip = cloudscraper.create_scraper().get("https://api.ipify.org", timeout=10).text
        log(f"🌐 Egress IP: {ip}")
    except Exception as e:
        log(f"🌐 Egress IP lookup failed: {type(e).__name__}")


def log_block_diagnostics(r):
    """Log once who is blocking us (Cloudflare / Akamai / app)."""
    global _diag_logged
    if _diag_logged:
        return
    _diag_logged = True
    h = {k.lower(): v for k, v in r.headers.items()}
    interesting = {k: h[k] for k in (
        "server", "cf-ray", "x-akamai-request-id", "x-cache", "via") if k in h}
    snippet = r.text[:200].replace("\n", " ")
    log(f"🔎 Block diagnostics | status={r.status_code} | headers={interesting} | body={snippet}")


@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(venue_code, ident):
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    r = ident.session.get(
        url, headers=ident.api_headers(), timeout=API_TIMEOUT)
    if r.status_code in (403, 429):
        log_block_diagnostics(r)
        raise BlockedError(f"HTTP {r.status_code}")
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    text = r.text.strip()
    if not text.startswith("{"):
        log_block_diagnostics(r)
        snippet = text[:200].replace("\n", " ")
        raise BlockedError(f"Blocked / HTML (snippet: {snippet})")
    return r.json()


def fetch_with_retry(venue_code, attempts=MAX_ATTEMPTS):
    """Single retry layer. Blocks get a longer backoff and a fresh identity."""
    for attempt in range(1, attempts + 1):
        try:
            ident = get_identity()          # outside the hard timeout (may need several warm-ups)
            return fetch_api_raw(venue_code, ident)
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


# =====================================================
# PARSER
# =====================================================
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


def dedupe(rows):
    seen = set()
    out = []
    for r in rows:
        key = (r["venue"], r["time"], r["session_id"], r["audi"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def process_venue(vcode, venues, all_rows, attempts=MAX_ATTEMPTS):
    raw = fetch_with_retry(vcode, attempts=attempts)
    rows = parse_payload(raw)
    for r in rows:
        r["city"] = venues[vcode].get("City", "Unknown")
        r["state"] = venues[vcode].get("State", "Unknown")
        r["source"] = "BMS"
        r["date"] = DATE_CODE
    all_rows.extend(rows)


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    log(f"🚀 SCRIPT STARTED | shard={SHARD_ID} | date={DATE_CODE} | "
        f"client={'curl_cffi' if HAVE_CFFI else 'cloudscraper'} | proxy={'on' if PROXIES else 'off'}")
    if not HAVE_CFFI:
        log("⚠️ curl_cffi not installed – run `pip install -U curl_cffi` for far fewer 403s")

    log_egress_ip()

    # Spread out parallel shards so they don't all hit BMS in the same second
    jitter = random.uniform(0, START_JITTER_MAX)
    log(f"⏳ Startup jitter {jitter:.1f}s")
    time.sleep(jitter)

    with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
        venues = json.load(f)

    all_rows = []
    retry = set()
    failure_count = 0
    cooldowns_in_a_row = 0
    blocked_out = False
    venue_list = list(venues)

    # ------ INITIAL PASS ------
    for i, vcode in enumerate(venue_list, 1):
        log(f"[{i}/{len(venue_list)}] {vcode}")
        delay = min(BASE_DELAY + failure_count * 0.2, MAX_DELAY)
        time.sleep(random.uniform(delay, delay + 0.3))

        try:
            process_venue(vcode, venues, all_rows)
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
                    log("⛔ Still blocked after repeated cooldowns – this runner's IP is blocked. "
                        "Stopping early. Set BMS_PROXY or stagger shards.")
                    retry.update(venue_list[i:])
                    blocked_out = True
                    break
                log(f"⛔ {failure_count} consecutive failures – cooling down {COOLDOWN_SECONDS}s")
                time.sleep(COOLDOWN_SECONDS)
                failure_count = 0
                reset_identity()

    # ------ FINAL RETRY PASSES (skipped if clearly IP-blocked) ------
    if not blocked_out:
        for rnd in range(1, FINAL_RETRY_ROUNDS + 1):
            if not retry:
                break
            log(f"🔁 RETRY ROUND {rnd} | Remaining: {len(retry)}")
            time.sleep(random.uniform(20, 40))
            current_retry = list(retry)
            retry.clear()
            for vcode in current_retry:
                log(f"[RETRY-{rnd}] {vcode}")
                time.sleep(random.uniform(1.0, 2.5))
                try:
                    process_venue(vcode, venues, all_rows, attempts=2)
                except Exception as e:
                    retry.add(vcode)
                    log(f"❌ RETRY FAIL {vcode} | {type(e).__name__}")

    if retry:
        log(f"⚠️ FINAL FAILED VENUES: {len(retry)} / {len(venue_list)}")
        log(f"FAILED LIST: {sorted(retry)}")

    # Never overwrite good data in R2 with an empty result
    if not all_rows:
        log("🚫 No rows collected – NOT uploading to R2 (keeping existing data). Failing the job.")
        sys.exit(1)

    log("🧹 Deduping shows")
    detailed = dedupe(all_rows)

    # =====================================================
    # SUMMARY (AFTER DEDUPE)
    # =====================================================
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
            m["details"][ck] = {
                "city": city, "state": state, "venues": set(),
                "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
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
                "chain": chain, "venues": set(),
                "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
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

    # =====================================================
    # FINALIZE SUMMARY
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
            "details": [],
            "Chain_details": []
        }

        for d in m["details"].values():
            final_summary[movie]["details"].append({
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
            final_summary[movie]["Chain_details"].append({
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

    # =====================================================
    # SAVE TO R2
    # =====================================================
    r2_upload_json(R2_DETAILED_KEY, detailed)
    r2_upload_json(R2_SUMMARY_KEY, final_summary)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")
