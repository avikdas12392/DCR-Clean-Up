#!/usr/bin/env python3
import json
import time
import math
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests
import pandas as pd

# Optional fuzzy lib; fallback to difflib if unavailable
try:
    from rapidfuzz.fuzz import ratio as fuzz_ratio
    def fuzzy_score(a: str, b: str) -> float:
        return float(fuzz_ratio(a or "", b or ""))
except Exception:
    from difflib import SequenceMatcher
    def fuzzy_score(a: str, b: str) -> float:
        return 100.0 * SequenceMatcher(None, (a or ""), (b or "")).ratio()

# ========= CONFIG =========
API_KEY = "2e823b299673ed947dde2960a6b609da1f104d15"        # Serper API key
ENDPOINT = "https://google.serper.dev/maps"
INPUT_FILE = Path("input.xlsx")
OUTPUT1_FILE = Path("output 1.xlsx")
OUTPUT2_FILE = Path("output 2.xlsx")
PROGRESS_LOG = Path("progress_log.json")

# Per-call constraints
NUM_RESULTS = 20
RADIUS_METERS = 100
TIMEOUT = 30
BASE_SLEEP = 0.8
BACKOFF_FACTOR = 1.6
MAX_RETRIES = 4

# Fuzzy match threshold
FUZZY_THRESHOLD = 75.0

# Vicinity cache granularity: decimals=3 ≈ ~100–120 m; adjust to 4 for ~10–12 m
VICINITY_DECIMALS = 3

# Schema for output files
OUTPUT1_COLS = [
    "Name","Address","Lat","Long","Website","Pincode","Category","CID","InputRowIndex"
]
INPUT_COLS = [
    "HQ Name","Doctor Contact ID","UIN No","Customer Name","Associate Hospital",
    "Long","Lat","Tagged Address","pin"
]
OUTPUT2_COLS = INPUT_COLS + OUTPUT1_COLS + ["FuzzyScore"]

# SQLite files
DEDUP_DB_FILE = Path("serper_seen.sqlite3")      # global dedupe for output 1
CACHE_DB_FILE = Path("vicinity_cache.sqlite3")  # persistent vicinity cache

# ========= Utilities =========
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def load_progress() -> Dict[str, Any]:
    if not PROGRESS_LOG.exists():
        return {"last_processed_input_row": -1, "errors": []}
    try:
        return json.loads(PROGRESS_LOG.read_text(encoding="utf-8"))
    except Exception as e:
        corrupt = PROGRESS_LOG.with_suffix(".corrupt.json")
        PROGRESS_LOG.replace(corrupt)
        return {"last_processed_input_row": -1, "errors": [{"row": -1, "error": f"Corrupt progress: {e}", "time": now_iso()}]}

def save_progress(progress: Dict[str, Any]) -> None:
    tmp = PROGRESS_LOG.with_suffix(".tmp")
    tmp.write_text(json.dumps(progress, indent=2), encoding="utf-8")
    tmp.replace(PROGRESS_LOG)

def log_error(progress: Dict[str, Any], row_idx: int, err: str) -> None:
    progress.setdefault("errors", []).append({"row": row_idx, "error": err, "time": now_iso()})
    save_progress(progress)

def print_call_purpose(row_idx: int, lat: Any, lon: Any, pin: str, using_cache: bool = False):
    src = "Cache" if using_cache else "Serper Maps API"
    print(f"[{now_iso()}] Row {row_idx}: Finding 'hospital' within {RADIUS_METERS}m via {src}. "
          f"Inputs: lat={lat}, long={lon}, pin={pin}. Limit={NUM_RESULTS}")

def extract_pincode(address: str) -> str:
    if not address:
        return ""
    m = re.search(r"\b(\d{6})\b", address)
    if m:
        return m.group(1)
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) >= 2:
        candidate = parts[-2]
        m2 = re.search(r"\b(\d{6})\b", candidate)
        if m2:
            return m2.group(1)
    return ""

def stable_place_key(p: Dict[str, Any]) -> str:
    """Prefer CID; fallback to normalized name+address (lowercased)."""
    cid = (p.get("cid") or "").strip()
    if cid:
        return f"cid:{cid}"
    name = (p.get("title") or "").strip().lower()
    addr = (p.get("address") or "").strip().lower()
    return f"na:{name}|{addr}"

def ensure_excel_with_sheet(path: Path, columns: List[str], sheet_name: str = "Sheet1") -> None:
    if path.exists():
        return
    df = pd.DataFrame(columns=columns)
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as xlw:
        df.to_excel(xlw, index=False, sheet_name=sheet_name)

def append_rows_excel(path: Path, rows: List[Dict[str, Any]], columns: List[str], sheet_name: str = "Sheet1") -> None:
    if not rows:
        return
    ensure_excel_with_sheet(path, columns, sheet_name)
    existing = pd.read_excel(path, sheet_name=sheet_name)
    startrow = len(existing) + 1
    df = pd.DataFrame(rows, columns=columns)
    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="overlay") as xlw:
        df.to_excel(xlw, index=False, header=False, startrow=startrow, sheet_name=sheet_name)

def to_float(x: Any) -> Any:
    try:
        return float(x)
    except Exception:
        return ""

# ========= Global dedupe (output 1) =========
def open_dedupe_db() -> sqlite3.Connection:
    db = sqlite3.connect(DEDUP_DB_FILE)
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA synchronous=NORMAL;")
    db.execute("""
        CREATE TABLE IF NOT EXISTS places_seen (
            key TEXT PRIMARY KEY,
            first_seen_at INTEGER DEFAULT (strftime('%s','now'))
        );
    """)
    db.commit()
    return db

def is_new_globally_and_mark(db: sqlite3.Connection, place_key: str) -> bool:
    try:
        db.execute("INSERT INTO places_seen (key) VALUES (?);", (place_key,))
        db.commit()
        return True   # inserted → new globally
    except sqlite3.IntegrityError:
        return False  # already present

# ========= Vicinity cache (two-level) =========
_in_memory_cache: Dict[str, Dict[str, Any]] = {}

def open_cache_db() -> sqlite3.Connection:
    db = sqlite3.connect(CACHE_DB_FILE)
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA synchronous=NORMAL;")
    db.execute("""
        CREATE TABLE IF NOT EXISTS vicinity_cache (
            vkey TEXT PRIMARY KEY,
            response_json TEXT NOT NULL,
            last_used INTEGER DEFAULT (strftime('%s','now'))
        );
    """)
    db.commit()
    return db

def make_vicinity_key(lat: Any, lon: Any, pin: str) -> str:
    try:
        lat_f = float(lat)
    except Exception:
        lat_f = None
    try:
        lon_f = float(lon)
    except Exception:
        lon_f = None
    # Include pin, radius, and num to avoid accidental cross-condition reuse
    lat_part = f"{round(lat_f, VICINITY_DECIMALS)}" if lat_f is not None else "NA"
    lon_part = f"{round(lon_f, VICINITY_DECIMALS)}" if lon_f is not None else "NA"
    return f"{pin}|{lat_part}|{lon_part}|r{RADIUS_METERS}|n{NUM_RESULTS}"

def cache_get(db: sqlite3.Connection, vkey: str) -> Optional[Dict[str, Any]]:
    # 1) In-memory
    js = _in_memory_cache.get(vkey)
    if js is not None:
        return js
    # 2) SQLite
    cur = db.execute("SELECT response_json FROM vicinity_cache WHERE vkey=?;", (vkey,))
    row = cur.fetchone()
    if row:
        try:
            js = json.loads(row[0])
            _in_memory_cache[vkey] = js
            db.execute("UPDATE vicinity_cache SET last_used=strftime('%s','now') WHERE vkey=?;", (vkey,))
            db.commit()
            return js
        except Exception:
            return None
    return None

def cache_set(db: sqlite3.Connection, vkey: str, js: Dict[str, Any]) -> None:
    payload = json.dumps(js, separators=(",", ":"))  # compact
    _in_memory_cache[vkey] = js
    db.execute(
        "INSERT OR REPLACE INTO vicinity_cache (vkey, response_json, last_used) VALUES (?, ?, strftime('%s','now'));",
        (vkey, payload),
    )
    db.commit()

# ========= Serper call with cache =========
def call_serper_or_cache(vdb: sqlite3.Connection, keyword: str, lat: Any, lon: Any, pin: str) -> Optional[Dict[str, Any]]:
    vkey = make_vicinity_key(lat, lon, pin)
    # Try cache
    js = cache_get(vdb, vkey)
    if js is not None:
        print_call_purpose(-1, lat, lon, pin, using_cache=True)
        print(f"[{now_iso()}]   ✅ Cache hit: {len(js.get('places', []) or [])} places.")
        return js

    # No cache → call API
    headers = {"X-API-KEY": API_KEY, "Content-Type": "application/json"}
    q = f"{keyword} near {lat},{lon} {pin}".strip()
    payload = {"q": q, "num": NUM_RESULTS, "start": 0, "ll": f"{lat},{lon}", "radius": RADIUS_METERS}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(ENDPOINT, headers=headers, json=payload, timeout=TIMEOUT)
            resp.raise_for_status()
            js = resp.json()
            if not isinstance(js, dict) or "places" not in js:
                raise ValueError("Unexpected response structure (no 'places').")
            print(f"[{now_iso()}]   ✅ API OK: received {len(js.get('places', []) or [])} places.")
            cache_set(vdb, vkey, js)
            return js
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"[{now_iso()}]   ❌ API failed after {MAX_RETRIES} attempts: {e}")
                return None
            sleep_for = BASE_SLEEP * (BACKOFF_FACTOR ** (attempt - 1))
            print(f"[{now_iso()}]   ~ Retry {attempt}/{MAX_RETRIES} in {sleep_for:.1f}s due to: {e}")
            time.sleep(sleep_for)
    return None

# ========= Main =========
def main():
    # Ensure outputs
    ensure_excel_with_sheet(OUTPUT1_FILE, OUTPUT1_COLS)
    ensure_excel_with_sheet(OUTPUT2_FILE, OUTPUT2_COLS)

    # Progress & input
    progress = load_progress()
    last_done = int(progress.get("last_processed_input_row", -1))

    try:
        df_in = pd.read_excel(INPUT_FILE, dtype=str)
    except Exception as e:
        print(f"[{now_iso()}] FATAL: cannot read input: {e}")
        progress["last_processed_input_row"] = last_done
        log_error(progress, -1, f"Input read error: {e}")
        return

    # Databases
    dedupe_db = open_dedupe_db()
    cache_db = open_cache_db()

    n = len(df_in)
    print(f"[{now_iso()}] Loaded input with {n} rows. Resuming from row {last_done + 1}.")

    for idx in range(last_done + 1, n):
        row = df_in.iloc[idx].fillna("")
        try:
            lat_raw = row.get("Lat", "")
            lon_raw = row.get("Long", "")
            tagged_addr = row.get("Tagged Address", "")
            pin = row.get("pin", "")

            lat = lat_raw
            lon = lon_raw
            try: lat = float(lat_raw)
            except Exception: pass
            try: lon = float(lon_raw)
            except Exception: pass

            # Announce purpose (minimal inputs)
            print_call_purpose(idx, lat, lon, pin, using_cache=False)

            # Get results from cache or API
            js = call_serper_or_cache(cache_db, "hospital", lat, lon, pin)
            if js is None:
                log_error(progress, idx, "API call failed after retries")
                progress["last_processed_input_row"] = idx
                save_progress(progress)
                continue

            places = js.get("places", []) or []

            # Per-row dedupe AND global dedupe for output 1
            seen_keys_row = set()
            out1_rows: List[Dict[str, Any]] = []
            out2_rows: List[Dict[str, Any]] = []

            for p in places[:NUM_RESULTS]:
                row_key = stable_place_key(p)
                if row_key in seen_keys_row:
                    continue
                seen_keys_row.add(row_key)

                name = p.get("title", "")
                addr = p.get("address", "")
                lat2 = to_float(p.get("latitude", ""))
                lon2 = to_float(p.get("longitude", ""))
                website = p.get("website", "") or ""
                cid = (p.get("cid") or "").strip()
                category = p.get("category", "") or p.get("type", "")
                pinc = extract_pincode(addr)

                r1 = {
                    "Name": name,
                    "Address": addr,
                    "Lat": lat2,
                    "Long": lon2,
                    "Website": website,
                    "Pincode": pinc,
                    "Category": category,
                    "CID": cid,
                    "InputRowIndex": idx
                }

                # Global dedupe for output 1
                is_new_global = is_new_globally_and_mark(dedupe_db, row_key)
                if is_new_global:
                    out1_rows.append(r1)

                # Fuzzy match for output 2 (always evaluate)
                score = fuzzy_score(addr.lower(), str(tagged_addr).lower())
                if score >= FUZZY_THRESHOLD:
                    left = {col: row.get(col, "") for col in INPUT_COLS}
                    out2_rows.append({**left, **r1, "FuzzyScore": round(score, 2)})

            # Incremental writes
            if out1_rows:
                append_rows_excel(OUTPUT1_FILE, out1_rows, OUTPUT1_COLS)
            if out2_rows:
                append_rows_excel(OUTPUT2_FILE, out2_rows, OUTPUT2_COLS)

            print(f"[{now_iso()}]   ✅ Wrote {len(out1_rows)} new uniques to 'output 1', "
                  f"{len(out2_rows)} fuzzy matches to 'output 2'.")

            # Progress
            progress["last_processed_input_row"] = idx
            save_progress(progress)

        except Exception as e:
            print(f"[{now_iso()}]   ❌ Row {idx} error: {e}")
            log_error(progress, idx, str(e))
            progress["last_processed_input_row"] = idx
            save_progress(progress)
            continue

    print(f"[{now_iso()}] All done. Progress saved to {PROGRESS_LOG.name}.")


if __name__ == "__main__":
    main()
