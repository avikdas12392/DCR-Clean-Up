# DCR-Clean-Up
latlong v1 is without cache memory


latlong v2 is with cache memory: if multiple input rows are within ~100m of each other, you can cache results for a “vicinity key” (e.g., round lat,lon to 4–5 decimals + pin) and reuse the last response for that key instead of re-calling the API. That’s a small in-memory dict (or a tiny SQLite table); 


upgraded script with a vicinity cache that saves credits when consecutive rows are geographically close. It implements a two-level cache:

In-memory dict (fast during a single run).

SQLite cache (persists across runs): if you rerun tomorrow, nearby rows will still reuse yesterday’s results.

Vicinity definition (configurable): by default rounds lat/lon to 3 decimals (~100–120 m) and includes pin, radius, and num in the key.

What changed (quick)

Checks vicinity_cache before calling Serper; if found → reuses results and prints ✅ Cache hit.

If not found → calls Serper, then stores the JSON in both caches.

Keeps: per-row dedupe, global dedupe for output 1, fuzzy matching to build output 2, robust progress logging/resume


What changed (per your ask)

Search payload uses: Associate Hospital (as the query base), OLat, OLong, pin — with gl="in" and optional radius.

Output 1 now includes: Rating, Reviews (and keeps prior fields).

Output 2 = all input columns (including OLat, OLong) + Output 1 fields (Lat, Long, etc.) + FuzzyScore.

Keeps resumability, per-row + global dedupe, and vicinity cache.

Reset behavior for a fresh batch

Clear progress only: delete progress_log.json.

Clear global dedupe (allow re-writing Output 1 for previously-seen places): delete serper_seen.sqlite3.

Clear cache (force fresh API calls): delete vicinity_cache.sqlite3.

If you want clean headers in outputs: delete output 1.xlsx and/or output 2.xlsx.
