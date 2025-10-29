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
