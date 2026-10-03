"""
Independent completeness check for the downloaded MTA OD Parquet files.

Does NOT trust _manifest.json or download.log: every expected value is re-queried
live from the SODA API and compared against what is actually on disk.

Checks
  1. dataset freshness (did the source change since the download?)
  2. total row count            local vs API
  3. per-month row count        local vs API
  4. per-month ridership sum    local vs API   (catches wrong/duplicated rows)
  5. every timestamp the API lists is present, each with the exact API row count
  6. no duplicate (timestamp, origin, destination) keys  (catches paging overlap)
  7. no nulls in any column; value ranges sane
  8. distinct origin/destination station counts vs API
"""
import glob
import json
import os
import sys

import pandas as pd
import pyarrow.parquet as pq
import requests

BASE = "https://data.ny.gov/resource/28vm-gjqr"
VIEW = "https://data.ny.gov/api/views/28vm-gjqr.json"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # this file lives in code/
OUT_DIR = os.path.join(ROOT, "data", "mta_subway_od")

fails = []


def check(label, ok, detail=""):
    mark = "PASS" if ok else "FAIL"
    print("  [%s] %s%s" % (mark, label, ("  " + detail) if detail else ""), flush=True)
    if not ok:
        fails.append(label)


def api(params):
    r = requests.get(BASE + ".json", params=params, timeout=180)
    r.raise_for_status()
    return r.json()


files = sorted(glob.glob(os.path.join(OUT_DIR, "mta_subway_od_2026-*.parquet")))
print("local files: %d" % len(files))

# ---------------------------------------------------------------- 1. freshness
meta = requests.get(VIEW, timeout=60).json()
updated = pd.to_datetime(meta["rowsUpdatedAt"], unit="s", utc=True)
newest_local = max(os.path.getmtime(f) for f in files)
newest_local = pd.to_datetime(newest_local, unit="s", utc=True)
print("\n1. source freshness")
print("     source rows last updated : %s UTC" % updated.strftime("%Y-%m-%d %H:%M"))
print("     local files written      : %s UTC" % newest_local.strftime("%Y-%m-%d %H:%M"))
check("source unchanged since download", updated <= newest_local,
      "" if updated <= newest_local else "SOURCE UPDATED AFTER DOWNLOAD - re-run needed")

# ------------------------------------------------------------ 2/3/4. counts+sums
print("\n2-4. row counts and ridership sums, per month")
api_months = {r["month"].zfill(2): (int(r["n"]), float(r["s"])) for r in api(
    {"$select": "month, count(1) as n, sum(estimated_average_ridership) as s",
     "$group": "month", "$order": "month", "$limit": 100})}

local_total = api_total = 0
print("     month     local rows       api rows   local sum        api sum")
for f in files:
    month = os.path.basename(f).split("_")[-1].replace(".parquet", "")
    mm = month[-2:]
    tbl = pq.read_table(f, columns=["estimated_average_ridership"])
    n_local = tbl.num_rows
    s_local = pd.Series(tbl.column(0).to_numpy()).sum()
    n_api, s_api = api_months.get(mm, (0, 0.0))
    local_total += n_local
    api_total += n_api
    same = (n_local == n_api) and abs(s_local - s_api) < max(1e-3, abs(s_api) * 1e-9)
    print("     %s  %13s  %13s  %10.1f  %13.1f  %s"
          % (month, format(n_local, ","), format(n_api, ","),
             s_local, s_api, "OK" if same else "<-- MISMATCH"))
    if not same:
        fails.append("month %s" % month)

check("every month matches the API (count + sum)",
      not any(x.startswith("month ") for x in fails))
check("total row count", local_total == api_total,
      "%s local vs %s api" % (format(local_total, ","), format(api_total, ",")))
check("no extra months on the server", len(api_months) == len(files),
      "%d api months vs %d local files" % (len(api_months), len(files)))

# --------------------------------------------------- 5. every timestamp present
print("\n5. per-timestamp coverage (re-queried from the API)")
api_ts = {r["timestamp"]: int(r["n"]) for r in api(
    {"$select": "timestamp, count(1) as n", "$group": "timestamp",
     "$order": "timestamp", "$limit": 5000})}
local_ts = {}
for f in files:
    tbl = pq.read_table(f, columns=["timestamp"])
    vc = pd.Series(tbl.column(0).to_pandas()).value_counts()
    for ts, n in vc.items():
        key = ts.strftime("%Y-%m-%dT%H:%M:%S.000")
        local_ts[key] = local_ts.get(key, 0) + int(n)

missing = sorted(set(api_ts) - set(local_ts))
extra = sorted(set(local_ts) - set(api_ts))
wrong = sorted(k for k in set(api_ts) & set(local_ts) if api_ts[k] != local_ts[k])
print("     api timestamps: %d   local timestamps: %d" % (len(api_ts), len(local_ts)))
check("no missing timestamps", not missing, str(missing[:5]) if missing else "")
check("no unexpected timestamps", not extra, str(extra[:5]) if extra else "")
check("every timestamp has the exact API row count", not wrong,
      str([(k, api_ts[k], local_ts[k]) for k in wrong[:5]]) if wrong else "")

dates = sorted({k[:10] for k in local_ts})
hours_per_date = {d: sum(1 for k in local_ts if k[:10] == d) for d in dates}
bad_days = {d: h for d, h in hours_per_date.items() if h != 24}
check("every covered day has all 24 hours", not bad_days, str(bad_days) if bad_days else "")
print("     %d distinct dates, %s .. %s" % (len(dates), dates[0], dates[-1]))

# ---------------------------------------------- 6/7. duplicates, nulls, ranges
print("\n6-7. integrity per month (duplicate keys, nulls, value ranges)")
key_cols = ["timestamp", "origin_station_complex_id", "destination_station_complex_id"]
all_cols = [f.name for f in pq.ParquetFile(files[0]).schema_arrow]
dup_bad = null_bad = range_bad = []
dup_bad, null_bad, range_bad = [], [], []
o_ids, d_ids = set(), set()
for f in files:
    month = os.path.basename(f).split("_")[-1].replace(".parquet", "")
    df = pq.read_table(f, columns=key_cols).to_pandas()
    ndup = int(df.duplicated().sum())
    if ndup:
        dup_bad.append((month, ndup))
    o_ids |= set(df.origin_station_complex_id.unique().tolist())
    d_ids |= set(df.destination_station_complex_id.unique().tolist())
    del df
    nulls = {}
    ranges_ok = True
    for c in all_cols:
        col = pq.read_table(f, columns=[c]).column(0)
        if col.null_count:
            nulls[c] = col.null_count
        if c == "estimated_average_ridership":
            s = pd.Series(col.to_numpy())
            if s.min() < 0 or not s.notna().all():
                ranges_ok = False
        if c == "hour_of_day":
            s = pd.Series(col.to_numpy())
            if s.min() < 0 or s.max() > 23:
                ranges_ok = False
        if c in ("origin_latitude", "destination_latitude"):
            s = pd.Series(col.to_numpy())
            if s.min() < 40.0 or s.max() > 41.5:
                ranges_ok = False
        del col
    if nulls:
        null_bad.append((month, nulls))
    if not ranges_ok:
        range_bad.append(month)
    print("     %s  dup keys=%d  nulls=%s  ranges=%s"
          % (month, ndup, nulls or "none", "ok" if ranges_ok else "BAD"), flush=True)

check("no duplicate (timestamp, origin, destination) rows", not dup_bad, str(dup_bad))
check("no null values in any column", not null_bad, str(null_bad))
check("hour/lat/ridership values in range", not range_bad, str(range_bad))

# ------------------------------------------------------- 8. station coverage
print("\n8. station coverage")
# count(distinct) over 83M rows times out server-side; grouping and counting the
# groups returns the same answer cheaply.
a_o = {int(r["origin_station_complex_id"]) for r in api(
    {"$select": "origin_station_complex_id", "$group": "origin_station_complex_id",
     "$limit": 50000})}
a_d = {int(r["destination_station_complex_id"]) for r in api(
    {"$select": "destination_station_complex_id", "$group": "destination_station_complex_id",
     "$limit": 50000})}
check("distinct origin stations", o_ids == a_o,
      "%d local vs %d api" % (len(o_ids), len(a_o)))
check("distinct destination stations", d_ids == a_d,
      "%d local vs %d api" % (len(d_ids), len(a_d)))

# ------------------------------------------------------------------- verdict
print("\n" + "=" * 62)
if fails:
    print("INCOMPLETE - %d check(s) failed:" % len(fails))
    for x in fails:
        print("   - %s" % x)
    sys.exit(1)
print("COMPLETE - all checks passed. %s rows across %d monthly files."
      % (format(local_total, ","), len(files)))
