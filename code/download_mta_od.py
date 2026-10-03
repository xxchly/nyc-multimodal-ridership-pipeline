"""
Download the full MTA Subway Origin-Destination Ridership Estimate (2026) dataset
from the NY Open Data SODA API, in batches, into monthly Parquet files.

Dataset: https://data.ny.gov/Transportation/MTA-Subway-Origin-Destination-Ridership-Estimate-B/28vm-gjqr

Strategy: the data contains 1,344 distinct `timestamp` values (56 representative
days x 24 hours). Each timestamp is fetched as its own batch (<= ~110k rows), so
pagination offsets stay shallow and every batch is independently resumable and
verifiable against the server-side row count.

Stage 1: one Parquet file per timestamp under data/mta_subway_od/_staging/
Stage 2: merge into one Parquet file per month (streamed, low memory)

Usage:
    python download_mta_od.py              # download + merge (resumable, just re-run)
    python download_mta_od.py --merge-only # rebuild monthly files from _staging/
"""
import io
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE = "https://data.ny.gov/resource/28vm-gjqr"
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")  # optional, raises rate limits
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # this file lives in code/
OUT_DIR = os.path.join(ROOT, "data", "mta_subway_od")
STAGING = os.path.join(OUT_DIR, "_staging")
MANIFEST = os.path.join(OUT_DIR, "_manifest.json")
PAGE = 50000
WORKERS = 6

COLS = [
    "year", "month", "day_of_week", "hour_of_day", "timestamp",
    "origin_station_complex_id", "origin_station_complex_name",
    "origin_latitude", "origin_longitude",
    "destination_station_complex_id", "destination_station_complex_name",
    "destination_latitude", "destination_longitude",
    "estimated_average_ridership",
]
# origin_point / destination_point are intentionally dropped: they are just
# "POINT (lon lat)" restatements of the latitude/longitude columns.

SCHEMA = pa.schema([
    ("year", pa.int16()),
    ("month", pa.int8()),
    ("day_of_week", pa.string()),
    ("hour_of_day", pa.int8()),
    ("timestamp", pa.timestamp("s")),
    ("origin_station_complex_id", pa.int32()),
    ("origin_station_complex_name", pa.string()),
    ("origin_latitude", pa.float64()),
    ("origin_longitude", pa.float64()),
    ("destination_station_complex_id", pa.int32()),
    ("destination_station_complex_name", pa.string()),
    ("destination_latitude", pa.float64()),
    ("destination_longitude", pa.float64()),
    ("estimated_average_ridership", pa.float64()),
])

DTYPES = {
    "year": "int16", "month": "int8", "hour_of_day": "int8",
    "day_of_week": "string",
    "origin_station_complex_id": "int32", "destination_station_complex_id": "int32",
    "origin_station_complex_name": "string", "destination_station_complex_name": "string",
    "origin_latitude": "float64", "origin_longitude": "float64",
    "destination_latitude": "float64", "destination_longitude": "float64",
    "estimated_average_ridership": "float64",
}

session_local = threading.local()
print_lock = threading.Lock()


def sess():
    s = getattr(session_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers["User-Agent"] = "mta-od-bulk-download/1.0"
        if APP_TOKEN:
            s.headers["X-App-Token"] = APP_TOKEN
        session_local.s = s
    return s


def get(url, params, attempts=6):
    last = None
    for a in range(attempts):
        try:
            r = sess().get(url, params=params, timeout=180)
            if r.status_code == 200:
                return r.text
            if r.status_code in (202, 429, 500, 502, 503, 504):
                last = "HTTP %d" % r.status_code
            else:
                r.raise_for_status()
        except Exception as e:  # network layer: retry everything
            last = repr(e)
        time.sleep(min(60, 3 * 2 ** a))
    raise RuntimeError("giving up after %d attempts: %s" % (attempts, last))


def build_manifest():
    """Distinct timestamps + their server-side row counts (the batch plan)."""
    if os.path.exists(MANIFEST):
        with open(MANIFEST, encoding="utf-8") as f:
            return json.load(f)
    txt = get(BASE + ".json", {
        "$select": "timestamp,count(1) as n", "$group": "timestamp",
        "$order": "timestamp", "$limit": 5000,
    })
    rows = [{"timestamp": r["timestamp"], "n": int(r["n"])} for r in json.loads(txt)]
    with open(MANIFEST, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=1)
    return rows


def stage_path(ts):
    key = ts.replace("-", "").replace(":", "").replace(".000", "")
    return os.path.join(STAGING, key + ".parquet")


def fetch_batch(ts, expected):
    """Fetch every row for one timestamp, paging until the count matches."""
    frames, offset = [], 0
    while offset < expected:
        txt = get(BASE + ".csv", {
            "$select": ",".join(COLS),
            "$where": "timestamp = '%s'" % ts,
            "$order": ":id",
            "$limit": PAGE,
            "$offset": offset,
        })
        df = pd.read_csv(io.StringIO(txt), dtype=DTYPES, parse_dates=["timestamp"])
        if df.empty:
            break
        frames.append(df)
        offset += len(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLS)
    if len(out) != expected:
        raise RuntimeError("%s: got %d rows, expected %d" % (ts, len(out), expected))
    return out[COLS]


def worker(job):
    ts, expected = job["timestamp"], job["n"]
    path = stage_path(ts)
    if os.path.exists(path):
        try:
            if pq.ParquetFile(path).metadata.num_rows == expected:
                return ts, expected, True  # already downloaded
        except Exception:
            os.remove(path)
    df = fetch_batch(ts, expected)
    tbl = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
    tmp = path + ".part"
    pq.write_table(tbl, tmp, compression="zstd", compression_level=6)
    os.replace(tmp, path)
    return ts, expected, False


def download():
    os.makedirs(STAGING, exist_ok=True)
    jobs = build_manifest()
    total = sum(j["n"] for j in jobs)
    print("%d batches, %s rows expected" % (len(jobs), format(total, ",")), flush=True)

    done = {"batches": 0, "rows": 0, "cached": 0}
    t0 = time.time()
    errors = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(worker, j): j for j in jobs}
        for fut in as_completed(futs):
            j = futs[fut]
            try:
                ts, n, cached = fut.result()
            except Exception as e:
                errors.append((j["timestamp"], repr(e)))
                with print_lock:
                    print("  FAILED %s: %s" % (j["timestamp"], e), flush=True)
                continue
            done["batches"] += 1
            done["rows"] += n
            done["cached"] += int(cached)
            if done["batches"] % 20 == 0 or done["batches"] == len(jobs):
                el = time.time() - t0
                pct = done["rows"] / total * 100
                eta = el / max(pct, 1e-9) * (100 - pct)
                with print_lock:
                    print("  [%4d/%d] %12s rows (%5.1f%%)  elapsed %5.1fm  eta %5.1fm"
                          % (done["batches"], len(jobs), format(done["rows"], ","),
                             pct, el / 60, eta / 60), flush=True)
    if errors:
        print("\n%d batches failed; re-run this script to retry them." % len(errors), flush=True)
        return False
    print("\nall %d batches staged (%d were already on disk)"
          % (len(jobs), done["cached"]), flush=True)
    return True


def merge():
    """Stream the staged batches into one Parquet file per month."""
    jobs = build_manifest()
    by_month = {}
    for j in jobs:
        by_month.setdefault(j["timestamp"][:7], []).append(j)
    print("\nmerging monthly files:", flush=True)
    grand = 0
    for month in sorted(by_month):
        out = os.path.join(OUT_DIR, "mta_subway_od_%s.parquet" % month)
        writer, rows = None, 0
        try:
            for j in sorted(by_month[month], key=lambda x: x["timestamp"]):
                tbl = pq.read_table(stage_path(j["timestamp"]), schema=SCHEMA)
                if writer is None:
                    writer = pq.ParquetWriter(out + ".part", SCHEMA,
                                              compression="zstd", compression_level=6)
                writer.write_table(tbl)
                rows += tbl.num_rows
        finally:
            if writer is not None:
                writer.close()
        os.replace(out + ".part", out)
        expected = sum(j["n"] for j in by_month[month])
        ok = "OK" if rows == expected else "MISMATCH (expected %s)" % format(expected, ",")
        grand += rows
        print("  %s  %11s rows  %8.1f MB  %s"
              % (os.path.basename(out), format(rows, ","),
                 os.path.getsize(out) / 1e6, ok), flush=True)
    print("  total %s rows" % format(grand, ","), flush=True)


if __name__ == "__main__":
    if "--merge-only" not in sys.argv:
        if not download():
            sys.exit(1)
    merge()
