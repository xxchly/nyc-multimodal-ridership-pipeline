"""
Download all 2026 NYC trip data: TLC Yellow Taxi, TLC HVFHV (Uber/Lyft), Citi Bike.

Sources (all public, no key needed):
  TLC      https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
           files at https://d37ci6vzurychx.cloudfront.net/trip-data/
  CitiBike https://citibikenyc.com/system-data
           files at https://s3.amazonaws.com/tripdata/

Layout produced under the project root (this file lives in code/):
  data/tlc_yellow/yellow_tripdata_2026-MM.parquet   (as published, untouched)
  data/tlc_fhvhv/fhvhv_tripdata_2026-MM.parquet     (as published; Uber+Lyft together)
  data/citibike/_zips/YYYYMM-citibike-tripdata.zip  (raw download)
  data/citibike/citibike_2026-MM.parquet            (converted, ~10x smaller)
  reference/taxi_zone_lookup.csv, reference/taxi_zones.zip

Downloads resume: a file whose size already matches the server is skipped, and a
partial file is re-fetched with an HTTP Range request rather than from scratch.

Usage:
    python download_tlc_citibike.py                # everything
    python download_tlc_citibike.py yellow fhvhv   # only those groups
    python download_tlc_citibike.py citibike --convert-only
"""
import os
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # this file lives in code/
OUT = os.path.join(ROOT, "data")
REF = os.path.join(ROOT, "reference")
TLC = "https://d37ci6vzurychx.cloudfront.net"
S3 = "https://s3.amazonaws.com/tripdata/"
YEAR = "2026"
WORKERS = 4
CHUNK = 1 << 20

print_lock = __import__("threading").Lock()


def log(msg):
    with print_lock:
        print(msg, flush=True)


def head(url, tries=4):
    for a in range(tries):
        try:
            r = requests.head(url, timeout=60, allow_redirects=True)
            if r.status_code == 200:
                return int(r.headers.get("content-length", 0))
            if r.status_code in (403, 404):
                return None
        except Exception:
            pass
        time.sleep(3 * 2 ** a)
    return None


def download(url, dest, expect=None, tries=5):
    """Resumable download. Returns (bytes_written, 'cached'|'downloaded')."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    if expect is None:
        expect = head(url)
    if expect and os.path.exists(dest) and os.path.getsize(dest) == expect:
        return expect, "cached"
    for a in range(tries):
        have = os.path.getsize(dest) if os.path.exists(dest) else 0
        if expect and have > expect:
            os.remove(dest)
            have = 0
        headers = {"Range": "bytes=%d-" % have} if have else {}
        mode = "ab" if have else "wb"
        try:
            with requests.get(url, headers=headers, stream=True, timeout=300) as r:
                if have and r.status_code == 200:      # server ignored Range
                    have, mode = 0, "wb"
                elif r.status_code not in (200, 206):
                    raise RuntimeError("HTTP %d" % r.status_code)
                with open(dest, mode) as fh:
                    for chunk in r.iter_content(CHUNK):
                        fh.write(chunk)
            size = os.path.getsize(dest)
            if expect and size != expect:
                raise RuntimeError("size %d != expected %d" % (size, expect))
            return size, "downloaded"
        except Exception as e:
            log("    retry %d/%d %s: %s" % (a + 1, tries, os.path.basename(dest), e))
            time.sleep(5 * 2 ** a)
    raise RuntimeError("failed: %s" % url)


# --------------------------------------------------------------------- TLC
def tlc_jobs(prefix, subdir):
    jobs = []
    for m in range(1, 13):
        name = "%s_tripdata_%s-%02d.parquet" % (prefix, YEAR, m)
        url = "%s/trip-data/%s" % (TLC, name)
        size = head(url)
        if size:
            jobs.append((url, os.path.join(OUT, subdir, name), size))
    return jobs


def run_group(label, jobs):
    if not jobs:
        log("%s: nothing published yet" % label)
        return []
    total = sum(j[2] for j in jobs)
    log("\n%s: %d files, %.1f MB" % (label, len(jobs), total / 1e6))
    got = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(download, u, d, s): (u, d, s) for u, d, s in jobs}
        n = 0
        for fut in as_completed(futs):
            u, d, s = futs[fut]
            size, how = fut.result()
            n += 1
            got.append(d)
            log("  [%d/%d] %-42s %8.1f MB  %s"
                % (n, len(jobs), os.path.basename(d), size / 1e6, how))
    return got


# ---------------------------------------------------------------- Citi Bike
def s3_list():
    keys, tok = [], None
    while True:
        p = {"list-type": "2", "max-keys": "1000"}
        if tok:
            p["continuation-token"] = tok
        r = requests.get(S3, params=p, timeout=120)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        for c in root.findall(ns + "Contents"):
            keys.append((c.find(ns + "Key").text, int(c.find(ns + "Size").text)))
        t = root.find(ns + "NextContinuationToken")
        if t is None:
            return keys
        tok = t.text


CB_SCHEMA = pa.schema([
    ("ride_id", pa.string()),
    ("rideable_type", pa.string()),
    # Citi Bike timestamps carry sub-second precision; "s" would silently be a
    # lossy cast (pyarrow refuses it outright), so keep microseconds.
    ("started_at", pa.timestamp("us")),
    ("ended_at", pa.timestamp("us")),
    ("start_station_name", pa.string()),
    ("start_station_id", pa.string()),
    ("end_station_name", pa.string()),
    ("end_station_id", pa.string()),
    ("start_lat", pa.float64()),
    ("start_lng", pa.float64()),
    ("end_lat", pa.float64()),
    ("end_lng", pa.float64()),
    ("member_casual", pa.string()),
])
CB_STR = ["ride_id", "rideable_type", "start_station_name", "start_station_id",
          "end_station_name", "end_station_id", "member_casual"]


def iter_csvs(zf, path=""):
    """Yield (name, file-like) for every CSV in a zip, recursing into nested zips."""
    for info in zf.infolist():
        n = info.filename
        if info.is_dir() or "__MACOSX" in n or os.path.basename(n).startswith("."):
            continue
        low = n.lower()
        if low.endswith(".csv"):
            yield path + n, zf.open(info)
        elif low.endswith(".zip"):
            import io
            with zf.open(info) as fh:
                inner = zipfile.ZipFile(io.BytesIO(fh.read()))
            for sub in iter_csvs(inner, path + n + "!/"):
                yield sub


def convert_citibike(zip_path, out_path):
    """Zip of monthly CSVs -> one Parquet file. Streamed, low memory."""
    if os.path.exists(out_path):
        try:
            return pq.ParquetFile(out_path).metadata.num_rows, "cached"
        except Exception:
            os.remove(out_path)
    writer, rows, parts = None, 0, 0
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for name, fh in iter_csvs(zf):
                parts += 1
                for chunk in pd.read_csv(
                        fh, chunksize=500000, low_memory=False,
                        dtype={c: "string" for c in CB_STR}):
                    for c in ("started_at", "ended_at"):
                        chunk[c] = pd.to_datetime(chunk[c], errors="coerce")
                    for c in ("start_lat", "start_lng", "end_lat", "end_lng"):
                        chunk[c] = pd.to_numeric(chunk[c], errors="coerce")
                    chunk = chunk.reindex(columns=CB_SCHEMA.names)
                    tbl = pa.Table.from_pandas(chunk, schema=CB_SCHEMA,
                                               preserve_index=False)
                    if writer is None:
                        writer = pq.ParquetWriter(out_path + ".part", CB_SCHEMA,
                                                  compression="zstd",
                                                  compression_level=6)
                    writer.write_table(tbl)
                    rows += len(chunk)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise RuntimeError("no CSV found inside %s" % zip_path)
    os.replace(out_path + ".part", out_path)
    return rows, "converted (%d csv part%s)" % (parts, "" if parts == 1 else "s")


def do_citibike(convert_only=False):
    zdir = os.path.join(OUT, "citibike", "_zips")
    keys = [(k, s) for k, s in s3_list()
            if k.startswith(YEAR) and k.endswith(".zip") and not k.startswith("JC-")]
    keys.sort()
    if not convert_only:
        jobs = [(S3 + k, os.path.join(zdir, k), s) for k, s in keys]
        run_group("Citi Bike (raw zips)", jobs)
    log("\nCiti Bike: converting to Parquet")
    out = []
    for k, _ in keys:
        month = "%s-%s" % (k[:4], k[4:6])
        dest = os.path.join(OUT, "citibike", "citibike_%s.parquet" % month)
        rows, how = convert_citibike(os.path.join(zdir, k), dest)
        out.append((dest, rows))
        log("  citibike_%s.parquet  %11s rows  %8.1f MB  %s"
            % (month, format(rows, ","), os.path.getsize(dest) / 1e6, how))
    return out


# ------------------------------------------------------------------- main
def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    convert_only = "--convert-only" in sys.argv
    groups = args or ["yellow", "green", "fhvhv", "citibike"]
    t0 = time.time()

    if "yellow" in groups and not convert_only:
        run_group("Yellow Taxi", tlc_jobs("yellow", "tlc_yellow"))
        for f in ("taxi_zone_lookup.csv", "taxi_zones.zip"):
            size, how = download(TLC + "/misc/" + f, os.path.join(REF, f))
            log("  %-42s %8.1f KB  %s" % (f, size / 1e3, how))
    if "green" in groups and not convert_only:
        run_group("Green Taxi", tlc_jobs("green", "tlc_green"))
    if "fhvhv" in groups and not convert_only:
        run_group("HVFHV (Uber + Lyft)", tlc_jobs("fhvhv", "tlc_fhvhv"))
    if "citibike" in groups:
        do_citibike(convert_only)

    log("\ndone in %.1f min" % ((time.time() - t0) / 60))


if __name__ == "__main__":
    main()
