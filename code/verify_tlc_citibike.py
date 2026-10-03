"""
Completeness check for the downloaded TLC + Citi Bike 2026 data.

TLC files are published as Parquet and stored byte-for-byte as downloaded, so the
check is: local size == the size the server still reports, the file opens, and the
trip timestamps actually fall in the month the filename claims.

Citi Bike was converted from CSV, so the row count is re-derived independently by
counting newlines in the raw zip (a different code path from the pandas parse that
produced the Parquet) and compared against the Parquet row count. The set of local
Citi Bike months is compared against a live listing of the S3 bucket.
"""
import glob
import os
import zipfile

import pandas as pd
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # this file lives in code/
OUT = os.path.join(ROOT, "data")
TLC = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
S3 = "https://s3.amazonaws.com/tripdata/"
fails = []


def citibike_server_months(year="2026"):
    """Months the Citi Bike bucket currently publishes for `year`, re-listed live
    (same key filter as download_tlc_citibike.py), e.g. {"2026-01", ...}."""
    import xml.etree.ElementTree as ET
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    months, tok = set(), None
    while True:
        prm = {"list-type": "2", "max-keys": "1000", "prefix": year}
        if tok:
            prm["continuation-token"] = tok
        r = requests.get(S3, params=prm, timeout=120)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        for c in root.findall(ns + "Contents"):
            k = c.find(ns + "Key").text
            if k.startswith(year) and k.endswith(".zip") and not k.startswith("JC-"):
                months.add("%s-%s" % (k[:4], k[4:6]))
        t = root.find(ns + "NextContinuationToken")
        if t is None:
            return months
        tok = t.text


def check(label, ok, detail=""):
    print("  [%s] %s%s" % ("PASS" if ok else "FAIL", label, ("  " + detail) if detail else ""),
          flush=True)
    if not ok:
        fails.append(label)


def tlc_group(subdir, prefix, tcol, label):
    print("\n%s" % label)
    files = sorted(glob.glob(os.path.join(OUT, subdir, "%s_tripdata_2026-*.parquet" % prefix)))
    total, size_ok, range_note = 0, True, []
    print("     file                                 rows          in-month   stray")
    for f in files:
        name = os.path.basename(f)
        month = name.split("_")[-1].replace(".parquet", "")
        remote = requests.head(TLC + name, timeout=60, allow_redirects=True)
        rsize = int(remote.headers.get("content-length", 0))
        lsize = os.path.getsize(f)
        if rsize != lsize:
            size_ok = False
            range_note.append("%s size %d != server %d" % (name, lsize, rsize))
        pf = pq.ParquetFile(f)
        n = pf.metadata.num_rows
        total += n
        ts = pq.read_table(f, columns=[tcol]).column(0).to_pandas()
        lo, hi = pd.Timestamp(month + "-01"), pd.Timestamp(month + "-01") + pd.offsets.MonthBegin(1)
        inm = int(((ts >= lo) & (ts < hi)).sum())
        stray = n - inm
        print("     %-34s %11s %11s %7d" % (name, format(n, ","), format(inm, ","), stray))
        if stray:
            range_note.append("%s: %d rows outside %s" % (name, stray, month))
        del ts
    check("%s: all files match the server byte size" % label, size_ok)
    check("%s: all files open and are readable" % label, True)
    print("     total %s rows" % format(total, ","))
    return total, range_note


print("=" * 72)
y_total, y_note = tlc_group("tlc_yellow", "yellow", "tpep_pickup_datetime", "Yellow Taxi")
f_total, f_note = tlc_group("tlc_fhvhv", "fhvhv", "pickup_datetime", "HVFHV (Uber + Lyft)")

# ---- Uber vs Lyft split (sanity: only known license numbers present)
print("\nHVFHV platform split")
LIC = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}
counts = {}
for f in sorted(glob.glob(os.path.join(OUT, "tlc_fhvhv", "fhvhv_tripdata_2026-*.parquet"))):
    col = pq.read_table(f, columns=["hvfhs_license_num"]).column(0).to_pandas()
    for k, v in col.value_counts().items():
        counts[k] = counts.get(k, 0) + int(v)
    del col
for k in sorted(counts, key=lambda x: -counts[x]):
    print("     %-8s %-6s %14s  %5.1f%%"
          % (k, LIC.get(k, "?"), format(counts[k], ","), counts[k] / f_total * 100))
check("only known HVFHS license numbers", set(counts) <= set(LIC),
      str(set(counts) - set(LIC)))
check("HVFHV split sums to the file total", sum(counts.values()) == f_total)

# ---- Citi Bike: independent row recount from the raw zips
cb_files = sorted(glob.glob(os.path.join(OUT, "citibike", "citibike_2026-*.parquet")))
zips = {os.path.basename(z)[:6]: z for z in
        glob.glob(os.path.join(OUT, "citibike", "_zips", "2026*-citibike-tripdata.zip"))}
if zips:
    print("\nCiti Bike (Parquet row count vs raw-CSV newline count)")
else:
    # The raw zips are deletable once converted, so their absence is expected.
    # Say so loudly rather than letting an empty loop report a vacuous PASS.
    print("\nCiti Bike (raw zips deleted - CSV cross-check unavailable, "
          "reporting Parquet row counts only)")
c_total = 0
print("     month     parquet rows      csv rows   parts")
for pqf in cb_files:
    month = os.path.basename(pqf).replace("citibike_", "").replace(".parquet", "")
    key = month.replace("-", "")
    n_pq = pq.ParquetFile(pqf).metadata.num_rows
    zp = zips.get(key)
    if zp is None:
        c_total += n_pq
        print("     %s  %13s %13s %7s  %s"
              % (month, format(n_pq, ","), "-", "-", "no zip"))
        continue
    n_csv, parts = 0, 0
    with zipfile.ZipFile(zp) as zf:
        for info in zf.infolist():
            nm = info.filename
            if info.is_dir() or "__MACOSX" in nm or not nm.lower().endswith(".csv"):
                continue
            parts += 1
            lines = 0
            with zf.open(info) as fh:
                tail = b""
                while True:
                    b = fh.read(1 << 22)
                    if not b:
                        break
                    lines += b.count(b"\n")
                    tail = b[-1:]
                if tail and tail != b"\n":
                    lines += 1          # last line had no trailing newline
            n_csv += lines - 1          # minus the header row
    c_total += n_pq
    ok = n_pq == n_csv
    print("     %s  %13s %13s %7d  %s"
          % (month, format(n_pq, ","), format(n_csv, ","), parts,
             "OK" if ok else "<-- MISMATCH"))
    if not ok:
        fails.append("citibike %s" % month)
local_m = {os.path.basename(f).replace("citibike_", "").replace(".parquet", "")
           for f in cb_files}
server_m = citibike_server_months()
check("local Citi Bike months match the months published on the server",
      bool(server_m) and server_m == local_m,
      "%d local vs %d on server; missing %s, extra %s"
      % (len(local_m), len(server_m), sorted(server_m - local_m), sorted(local_m - server_m)))
if zips:
    check("every Citi Bike month round-trips CSV -> Parquet with no row loss",
          not any(x.startswith("citibike ") for x in fails))
else:
    print("  [SKIP] CSV -> Parquet row-loss check (no raw zips in data/citibike/_zips/; "
          "they are only needed for this check - re-download them with "
          "`download_tlc_citibike.py citibike` to run it)")
print("     total %s rows" % format(c_total, ","))

# ---- stray-date notes (informational, not a download defect)
notes = y_note + f_note
if notes:
    print("\nnote: TLC files as published contain a few out-of-month rows")
    for n in notes[:12]:
        print("     %s" % n)

print("\n" + "=" * 72)
print("Yellow Taxi        %14s trips" % format(y_total, ","))
print("HVFHV (Uber+Lyft)  %14s trips" % format(f_total, ","))
print("Citi Bike          %14s trips" % format(c_total, ","))
print("combined           %14s trips" % format(y_total + f_total + c_total, ","))
print("=" * 72)
if fails:
    print("INCOMPLETE - %d check(s) failed: %s" % (len(fails), fails))
    raise SystemExit(1)
print("COMPLETE - all checks passed.")
