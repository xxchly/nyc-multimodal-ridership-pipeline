# -*- coding: utf-8 -*-
"""
Check that the 2026 NTA-OD panels line up with the copied 2023-2025 panels.

The point of these panels is that the two eras stack into one table, so the
checks are about joinability, not just row counts:
  1. identical 17-column schema, same order, same dtypes
  2. monthly files are complete and each one holds only its nominal month
  3. day_of_week really is 1=Mon..7=Sun in both eras (verified against `date`)
  4. NTA code vocabulary and CBD flagging agree across eras
  5. subway routing coverage: how many rows got distance/duration from the
     GTFS lookup rather than being dropped for lack of one
  6. per-mode magnitudes are in line with the same months of 2025
  7. a real UNION ALL of both eras parses and aggregates
"""
from __future__ import annotations

import glob
import os

import duckdb
import pandas as pd
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "nta_od_riderships")
DATA = os.path.join(ROOT, "data")
# raw 2026 inputs each mode is built from (build_nta_od_2026.py); used to derive
# the expected set of monthly panel files instead of hard-coding a count
RAW = {"yellow": os.path.join(DATA, "tlc_yellow", "yellow_tripdata_2026-*.parquet"),
       "uber": os.path.join(DATA, "tlc_fhvhv", "fhvhv_tripdata_2026-*.parquet"),
       "lyft": os.path.join(DATA, "tlc_fhvhv", "fhvhv_tripdata_2026-*.parquet"),
       "citibike": os.path.join(DATA, "citibike", "citibike_2026-*.parquet"),
       "subway": os.path.join(DATA, "mta_subway_od", "mta_subway_od_2026-*.parquet")}
MODES = ["yellow", "uber", "lyft", "citibike", "subway"]
EXPECT_COLS = ["mode", "year", "month", "date", "day_of_week", "hour_of_day",
               "origin_nta", "dest_nta", "origin_in_cbd", "dest_in_cbd",
               "total_rides", "log1p_total_rides",
               "avg_distance", "avg_duration", "avg_mph", "post", "treated"]
fails = []


def check(label, ok, detail=""):
    print("  [%s] %s%s" % ("PASS" if ok else "FAIL", label, ("  " + detail) if detail else ""),
          flush=True)
    if not ok:
        fails.append(label)


def p(x):
    return str(x).replace("\\", "/")


con = duckdb.connect()
con.execute("SET memory_limit='6GB'; SET threads=6;")

hist = {m: os.path.join(OUT, m, "2023-2025", "%s_2023-2025.parquet" % m) for m in MODES}
new = {m: sorted(glob.glob(os.path.join(OUT, m, "2026", "%s_2026-*.parquet" % m))) for m in MODES}

# ---------------------------------------------------------------- 1. schema
print("\n1. schema identical to the 2023-2025 panels")
for m in MODES:
    hs = pq.ParquetFile(hist[m]).schema_arrow
    ns = pq.ParquetFile(new[m][0]).schema_arrow
    same_names = list(hs.names) == list(ns.names) == EXPECT_COLS
    htypes = [str(t) for t in hs.types]
    ntypes = [str(t) for t in ns.types]
    diff = [(n, a, b) for n, a, b in zip(hs.names, htypes, ntypes) if a != b]
    check("%-9s column names + order" % m, same_names,
          "" if same_names else str(list(ns.names)))
    check("%-9s column dtypes" % m, not diff, str(diff) if diff else "")

# ------------------------------------------------- 2. monthly completeness
print("\n2. monthly files complete and pinned to their nominal month")
def ym_of(path):
    return os.path.basename(path).rsplit(".", 1)[0][-7:]      # "2026-01"


for m in MODES:
    have = sorted(ym_of(f) for f in new[m])
    raw = sorted(ym_of(f) for f in glob.glob(RAW[m]))
    if raw:
        # expected months = the raw input months on disk, not a fixed count
        check("%-9s one panel file per raw input month (%d)" % (m, len(raw)), have == raw,
              "" if have == raw else "missing %s, extra %s"
              % (sorted(set(raw) - set(have)), sorted(set(have) - set(raw))))
    else:
        # raw inputs not on disk: the panel months must at least be gap-free
        idx = [int(y[:4]) * 12 + int(y[5:7]) for y in have]
        ok = bool(idx) and idx == list(range(idx[0], idx[0] + len(idx)))
        check("%-9s monthly files present with no gap (raw inputs absent)" % m, ok,
              "%d files: %s" % (len(have), ", ".join(have)))
    bad = []
    for f in new[m]:
        ym = os.path.basename(f).rsplit("_", 1)[1].replace(".parquet", "")
        r = con.execute("SELECT COUNT(*) FROM read_parquet('%s') WHERE year <> %d OR month <> %d"
                        % (p(f), int(ym[:4]), int(ym[5:7]))).fetchone()[0]
        if r:
            bad.append((ym, r))
    check("%-9s no out-of-month rows" % m, not bad, str(bad))

# ------------------------------------------------------- 3. dow convention
print("\n3. day_of_week is 1=Mon..7=Sun (checked against `date`)")
for m in MODES:
    for era, src in (("2023-2025", "'%s'" % p(hist[m])),
                     ("2026", "[%s]" % ",".join("'%s'" % p(f) for f in new[m]))):
        r = con.execute(
            "SELECT COUNT(*) FROM read_parquet(%s) WHERE day_of_week <> EXTRACT(isodow FROM \"date\")"
            % src).fetchone()[0]
        check("%-9s %-9s dow matches date" % (m, era), r == 0, "%s mismatched" % format(r, ","))

# --------------------------------------------------- 4. NTA vocab + CBD flag
print("\n4. NTA vocabulary and CBD flagging agree across eras")
for m in MODES:
    q = ("SELECT COUNT(DISTINCT origin_nta), COUNT(DISTINCT dest_nta), "
         "COUNT(DISTINCT CASE WHEN origin_in_cbd THEN origin_nta END) FROM read_parquet(%s)")
    h = con.execute(q % ("'%s'" % p(hist[m]))).fetchone()
    n = con.execute(q % ("[%s]" % ",".join("'%s'" % p(f) for f in new[m]))).fetchone()
    print("     %-9s 2023-2025 o=%d d=%d cbd=%d   |   2026 o=%d d=%d cbd=%d"
          % (m, h[0], h[1], h[2], n[0], n[1], n[2]))
    # NTA codes seen only in 2026. Not a defect by itself: Citi Bike genuinely
    # added service area in 2026, and the subway panel's vocabulary follows the
    # routing table, which was rebuilt on a newer GTFS feed. Flag it only if the
    # rides landing on unseen codes are more than 1% of all endpoints.
    ns = ",".join("'%s'" % p(f) for f in new[m])
    extra = con.execute(
        "SELECT COUNT(*), COALESCE(SUM(rides), 0) FROM ("
        "  SELECT x, SUM(r) AS rides FROM ("
        "    SELECT origin_nta AS x, total_rides AS r FROM read_parquet([%s])"
        "    UNION ALL SELECT dest_nta, total_rides FROM read_parquet([%s])) a"
        "  WHERE x NOT IN (SELECT origin_nta FROM read_parquet('%s')"
        "                  UNION SELECT dest_nta FROM read_parquet('%s'))"
        "  GROUP BY 1)" % (ns, ns, p(hist[m]), p(hist[m]))).fetchone()
    tot = con.execute("SELECT SUM(total_rides) FROM read_parquet([%s])" % ns).fetchone()[0]
    share = 100.0 * extra[1] / (2 * tot) if tot else 0.0
    check("%-9s NTA codes new in 2026 carry <1%% of rides" % m, share < 1.0,
          "%d codes, %.4f%% of endpoints" % (extra[0], share))
    # CBD coverage is a property of the mode's network, not a constant: Citi Bike
    # reaches 16 of the 17 CBD NTAs and the subway panel 14. What must hold is
    # that 2026 matches the historical era for the same mode.
    def cbd_touched(src):
        return con.execute(
            "SELECT COUNT(DISTINCT x) FROM ("
            "SELECT origin_nta AS x FROM read_parquet(%s) WHERE origin_in_cbd "
            "UNION SELECT dest_nta FROM read_parquet(%s) WHERE dest_in_cbd)" % (src, src)
        ).fetchone()[0]
    ch, cn = cbd_touched("'%s'" % p(hist[m])), cbd_touched("[%s]" % ns)
    check("%-9s CBD NTAs touched match across eras" % m, ch == cn,
          "2023-2025 %d vs 2026 %d (of 17)" % (ch, cn))

# --------------------------------------------- 5. subway routing coverage
print("\n5. subway routing coverage (metrics come from the GTFS lookup, not observation)")
src = "[%s]" % ",".join("'%s'" % p(f) for f in new["subway"])
r = con.execute("SELECT COUNT(*), COUNT(avg_distance), "
                "COUNT(DISTINCT origin_nta || '>' || dest_nta) FROM read_parquet(%s)" % src).fetchone()
print("     final rows %s, with metrics %s, distinct NTA pairs %s"
      % (format(r[0], ","), format(r[1], ","), format(r[2], ",")))
check("subway: every surviving row has routed metrics", r[0] == r[1],
      "%s missing" % format(r[0] - r[1], ","))

# -------------------------------------------------- 6. magnitude vs 2025
print("\n6. 2026 monthly magnitude against the same months of 2025")
print("     mode       2026 rows/mo   2025 rows/mo    ratio   2026 rides/mo   2025 rides/mo")
for m in MODES:
    nm = len(new[m])
    mons = ",".join(str(int(ym_of(f)[5:7])) for f in new[m])   # the months actually present
    n = con.execute("SELECT COUNT(*)/%d, SUM(total_rides)/%d FROM read_parquet([%s])"
                    % (nm, nm, ",".join("'%s'" % p(f) for f in new[m]))).fetchone()
    h = con.execute("SELECT COUNT(*)/%d, SUM(total_rides)/%d FROM read_parquet('%s') "
                    "WHERE year=2025 AND month IN (%s)" % (nm, nm, p(hist[m]), mons)).fetchone()
    print("     %-9s %13s %14s   %5.2fx %15s %15s"
          % (m, format(int(n[0]), ","), format(int(h[0]), ","),
             n[0] / max(h[0], 1), format(int(n[1]), ","), format(int(h[1]), ",")))

# ----------------------------------------------------- 7. real concatenation
print("\n7. both eras stack into one table")
allsrc = []
for m in MODES:
    allsrc.append("'%s'" % p(hist[m]))
    allsrc += ["'%s'" % p(f) for f in new[m]]
r = con.execute("SELECT mode, MIN(year), MAX(year), COUNT(*) FROM read_parquet([%s]) "
                "GROUP BY mode ORDER BY mode" % ",".join(allsrc)).fetchdf()
print(r.to_string(index=False))
check("union of all 5 modes x 2 eras reads as one table", len(r) == 5, "%d modes" % len(r))

print("\n" + "=" * 62)
if fails:
    print("%d check(s) FAILED:" % len(fails))
    for x in fails:
        print("   - %s" % x)
    raise SystemExit(1)
print("all checks passed.")
