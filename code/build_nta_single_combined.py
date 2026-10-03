# -*- coding: utf-8 -*-
"""
Merge the pick_up and drop_off single-sided panels into one row per
(nta, day, hour), carrying both volumes side by side.

Join
----
FULL OUTER on (nta, day, hour). An NTA-hour can easily have pickups and no
dropoffs or the other way round -- a residential block early in the morning, a
venue at closing time -- so an inner join would silently drop those cells. Over
the four modes 2.1-4.6% of merged rows are one-sided.

Nulls
-----
Where a side is absent its volume is filled with 0 and its two means are left
NULL, because a mean over no trips has no value. One caveat on the zeros: a
missing side usually means no trips, but it can also mean the side's
post-aggregation filter removed that cell (an NTA-hour whose mean distance or
duration came out <= 0). That is rare -- at most a few thousand trips per mode
against tens of millions -- but a 0 here is "no usable trips", not strictly "no
trips".

Columns (9)
-----------
  nta, day, hour
  pick_up_total_rides,  pick_up_average_distance,  pick_up_average_duration
  drop_off_total_rides, drop_off_average_distance, drop_off_average_duration

Reads the intermediate per-side panels from _sides_staging/ and writes the
published panels:
  nta_single_riderships/<mode>/2023-2025/<mode>_2023-2025.parquet
  nta_single_riderships/<mode>/2026/<mode>_2026-MM.parquet

Usage:
    python code/build_nta_single_combined.py            # all modes
    python code/build_nta_single_combined.py yellow     # one mode
"""
from __future__ import annotations

import glob
import os
import sys
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIDES_IN = os.path.join(ROOT, "_sides_staging")
OUT = os.path.join(ROOT, "nta_single_riderships")
MODES = ["yellow", "uber", "lyft", "citibike"]

OUT_SCHEMA = pa.schema([
    ("nta", pa.string()),
    ("day", pa.date32()),
    ("hour", pa.int64()),
    ("pick_up_total_rides", pa.float64()),
    ("pick_up_average_distance", pa.float64()),
    ("pick_up_average_duration", pa.float64()),
    ("drop_off_total_rides", pa.float64()),
    ("drop_off_average_distance", pa.float64()),
    ("drop_off_average_duration", pa.float64()),
])


def p(x) -> str:
    return str(x).replace("\\", "/")


def con_new() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("SET memory_limit='8GB'; SET threads=6; SET preserve_insertion_order=false;")
    c.execute("SET enable_progress_bar=false;")
    return c


def merge_one(con, pu_files, do_files, out_path) -> tuple:
    """FULL OUTER join of the two sides; returns (rows, pu_only, do_only)."""
    pu = "[%s]" % ",".join("'%s'" % p(f) for f in pu_files)
    do = "[%s]" % ",".join("'%s'" % p(f) for f in do_files)
    con.execute(f"""
      COPY (
        SELECT
          COALESCE(a.nta,  b.nta)  AS nta,
          COALESCE(a.day,  b.day)  AS day,
          COALESCE(a.hour, b.hour) AS hour,
          COALESCE(a.total_rides, 0.0) AS pick_up_total_rides,
          a.average_distance           AS pick_up_average_distance,
          a.average_duration           AS pick_up_average_duration,
          COALESCE(b.total_rides, 0.0) AS drop_off_total_rides,
          b.average_distance           AS drop_off_average_distance,
          b.average_duration           AS drop_off_average_duration
        FROM read_parquet({pu}) a
        FULL OUTER JOIN read_parquet({do}) b
          ON a.nta = b.nta AND a.day = b.day AND a.hour = b.hour
      ) TO '{p(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    pf = pq.ParquetFile(out_path)
    if pf.schema_arrow != OUT_SCHEMA:
        raise RuntimeError("schema drift in %s: got %s, want %s"
                           % (out_path, pf.schema_arrow, OUT_SCHEMA))
    one = con.execute(
        "SELECT COUNT(*) FILTER (WHERE drop_off_average_distance IS NULL), "
        "COUNT(*) FILTER (WHERE pick_up_average_distance IS NULL) "
        "FROM read_parquet('%s')" % p(out_path)).fetchone()
    return pf.metadata.num_rows, one[0], one[1]


def build(mode: str) -> None:
    con = con_new()
    t0 = time.time()
    for era in ("2023-2025", "2026"):
        pu_dir = os.path.join(SIDES_IN, mode, "pick_up", era)
        do_dir = os.path.join(SIDES_IN, mode, "drop_off", era)
        pu_all = sorted(glob.glob(os.path.join(pu_dir, "*.parquet")))
        do_all = sorted(glob.glob(os.path.join(do_dir, "*.parquet")))
        os.makedirs(os.path.join(OUT, mode, era), exist_ok=True)
        if not pu_all or not do_all:
            # e.g. Citi Bike before 2026: leave the directory in place but empty
            print("  [%s/%s] no per-side input, left empty" % (mode, era), flush=True)
            continue
        outdir = os.path.join(OUT, mode, era)
        os.makedirs(outdir, exist_ok=True)
        if era == "2023-2025":
            out = os.path.join(outdir, "%s_2023-2025.parquet" % mode)
            n, po, do_ = merge_one(con, pu_all, do_all, out)
            print("  [%s/%s] %11s rows  %7.1f MB  (pickup-only %s, dropoff-only %s)"
                  % (mode, era, format(n, ","), os.path.getsize(out) / 1e6,
                     format(po, ","), format(do_, ",")), flush=True)
        else:
            total = po_t = do_t = 0
            for f in pu_all:
                ym = os.path.basename(f).rsplit("_", 1)[1].replace(".parquet", "")
                mate = os.path.join(do_dir, "%s_drop_off_%s.parquet" % (mode, ym))
                if not os.path.exists(mate):
                    raise RuntimeError("missing drop_off file for %s %s" % (mode, ym))
                out = os.path.join(outdir, "%s_%s.parquet" % (mode, ym))
                n, po, do_ = merge_one(con, [f], [mate], out)
                total += n
                po_t += po
                do_t += do_
                print("        %s  %9s rows  %6.1f MB" % (ym, format(n, ","),
                                                          os.path.getsize(out) / 1e6), flush=True)
            print("  [%s/%s] %11s rows  (pickup-only %s, dropoff-only %s)"
                  % (mode, era, format(total, ","), format(po_t, ","), format(do_t, ",")),
                  flush=True)
    con.close()
    print("  [%s] %.1f min" % (mode, (time.time() - t0) / 60), flush=True)


def main() -> None:
    modes = [a for a in sys.argv[1:] if not a.startswith("-")] or MODES
    bad = [m for m in modes if m not in MODES]
    if bad:
        raise SystemExit("unknown mode(s): %s; expected one of %s" % (bad, MODES))
    t0 = time.time()
    for mode in modes:
        print("\n=== %s ===" % mode, flush=True)
        build(mode)
    print("\n[done] %.1f min total" % ((time.time() - t0) / 60), flush=True)


if __name__ == "__main__":
    main()
