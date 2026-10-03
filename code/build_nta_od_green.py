# -*- coding: utf-8 -*-
"""
build_nta_od_green.py -- Green Taxi NTA-pair OD panel, 2023-01 .. 2026-07

The 2023-2025 panels from the earlier internal pipeline never included green, so
this script builds both eras in one pass, with exactly the same method as the
yellow mode in build_nta_od_2026.py (it reuses those functions):
  * bucket by (PU_zone, DO_zone, date, hour) -> spread onto NTA pairs by the
    tensor product of the zone->NTA area-share weights
  * weight threshold 0.01, then renormalise
  * Stage 7a physical bounds + 7b sequential per-column 0.001/0.999 quantile clip
    -- thresholds computed once over the whole green period (all 2023-2026
    months) and shared by every output file

The only structural difference from the other modes: 2023-2025 is written as a
single file green_2023-2025.parquet and 2026 as monthly files, matching the
existing nta_od_riderships layout.

Raw inputs:
  2023-2025  $HISTORICAL_DATA_ROOT/green/green_YYYY-MM.parquet (36 months;
             HISTORICAL_DATA_ROOT defaults to <repo>/data/historical)
  2026       data/tlc_green/green_tripdata_2026-MM.parquet

Usage:  python code/build_nta_od_green.py
"""
import os
import sys
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_nta_od_2026 import (  # noqa: E402
    ROOT, DATA, OUTROOT, TMP, CRZ_START, PHYS, OUT_COLS, OUT_SCHEMA,
    con_new, months_of, load_weights, load_cbd, tlc_month, p,
)

# 2023-2025 raw green trips: set HISTORICAL_DATA_ROOT, default <repo>/data/historical
HIST_DIR = os.path.join(os.environ.get("HISTORICAL_DATA_ROOT",
                                       os.path.join(DATA, "historical")), "green")
PICKUP, DROPOFF = "lpep_pickup_datetime", "lpep_dropoff_datetime"
DIST = "trip_distance"
DUR = "AVG(EPOCH(lpep_dropoff_datetime - lpep_pickup_datetime))/60.0"


def main():
    t0 = time.time()
    os.makedirs(TMP, exist_ok=True)
    os.makedirs(os.path.join(TMP, "_duck"), exist_ok=True)
    cbd = load_cbd()
    cbd_map = dict(zip(cbd.NTA2020, cbd.is_cbd))

    srcs = months_of(os.path.join(HIST_DIR, "green_*.parquet")) + \
           months_of(os.path.join(DATA, "tlc_green", "green_tripdata_2026-*.parquet"))
    print("green: %d source months (%s ~ %s)" % (len(srcs), srcs[0][0], srcs[-1][0]),
          flush=True)

    # ---- per-month bucketing + area-weight distribution (reuses tlc_month) ----
    con = con_new()
    load_weights(con)
    parts = []
    for month, src in srcs:
        out = os.path.join(TMP, "green_%s.parquet" % month)
        t = time.time()
        n = tlc_month(con, src, "green", PICKUP, DROPOFF, DIST, DUR, None, out, month)
        print("    %s  %11s NTA-pair rows  %5.1fs"
              % (month, format(n, ","), time.time() - t), flush=True)
        parts.append((month, out))
    con.close()

    # ---- Stage 7: thresholds computed once over the whole period (same algorithm as finalise) ----
    con = con_new()
    src_all = "read_parquet([%s])" % ",".join("'%s'" % p(f) for _, f in parts)
    phys = ("avg_distance BETWEEN %g AND %g AND avg_duration BETWEEN %g AND %g "
            "AND avg_mph BETWEEN %g AND %g" % (PHYS["dist"] + PHYS["dur"] + PHYS["mph"]))
    con.execute("CREATE OR REPLACE VIEW v AS SELECT *, "
                "60.0*avg_distance/NULLIF(avg_duration,0) AS avg_mph FROM %s;" % src_all)
    n0 = con.execute("SELECT COUNT(*) FROM v").fetchone()[0]
    con.execute("CREATE OR REPLACE TABLE s7a AS SELECT * FROM v WHERE %s;" % phys)
    thr, where = {}, []
    for col in ("avg_distance", "avg_duration", "avg_mph"):
        cond = (" WHERE " + " AND ".join(where)) if where else ""
        lo, hi = con.execute(
            "SELECT quantile_cont(%s, 0.001), quantile_cont(%s, 0.999) FROM s7a%s"
            % (col, col, cond)).fetchone()
        thr[col] = (lo, hi)
        where.append("%s BETWEEN %.12g AND %.12g" % (col, lo, hi))
    keep = " AND ".join(where)
    n2 = con.execute("SELECT COUNT(*) FROM s7a WHERE %s" % keep).fetchone()[0]
    print("  [green] %s rows -> 7a+7b %s (%.2f%% kept)"
          % (format(n0, ","), format(n2, ","), 100.0 * n2 / max(n0, 1)), flush=True)
    for c, (lo, hi) in thr.items():
        print("        %-13s [%.4f, %.4f]" % (c, lo, hi), flush=True)

    # ---- write: 2023-2025 as one file, 2026 as monthly files ----
    def load_clean(f):
        df = con.execute(
            "SELECT *, 60.0*avg_distance/NULLIF(avg_duration,0) AS avg_mph "
            "FROM read_parquet('%s') WHERE %s AND %s" % (p(f), phys, keep)).fetchdf()
        df["mode"] = "green"
        df["origin_in_cbd"] = df.origin_nta.map(cbd_map).fillna(False).astype(bool)
        df["dest_in_cbd"] = df.dest_nta.map(cbd_map).fillna(False).astype(bool)
        df["log1p_total_rides"] = np.log1p(df.total_rides)
        d = pd.to_datetime(df["date"], errors="coerce")
        df["post"] = np.where(d.notna(), d >= pd.Timestamp(CRZ_START), df.year >= 2025)
        df["treated"] = df.origin_in_cbd | df.dest_in_cbd
        for c in ("year", "month", "day_of_week", "hour_of_day"):
            df[c] = df[c].astype("int64")
        df = df[OUT_COLS]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return pa.Table.from_pandas(df, schema=OUT_SCHEMA, preserve_index=False)

    hist_dir = os.path.join(OUTROOT, "green", "2023-2025")
    cur_dir = os.path.join(OUTROOT, "green", "2026")
    os.makedirs(hist_dir, exist_ok=True)
    os.makedirs(cur_dir, exist_ok=True)

    hist_fp = os.path.join(hist_dir, "green_2023-2025.parquet")
    writer, n_hist, n_cur = None, 0, 0
    for month, f in parts:
        tbl = load_clean(f)
        if month < "2026":
            if writer is None:
                writer = pq.ParquetWriter(hist_fp, OUT_SCHEMA, compression="zstd")
            writer.write_table(tbl)
            n_hist += tbl.num_rows
        else:
            fp = os.path.join(cur_dir, "green_%s.parquet" % month)
            pq.write_table(tbl, fp, compression="zstd")
            n_cur += tbl.num_rows
            print("        %s  %11s rows" % (month, format(tbl.num_rows, ",")), flush=True)
    if writer:
        writer.close()
    con.close()
    for _, f in parts:
        os.remove(f)
    if writer is not None and os.path.exists(hist_fp):
        print("  [green] 2023-2025: %s rows -> %s (%.1f MB)"
              % (format(n_hist, ","), hist_fp, os.path.getsize(hist_fp) / 1e6), flush=True)
    else:
        print("  [green] 2023-2025: no historical input found under %s, file not written"
              % HIST_DIR, flush=True)
    print("  [green] 2026: %s rows across monthly files" % format(n_cur, ","), flush=True)
    print("[done] %.1f min" % ((time.time() - t0) / 60), flush=True)


if __name__ == "__main__":
    main()
