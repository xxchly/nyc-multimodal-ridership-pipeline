# -*- coding: utf-8 -*-
"""
Subway single-sided NTA panel, derived from the OD panel (not from raw).

Unlike the other four modes, the subway panel is collapsed from
nta_od_riderships/subway/, because the MTA source is already an OD estimate --
there is no trip-level file to re-aggregate. The original inflow/outflow pivot
from the earlier internal pipeline is not available, so this is a fresh
implementation of the same collapse.

Direction semantics -- the reason the naming needs care
-------------------------------------------------------
inflow / outflow are defined relative to the NTA, not to the station:

  outflow  people LEAVING the NTA  = trips whose ORIGIN is in it
           NYC subway taps on entry only, so this side rests on a recorded swipe
  inflow   people ARRIVING in it   = trips whose DESTINATION is in it
           there is no exit tap, so the destination is INFERRED by MTA's model
           ("inferred to have ended", in the dataset's own column description)

Mapping onto the shared 9-column schema, so this panel stacks with the other
four modes without renaming anything:

  pick_up_*   = origin side = OUTFLOW  (leaving the NTA, recorded)
  drop_off_*  = dest side   = INFLOW   (arriving in the NTA, estimated)

Two ways this panel is NOT like the other four
----------------------------------------------
1. It inherits the OD panel's cleaning. The taxi/Citi Bike single-sided panels
   follow the R recipe -- no weight threshold, filter after aggregation, no
   Stage 7. This one carries whatever the OD build already applied: the 0.01
   zone-share rule does not apply (subway uses point-in-polygon), but Stage 7's
   physical bounds and percentile clip do.
2. `day` is a representative-week date, not a full calendar. The MTA publishes
   one representative week per month, so 2023-2025 has 252 distinct dates and
   2026 has 56, against ~1,300 for the other modes. total_rides is a monthly
   average for that weekday and hour, not a count for that specific date.

Output: nta_single_riderships/subway/{2023-2025,2026}/subway_*.parquet
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
OD = os.path.join(ROOT, "nta_od_riderships", "subway")
OUT = os.path.join(ROOT, "nta_single_riderships", "subway")

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


def collapse(con, od_files, out_path) -> tuple:
    """Collapse OD pairs to each endpoint, then FULL OUTER JOIN the two sides.

    Metrics are ride-weighted means over the other endpoint: the average trip
    length of journeys starting (resp. ending) in this NTA in this hour.
    """
    src = "[%s]" % ",".join("'%s'" % p(f) for f in od_files)
    con.execute(f"""
      COPY (
        WITH o AS (
          SELECT origin_nta AS nta, "date" AS day, hour_of_day AS hour,
                 SUM(total_rides) AS rides,
                 SUM(avg_distance * total_rides) / NULLIF(SUM(total_rides), 0) AS ad,
                 SUM(avg_duration * total_rides) / NULLIF(SUM(total_rides), 0) AS au
          FROM read_parquet({src}) GROUP BY 1, 2, 3
        ),
        d AS (
          SELECT dest_nta AS nta, "date" AS day, hour_of_day AS hour,
                 SUM(total_rides) AS rides,
                 SUM(avg_distance * total_rides) / NULLIF(SUM(total_rides), 0) AS ad,
                 SUM(avg_duration * total_rides) / NULLIF(SUM(total_rides), 0) AS au
          FROM read_parquet({src}) GROUP BY 1, 2, 3
        )
        SELECT
          COALESCE(o.nta,  d.nta)  AS nta,
          COALESCE(o.day,  d.day)  AS day,
          COALESCE(o.hour, d.hour) AS hour,
          COALESCE(o.rides, 0.0)   AS pick_up_total_rides,
          o.ad                     AS pick_up_average_distance,
          o.au                     AS pick_up_average_duration,
          COALESCE(d.rides, 0.0)   AS drop_off_total_rides,
          d.ad                     AS drop_off_average_distance,
          d.au                     AS drop_off_average_duration
        FROM o FULL OUTER JOIN d ON o.nta = d.nta AND o.day = d.day AND o.hour = d.hour
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


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'; SET threads=6; SET preserve_insertion_order=false;")
    con.execute("SET enable_progress_bar=false;")
    t0 = time.time()
    for era in ("2023-2025", "2026"):
        od_files = sorted(glob.glob(os.path.join(OD, era, "*.parquet")))
        outdir = os.path.join(OUT, era)
        os.makedirs(outdir, exist_ok=True)
        if not od_files:
            print("  [%s] no OD input, left empty" % era, flush=True)
            continue
        if era == "2023-2025":
            out = os.path.join(outdir, "subway_2023-2025.parquet")
            n, po, do = collapse(con, od_files, out)
            print("  [%s] %11s rows  %7.1f MB  (outflow-only %s, inflow-only %s)"
                  % (era, format(n, ","), os.path.getsize(out) / 1e6,
                     format(po, ","), format(do, ",")), flush=True)
        else:
            total = 0
            for f in od_files:
                ym = os.path.basename(f).rsplit("_", 1)[1].replace(".parquet", "")
                out = os.path.join(outdir, "subway_%s.parquet" % ym)
                n, po, do = collapse(con, [f], out)
                total += n
                print("        %s  %9s rows  %6.1f MB" % (ym, format(n, ","),
                                                          os.path.getsize(out) / 1e6), flush=True)
            print("  [%s] %11s rows" % (era, format(total, ",")), flush=True)
    con.close()
    print("\n[done] %.1f min" % ((time.time() - t0) / 60), flush=True)


if __name__ == "__main__":
    main()
