# -*- coding: utf-8 -*-
"""
Build the 2026 NTA-pair OD panels for 5 modes: yellow, uber, lyft, citibike, subway.

Output schema is the 17 columns of the 2023-2025 panels
(nta_od_riderships/<mode>/2023-2025/<mode>_2023-2025.parquet), so the two eras
concatenate directly.

Conventions taken from the ON-DISK 2023-2025 panels, not from the earlier
internal pipeline's source code:
  * day_of_week is 1=Monday .. 7=Sunday  (that code writes DuckDB EXTRACT(dow),
    which is 0=Sunday; the shipped panels are 1..7, verified by reversing
    date -> weekday)
  * subway keeps its representative-week `date` (the shipped 2023-2025 subway
    panel has 252 distinct dates; that code sets NaT)

Per-mode method:
  yellow/uber/lyft  TLC gives taxi-zone ids and no coordinates. Bucket trips into
                    (PU_zone, DO_zone, date, hour), then distribute onto NTA pairs
                    by the tensor product of the zone->NTA area-share matrix
                    (w = w_origin * w_dest). Distance/duration are ride-weighted.
  citibike          Real GPS: point-in-polygon onto NTA polygons, Haversine distance.
  subway            Station complexes carry lat/lon: point-in-polygon, sum ridership
                    per NTA pair. MTA has no distance/duration, so those come from
                    the GTFS routing lookup built by build_subway_routing.py.

Cleaning (Stage 7, same as the historical build):
  7a hard physical bounds: distance 0.05-100 mi, duration 0.5-300 min, speed 1-200 mph
  7b per-mode 0.001/0.999 percentile clip on distance, duration, mph -- applied
     SEQUENTIALLY (each cut recomputed on the survivors of the previous one) and
     over the WHOLE mode, not per month, so monthly files share one threshold set.

Memory: everything heavy runs in DuckDB and is processed one month at a time; the
tensor-product join never materialises in pandas.

Usage:
    python code/build_nta_od_2026.py                 # all 5 modes
    python code/build_nta_od_2026.py yellow citibike # a subset
"""
from __future__ import annotations

import glob
import os
import shutil
import sys
import time

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
REF = os.path.join(ROOT, "reference")
OUTROOT = os.path.join(ROOT, "nta_od_riderships")
TMP = os.path.join(ROOT, "_build_tmp")

WEIGHT_LONG = os.path.join(REF, "taxi_nta_weight_long.parquet")
CBD_CSV = os.path.join(REF, "cbd_flag.csv")
NTA_GEOJSON = os.path.join(REF, "nta_2020.geojson")
ROUTING = os.path.join(REF, "subway_nta_routing_20260827.parquet")

CRZ_START = "2025-01-05"
# minimum zone->NTA area share kept before renormalising; see load_weights()
MIN_W = 0.01
PHYS = dict(dist=(0.05, 100.0), dur=(0.5, 300.0), mph=(1.0, 200.0))
OUT_COLS = ["mode", "year", "month", "date", "day_of_week", "hour_of_day",
            "origin_nta", "dest_nta", "origin_in_cbd", "dest_in_cbd",
            "total_rides", "log1p_total_rides",
            "avg_distance", "avg_duration", "avg_mph", "post", "treated"]

# Exact Arrow schema of the shipped 2023-2025 panels. pandas 3 would otherwise
# write large_string for the text columns and timestamp[us] for `date`, which
# makes a strict schema union across the two eras fail on type conflict.
OUT_SCHEMA = pa.schema([
    ("mode", pa.string()),
    ("year", pa.int64()),
    ("month", pa.int64()),
    ("date", pa.date32()),
    ("day_of_week", pa.int64()),
    ("hour_of_day", pa.int64()),
    ("origin_nta", pa.string()),
    ("dest_nta", pa.string()),
    ("origin_in_cbd", pa.bool_()),
    ("dest_in_cbd", pa.bool_()),
    ("total_rides", pa.float64()),
    ("log1p_total_rides", pa.float64()),
    ("avg_distance", pa.float64()),
    ("avg_duration", pa.float64()),
    ("avg_mph", pa.float64()),
    ("post", pa.bool_()),
    ("treated", pa.bool_()),
])

WEEKDAY_BUCKETS = [(0, 5, "Weekday_00_05"), (6, 9, "Weekday_06_09"),
                   (10, 15, "Weekday_10_15"), (16, 19, "Weekday_16_19"),
                   (20, 23, "Weekday_20_23")]


def p(x) -> str:
    return str(x).replace("\\", "/")


def con_new() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("SET memory_limit='8GB'; SET threads=6; SET preserve_insertion_order=false;")
    c.execute("SET temp_directory='%s';" % p(os.path.join(TMP, "_duck")))
    c.execute("INSTALL spatial; LOAD spatial;")
    return c


def months_of(pattern: str) -> list:
    out = []
    for f in sorted(glob.glob(pattern)):
        stem = os.path.basename(f).rsplit(".", 1)[0]
        out.append((stem[-7:], f))          # ("2026-01", path)
    return out


# --------------------------------------------------------------- references
def load_weights(con) -> None:
    """zone -> NTA area share: drop shares below MIN_W, then renormalise to 1 per zone.

    The shipped long table has 4 duplicate (LocationID, nta_code) rows and the
    wide table has 2 duplicate zones; the two files disagree only on zone 56
    (Corona, Queens). Keeping the first occurrence fixes that.

    MIN_W matters more than it looks. Using every non-zero share (4.69 NTAs per
    zone) spreads each zone pair over ~22 NTA pairs and yields ~4.2x the rows of
    the shipped 2023-2025 panels, with a long tail of near-zero cells (median
    total_rides 1e-4 against the shipped panels' 0.36). Reproducing one shipped
    hour (2025-03-05 08:00, yellow) pins the historical rule down: dropping
    shares < 0.01 and renormalising gives 2,975 rows against the shipped 2,974,
    with 0.03% absolute ridership error -- versus 0.21% at 0.001, 0.42% at 0.05
    and 13.3% when each zone is collapsed to its single dominant NTA. Over the
    whole of 2025-03 it gives 1,755,324 pre-cleaning rows against 1,728,593
    shipped post-cleaning rows, the 1.5% gap being the Stage-7 cut that the
    shipped file has already had applied.
    """
    w = pd.read_parquet(WEIGHT_LONG)[["LocationID", "nta_code", "proportion_normalized"]]
    w = w.drop_duplicates(["LocationID", "nta_code"])
    w = w[w.proportion_normalized >= MIN_W].copy()
    w["w"] = w.proportion_normalized / w.groupby("LocationID").proportion_normalized.transform("sum")
    con.register("W_df", w[["LocationID", "nta_code", "w"]])
    con.execute("CREATE OR REPLACE TABLE W AS SELECT * FROM W_df;")
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT LocationID) FROM W").fetchone()
    print("  weights: %d (zone,NTA) entries over %d zones" % n, flush=True)


def load_nta_polygons(con) -> None:
    con.execute("CREATE OR REPLACE TABLE nta AS SELECT * FROM ST_Read('%s');" % p(NTA_GEOJSON))
    cols = [r[1] for r in con.execute("PRAGMA table_info('nta');").fetchall()]
    if "geom" not in cols and "geometry" in cols:
        con.execute("ALTER TABLE nta RENAME COLUMN geometry TO geom;")


def load_cbd() -> pd.DataFrame:
    c = pd.read_csv(CBD_CSV)
    c.columns = [x.strip().lstrip("﻿") for x in c.columns]
    c["is_cbd"] = c["is_cbd"].astype(str).str.upper().isin(("TRUE", "1"))
    return c[["NTA2020", "is_cbd"]]


# ------------------------------------------------------- stage 3: TLC modes
def tlc_month(con, src: str, mode: str, pickup: str, dropoff: str,
              dist_col: str, dur_expr: str, lic, out_path: str, ym: str) -> int:
    """One month of one TLC mode -> NTA-pair-hour parquet (pre Stage-7b).

    `ym` ("2026-01") pins the output to its nominal month: TLC month files carry
    a handful of trips timestamped outside the month they are named for (7-46 raw
    trips per file, a few parts per million), which would otherwise land in a
    monthly output whose year/month columns disagree with its filename.
    """
    lic_where = " AND hvfhs_license_num = '%s'" % lic if lic else ""
    yy, mm = int(ym[:4]), int(ym[5:7])
    lic_where += " AND EXTRACT(year FROM %s) = %d AND EXTRACT(month FROM %s) = %d" % (
        pickup, yy, pickup, mm)
    q = f"""
    COPY (
      WITH b AS (
        SELECT PULocationID AS PU, DOLocationID AS DO,
               EXTRACT(year    FROM {pickup}) AS year,
               EXTRACT(month   FROM {pickup}) AS month,
               CAST({pickup} AS DATE)         AS date,
               EXTRACT(isodow  FROM {pickup}) AS day_of_week,   -- 1=Mon .. 7=Sun
               EXTRACT(hour    FROM {pickup}) AS hour_of_day,
               COUNT(*)         AS n,
               AVG({dist_col})  AS mean_dist,
               {dur_expr}       AS mean_dur
        FROM read_parquet('{p(src)}')
        WHERE {dist_col} > 0 AND {dropoff} > {pickup}{lic_where}
        GROUP BY 1,2,3,4,5,6,7
      ),
      x AS (
        SELECT b.year, b.month, b.date, b.day_of_week, b.hour_of_day,
               wo.nta_code AS origin_nta, wd.nta_code AS dest_nta,
               b.n * wo.w * wd.w                 AS wr,
               b.mean_dist * b.n * wo.w * wd.w   AS wdi,
               b.mean_dur  * b.n * wo.w * wd.w   AS wdu
        FROM b JOIN W wo ON wo.LocationID = b.PU
               JOIN W wd ON wd.LocationID = b.DO
      )
      SELECT year, month, date, day_of_week, hour_of_day, origin_nta, dest_nta,
             SUM(wr) AS total_rides,
             SUM(wdi) / NULLIF(SUM(wr), 0) AS avg_distance,
             SUM(wdu) / NULLIF(SUM(wr), 0) AS avg_duration
      FROM x
      GROUP BY 1,2,3,4,5,6,7
    ) TO '{p(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(q)
    return pq.ParquetFile(out_path).metadata.num_rows


# --------------------------------------------------- stage 4: Citi Bike GPS
def citibike_month(con, src: str, out_path: str, ym: str) -> int:
    yy, mm = int(ym[:4]), int(ym[5:7])
    q = f"""
    COPY (
      WITH t AS (
        SELECT started_at AS pu, ended_at AS dropoff,
               start_lat, start_lng, end_lat, end_lng
        FROM read_parquet('{p(src)}')
        WHERE start_lat IS NOT NULL AND start_lng IS NOT NULL
          AND end_lat IS NOT NULL AND end_lng IS NOT NULL
          AND ended_at > started_at
          AND EXTRACT(year FROM started_at) = {yy} AND EXTRACT(month FROM started_at) = {mm}
      ),
      o AS (SELECT t.*, n.NTA2020 AS origin_nta FROM t
            LEFT JOIN nta n ON ST_Intersects(n.geom, ST_Point(t.start_lng, t.start_lat))),
      od AS (SELECT o.*, n.NTA2020 AS dest_nta FROM o
             LEFT JOIN nta n ON ST_Intersects(n.geom, ST_Point(o.end_lng, o.end_lat)))
      SELECT EXTRACT(year FROM pu) AS year, EXTRACT(month FROM pu) AS month,
             CAST(pu AS DATE) AS date,
             EXTRACT(isodow FROM pu) AS day_of_week,
             EXTRACT(hour FROM pu) AS hour_of_day,
             origin_nta, dest_nta,
             COUNT(*)::DOUBLE AS total_rides,
             AVG(2*3958.8*ASIN(SQRT(POWER(SIN(RADIANS((end_lat-start_lat)/2)),2)
               + COS(RADIANS(start_lat))*COS(RADIANS(end_lat))
               * POWER(SIN(RADIANS((end_lng-start_lng)/2)),2)))) AS avg_distance,
             AVG(EPOCH(dropoff - pu))/60.0 AS avg_duration
      FROM od
      WHERE origin_nta IS NOT NULL AND dest_nta IS NOT NULL
      GROUP BY 1,2,3,4,5,6,7
    ) TO '{p(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(q)
    return pq.ParquetFile(out_path).metadata.num_rows


# ----------------------------------------------------- subway: OD + routing
def subway_month(con, src: str, out_path: str) -> int:
    """Station complexes -> NTA (point-in-polygon), sum ridership, join routing."""
    bucket_case = "CASE WHEN day_of_week = 6 THEN 'Saturday_all' " \
                  "WHEN day_of_week = 7 THEN 'Sunday_all' " + \
                  " ".join("WHEN hour_of_day BETWEEN %d AND %d THEN '%s'" % b
                           for b in WEEKDAY_BUCKETS) + \
                  " ELSE 'Weekday_00_05' END"
    q = f"""
    COPY (
      WITH t AS (
        SELECT year, month, CAST(timestamp AS DATE) AS date,
               EXTRACT(isodow FROM timestamp) AS day_of_week,
               hour_of_day,
               origin_latitude, origin_longitude,
               destination_latitude, destination_longitude,
               estimated_average_ridership AS rides
        FROM read_parquet('{p(src)}')
      ),
      o AS (SELECT t.*, n.NTA2020 AS origin_nta FROM t
            LEFT JOIN nta n ON ST_Intersects(n.geom, ST_Point(t.origin_longitude, t.origin_latitude))),
      od AS (SELECT o.*, n.NTA2020 AS dest_nta FROM o
             LEFT JOIN nta n ON ST_Intersects(n.geom, ST_Point(o.destination_longitude, o.destination_latitude))),
      agg AS (
        SELECT year, month, date, day_of_week, hour_of_day, origin_nta, dest_nta,
               SUM(rides) AS total_rides
        FROM od
        WHERE origin_nta IS NOT NULL AND dest_nta IS NOT NULL
        GROUP BY 1,2,3,4,5,6,7
      )
      SELECT a.year, a.month, a.date, a.day_of_week, a.hour_of_day,
             a.origin_nta, a.dest_nta, a.total_rides,
             r.avg_distance, r.avg_duration
      FROM agg a
      LEFT JOIN read_parquet('{p(ROUTING)}') r
        ON r.o_nta = a.origin_nta AND r.d_nta = a.dest_nta
       AND r.bucket = ({bucket_case})
    ) TO '{p(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(q)
    return pq.ParquetFile(out_path).metadata.num_rows


# ------------------------------------------------- stage 7b + finalise write
def finalise(mode: str, parts: list, cbd: pd.DataFrame) -> None:
    """Compute mode-level 0.001/0.999 thresholds across all months, then write."""
    con = con_new()
    src = "read_parquet([%s])" % ",".join("'%s'" % p(f) for _, f in parts)

    # Stage 7a in SQL, then read only the three metric columns to find thresholds.
    phys = ("avg_distance BETWEEN %g AND %g AND avg_duration BETWEEN %g AND %g "
            "AND avg_mph BETWEEN %g AND %g"
            % (PHYS["dist"] + PHYS["dur"] + PHYS["mph"]))
    con.execute("CREATE OR REPLACE VIEW v AS SELECT *, "
                "60.0*avg_distance/NULLIF(avg_duration,0) AS avg_mph FROM %s;" % src)
    n0 = con.execute("SELECT COUNT(*) FROM v").fetchone()[0]
    con.execute("CREATE OR REPLACE TABLE s7a AS SELECT * FROM v WHERE %s;" % phys)
    n1 = con.execute("SELECT COUNT(*) FROM s7a").fetchone()[0]

    # Stage 7b: sequential clip, exactly as the historical build did it.
    thr = {}
    where = []
    for col in ("avg_distance", "avg_duration", "avg_mph"):
        cond = (" WHERE " + " AND ".join(where)) if where else ""
        lo, hi = con.execute(
            "SELECT quantile_cont(%s, 0.001), quantile_cont(%s, 0.999) FROM s7a%s"
            % (col, col, cond)).fetchone()
        thr[col] = (lo, hi)
        where.append("%s BETWEEN %.12g AND %.12g" % (col, lo, hi))
    keep = " AND ".join(where)
    n2 = con.execute("SELECT COUNT(*) FROM s7a WHERE %s" % keep).fetchone()[0]
    print("  [%s] %s rows -> 7a %s -> 7b %s  (%.2f%% kept)"
          % (mode, format(n0, ","), format(n1, ","), format(n2, ","),
             100.0 * n2 / max(n0, 1)), flush=True)
    for c, (lo, hi) in thr.items():
        print("        %-13s [%.4f, %.4f]" % (c, lo, hi), flush=True)

    cbd_map = dict(zip(cbd.NTA2020, cbd.is_cbd))
    outdir = os.path.join(OUTROOT, mode, "2026")
    os.makedirs(outdir, exist_ok=True)
    total = 0
    for month, f in parts:
        df = con.execute(
            "SELECT *, 60.0*avg_distance/NULLIF(avg_duration,0) AS avg_mph "
            "FROM read_parquet('%s') WHERE %s AND %s" % (p(f), phys, keep)).fetchdf()
        df["mode"] = mode
        df["origin_in_cbd"] = df.origin_nta.map(cbd_map).fillna(False).astype(bool)
        df["dest_in_cbd"] = df.dest_nta.map(cbd_map).fillna(False).astype(bool)
        df["log1p_total_rides"] = np.log1p(df.total_rides)
        d = pd.to_datetime(df["date"], errors="coerce")
        df["post"] = np.where(d.notna(), d >= pd.Timestamp(CRZ_START), df.year >= 2025)
        df["treated"] = df.origin_in_cbd | df.dest_in_cbd
        df["year"] = df.year.astype("int64")
        df["month"] = df.month.astype("int64")
        df["day_of_week"] = df.day_of_week.astype("int64")
        df["hour_of_day"] = df.hour_of_day.astype("int64")
        df = df[OUT_COLS]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        fp = os.path.join(outdir, "%s_%s.parquet" % (mode, month))
        pq.write_table(pa.Table.from_pandas(df, schema=OUT_SCHEMA, preserve_index=False),
                       fp, compression="zstd")
        total += len(df)
        print("        %s  %11s rows  %7.1f MB"
              % (month, format(len(df), ","), os.path.getsize(fp) / 1e6), flush=True)
        del df
    print("  [%s] wrote %s rows across %d monthly files"
          % (mode, format(total, ","), len(parts)), flush=True)
    con.close()


# ------------------------------------------------------------------- driver
def main() -> None:
    modes = [a for a in sys.argv[1:] if not a.startswith("-")] or \
            ["yellow", "uber", "lyft", "citibike", "subway"]
    os.makedirs(TMP, exist_ok=True)
    os.makedirs(os.path.join(TMP, "_duck"), exist_ok=True)
    cbd = load_cbd()
    t0 = time.time()

    TLC_PLAN = {
        "yellow": (os.path.join(DATA, "tlc_yellow", "yellow_tripdata_2026-*.parquet"),
                   "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
                   "AVG(EPOCH(tpep_dropoff_datetime - tpep_pickup_datetime))/60.0", None),
        "uber":   (os.path.join(DATA, "tlc_fhvhv", "fhvhv_tripdata_2026-*.parquet"),
                   "pickup_datetime", "dropoff_datetime", "trip_miles",
                   "AVG(trip_time)/60.0", "HV0003"),
        "lyft":   (os.path.join(DATA, "tlc_fhvhv", "fhvhv_tripdata_2026-*.parquet"),
                   "pickup_datetime", "dropoff_datetime", "trip_miles",
                   "AVG(trip_time)/60.0", "HV0005"),
    }

    for mode in modes:
        tm = time.time()
        print("\n=== %s ===" % mode, flush=True)
        con = con_new()
        parts = []
        if mode in TLC_PLAN:
            pat, pu, do, dc, dur, lic = TLC_PLAN[mode]
            load_weights(con)
            for month, src in months_of(pat):
                out = os.path.join(TMP, "%s_%s.parquet" % (mode, month))
                t = time.time()
                n = tlc_month(con, src, mode, pu, do, dc, dur, lic, out, month)
                print("    %s  %11s NTA-pair rows  %5.1fs"
                      % (month, format(n, ","), time.time() - t), flush=True)
                parts.append((month, out))
        elif mode == "citibike":
            load_nta_polygons(con)
            for month, src in months_of(os.path.join(DATA, "citibike", "citibike_2026-*.parquet")):
                out = os.path.join(TMP, "citibike_%s.parquet" % month)
                t = time.time()
                n = citibike_month(con, src, out, month)
                print("    %s  %11s NTA-pair rows  %5.1fs"
                      % (month, format(n, ","), time.time() - t), flush=True)
                parts.append((month, out))
        elif mode == "subway":
            if not os.path.exists(ROUTING):
                print("  routing lookup missing -- run code/build_subway_routing.py first")
                continue
            load_nta_polygons(con)
            for month, src in months_of(os.path.join(DATA, "mta_subway_od",
                                                     "mta_subway_od_2026-*.parquet")):
                out = os.path.join(TMP, "subway_%s.parquet" % month)
                t = time.time()
                n = subway_month(con, src, out)
                print("    %s  %11s NTA-pair rows  %5.1fs"
                      % (month, format(n, ","), time.time() - t), flush=True)
                parts.append((month, out))
        else:
            print("  unknown mode, skipped")
            con.close()
            continue
        con.close()
        if parts:
            finalise(mode, parts, cbd)
            for _, f in parts:
                os.remove(f)
        print("  [%s] %.1f min" % (mode, (time.time() - tm) / 60), flush=True)

    shutil.rmtree(os.path.join(TMP, "_duck"), ignore_errors=True)
    print("\n[done] %.1f min total" % ((time.time() - t0) / 60), flush=True)


if __name__ == "__main__":
    main()
