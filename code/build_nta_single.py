# -*- coding: utf-8 -*-
"""
Single-sided (one endpoint) NTA x date x hour panels for yellow / uber / lyft,
built on the pickup side and the dropoff side separately.

This is a port of an existing R-based single-sided aggregation recipe (referred
to below as "the R script"), NOT of our OD pipeline. The R method is reproduced
faithfully, which means it deliberately differs from build_nta_od_2026.py in
three ways -- keep them in mind when comparing the two sets of panels:

  1. NO minimum weight. Every non-zero zone->NTA area share is kept and the row
     is renormalised per zone. Our OD panels drop shares below 0.01 (a rule
     recovered by reproducing the shipped 2023-2025 panels). Single-sided this
     matters much less: one weight join spreads a zone over ~4.7 NTAs, against
     ~22 NTA pairs for the tensor product on both endpoints.
  2. Filtering happens AFTER aggregation, on the means: the R script keeps
     zone-hours whose average_distance and average_trip_time are both > 0, and
     applies no trip-level predicate. A zone-hour containing a few zero-distance
     or negative-duration trips therefore survives, with those trips still
     inside the mean. Our OD pipeline filters at trip level instead.
  3. NO Stage-7 cleaning -- no physical bounds, no percentile clip.

Method, following the R script stage for stage:
  map      per month file, group to (hour, LocationID) and keep the ADDITIVE
           quantities total_rides / total_distance / total_trip_mins
  reduce   regroup across files, only then divide to get the means
           (storing means per file and averaging them would be a mean of means)
  weights  wide lookup -> long, keep weight > 0, renormalise per LocationID
  assign   join on the zone, adjusted_rides = weight * rides, group to
           (nta, day, hour), metrics as weighted.mean(..., adjusted_rides)

Duration comes from dropoff - pickup for all three modes, as in the R script,
even though the fhvhv files also carry a metered `trip_time`.

Two deviations from the R source, both deliberate:
  * `day` is undefined in the R script -- the final group_by references a column
    that is never created. It is read here as the calendar date, with `hour` as
    the hour of day, which is the only reading under which that group_by means
    anything.
  * the ride column is `total_rides`, not a mode-specific name, because
    three modes share this code.

Output is INTERMEDIATE, written to _sides_staging/<mode>/<side>/<era>/.
Run code/build_nta_single_combined.py afterwards to join the two sides into the
published panels at nta_single_riderships/<mode>/<era>/.

Usage:
    python code/build_nta_single.py                  # 3 modes x 2 sides x 2 eras
    python code/build_nta_single.py yellow           # one mode
    python code/build_nta_single.py --era 2026       # one era
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
DATA = os.path.join(ROOT, "data")
REF = os.path.join(ROOT, "reference")
# The per-side panels are an intermediate: build_nta_single_combined.py joins
# them into the published nta_single_riderships/<mode>/<era>/ layout. They land
# in a staging directory so a re-run cannot resurrect the old pick_up/drop_off
# folders next to the published files.
OUTROOT = os.path.join(ROOT, "_sides_staging")
# 2023-2025 raw trips ({yellow,uber,lyft}/<mode>_YYYY-MM.parquet); configurable
HIST = os.environ.get("HISTORICAL_DATA_ROOT", os.path.join(DATA, "historical"))

WEIGHT_LONG = os.path.join(REF, "taxi_nta_weight_long.parquet")

NTA_GEOJSON = os.path.join(REF, "nta_2020.geojson")

# TLC modes: zone ids, assigned to NTAs through the area-share matrix.
MODES = ["yellow", "uber", "lyft"]
SIDES = {"pick_up": "PULocationID", "drop_off": "DOLocationID"}

# Citi Bike: real GPS, so no zone and no weight matrix -- the endpoint is placed
# in an NTA by point-in-polygon and the distance is Haversine. The weight
# distribution in the R script exists only because TLC publishes zone ids and no
# coordinates; with coordinates it has nothing to do. Everything else follows the
# same R recipe: additive quantities first, means second, then the > 0 filter on
# the means, and no Stage-7 cleaning. total_rides is therefore a whole number
# here, not the fractional count the weight split produces for the TLC modes.
GPS_MODES = ["citibike"]
GPS_SIDES = {"pick_up": ("start_lat", "start_lng"), "drop_off": ("end_lat", "end_lng")}
ALL_MODES = MODES + GPS_MODES

# R output columns: nta, day, hour, <mode>_total_rides, average_distance, average_duration
OUT_SCHEMA = pa.schema([
    ("nta", pa.string()),
    ("day", pa.date32()),
    ("hour", pa.int64()),
    ("total_rides", pa.float64()),
    ("average_distance", pa.float64()),
    ("average_duration", pa.float64()),
])


def p(x) -> str:
    return str(x).replace("\\", "/")


def con_new() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("SET memory_limit='8GB'; SET threads=6; SET preserve_insertion_order=false;")
    return c


def load_weights(con) -> None:
    """Every non-zero share, renormalised per zone -- the R script's rule."""
    con.execute("""
      CREATE OR REPLACE TABLE W AS
      WITH d AS (
        SELECT DISTINCT ON (LocationID, nta_code) LocationID, nta_code,
               proportion_normalized AS w0
        FROM read_parquet('%s') WHERE proportion_normalized > 0
      )
      SELECT LocationID, nta_code, w0 / SUM(w0) OVER (PARTITION BY LocationID) AS w
      FROM d;
    """ % p(WEIGHT_LONG))
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT LocationID) FROM W").fetchone()
    print("  weights: %d (zone,NTA) entries over %d zones, no minimum share" % n, flush=True)


def sources(mode: str, era: str) -> list:
    """(label, path, extra WHERE) per month file for one mode and era."""
    out = []
    if mode in GPS_MODES:
        # Only 2026 is held locally. The 2023-2025 Citi Bike trip files are not
        # held locally (only aggregated panels were) and would have to come
        # back from the S3 bucket, ~19 GB.
        if era != "2026":
            return []
        for f in sorted(glob.glob(os.path.join(DATA, "citibike", "citibike_2026-*.parquet"))):
            ym = os.path.basename(f).rsplit("_", 1)[1].replace(".parquet", "")
            out.append((ym, f, ""))
        return out
    if era == "2023-2025":
        for f in sorted(glob.glob(os.path.join(HIST, mode, "%s_20*.parquet" % mode))):
            ym = os.path.basename(f).rsplit("_", 1)[1].replace(".parquet", "")
            out.append((ym, f, ""))
    else:
        if mode == "yellow":
            pat = os.path.join(DATA, "tlc_yellow", "yellow_tripdata_2026-*.parquet")
            lic = ""
        else:
            pat = os.path.join(DATA, "tlc_fhvhv", "fhvhv_tripdata_2026-*.parquet")
            lic = " AND hvfhs_license_num = '%s'" % ("HV0003" if mode == "uber" else "HV0005")
        for f in sorted(glob.glob(pat)):
            ym = os.path.basename(f).rsplit("_", 1)[1].replace(".parquet", "")
            out.append((ym, f, lic))
    return out


def cols_for(mode: str, path: str):
    """(pickup, dropoff, distance) column names for this file."""
    names = set(pq.ParquetFile(path).schema_arrow.names)
    if "tpep_pickup_datetime" in names:
        return "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"
    return "pickup_datetime", "dropoff_datetime", "trip_miles"


def map_month(con, mode, path, ym, extra, zone_col, tmp_tbl) -> None:
    """R stage 1: additive quantities per (hour, zone) for one month file.

    `hour` is the pickup timestamp truncated to the hour, exactly as
    floor_date(tpep_pickup_datetime, "hour") does; the calendar date and the
    hour of day are both derived from it in the reduce stage.

    No trip-level predicate, matching the R script. The only rows dropped are
    those with a NULL zone, which cannot be assigned to any NTA.
    """
    pu, do, dist = cols_for(mode, path)
    yy, mm = int(ym[:4]), int(ym[5:7])
    con.execute(f"""
      INSERT INTO {tmp_tbl}
      SELECT date_trunc('hour', {pu}) AS hr,
             {zone_col}               AS zone,
             COUNT(*)                 AS total_rides,
             SUM({dist})              AS total_distance,
             SUM(EPOCH({do} - {pu}) / 60.0) AS total_trip_mins
      FROM read_parquet('{p(path)}')
      WHERE {zone_col} IS NOT NULL
        AND EXTRACT(year FROM {pu}) = {yy} AND EXTRACT(month FROM {pu}) = {mm}
        {extra}
      GROUP BY 1, 2;
    """)


def reduce_and_assign(con, tmp_tbl, out_path, where_month="") -> int:
    """R stages 2-4: means, post-aggregation filter, weight join, NTA grouping."""
    con.execute(f"""
      COPY (
        WITH agg AS (
          SELECT hr, zone,
                 SUM(total_rides)                          AS rides,
                 SUM(total_distance) / SUM(total_rides)    AS average_distance,
                 SUM(total_trip_mins) / SUM(total_rides)   AS average_trip_time
          FROM {tmp_tbl}
          {where_month}
          GROUP BY 1, 2
        ),
        keep AS (                      -- R: filter(average_distance > 0, average_trip_time > 0)
          SELECT * FROM agg WHERE average_distance > 0 AND average_trip_time > 0
        ),
        j AS (
          SELECT W.nta_code AS nta,
                 CAST(keep.hr AS DATE)        AS day,
                 EXTRACT(hour FROM keep.hr)   AS hour,
                 keep.rides * W.w             AS adjusted_rides,
                 keep.average_distance, keep.average_trip_time
          FROM keep JOIN W ON W.LocationID = keep.zone
        )
        SELECT nta, day, hour,
               SUM(adjusted_rides) AS total_rides,
               SUM(average_distance  * adjusted_rides) / NULLIF(SUM(adjusted_rides), 0)
                 AS average_distance,
               SUM(average_trip_time * adjusted_rides) / NULLIF(SUM(adjusted_rides), 0)
                 AS average_duration
        FROM j
        GROUP BY 1, 2, 3
      ) TO '{p(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    pf = pq.ParquetFile(out_path)
    # DuckDB infers the output types; make sure they are still the ones declared,
    # so a change to the query cannot silently alter the published schema.
    if pf.schema_arrow != OUT_SCHEMA:
        raise RuntimeError("schema drift in %s: got %s, want %s"
                           % (out_path, pf.schema_arrow, OUT_SCHEMA))
    return pf.metadata.num_rows


def load_nta_polygons(con) -> None:
    con.execute("CREATE OR REPLACE TABLE nta AS SELECT * FROM ST_Read('%s');" % p(NTA_GEOJSON))
    cols = [r[1] for r in con.execute("PRAGMA table_info('nta');").fetchall()]
    if "geom" not in cols and "geometry" in cols:
        con.execute("ALTER TABLE nta RENAME COLUMN geometry TO geom;")


def map_month_gps(con, path, ym, lat_col, lng_col, tmp_tbl) -> None:
    """Citi Bike equivalent of map_month: point-in-polygon straight to an NTA.

    Trips whose endpoint falls outside every NTA polygon are dropped, the same
    way a TLC trip in a zone absent from the weight matrix is dropped -- there is
    no NTA to attribute it to.
    """
    yy, mm = int(ym[:4]), int(ym[5:7])
    # All four coordinates are required, on both sides: the distance is the
    # Haversine between the endpoints, so a trip missing either end has no
    # distance to contribute. About 0.27% of Citi Bike trips end away from a
    # station and carry no end coordinate.
    con.execute(f"""
      INSERT INTO {tmp_tbl}
      WITH t AS (
        SELECT started_at, ended_at, start_lat, start_lng, end_lat, end_lng
        FROM read_parquet('{p(path)}')
        WHERE start_lat IS NOT NULL AND start_lng IS NOT NULL
          AND end_lat   IS NOT NULL AND end_lng   IS NOT NULL
          AND EXTRACT(year FROM started_at) = {yy}
          AND EXTRACT(month FROM started_at) = {mm}
      ),
      g AS (
        SELECT t.*, n.NTA2020 AS nta
        FROM t LEFT JOIN nta n
          ON ST_Intersects(n.geom, ST_Point(t.{lng_col}, t.{lat_col}))
      )
      SELECT date_trunc('hour', started_at) AS hr,
             nta,
             COUNT(*) AS total_rides,
             SUM(2 * 3958.8 * ASIN(SQRT(
                   POWER(SIN(RADIANS((end_lat - start_lat) / 2)), 2)
                 + COS(RADIANS(start_lat)) * COS(RADIANS(end_lat))
                 * POWER(SIN(RADIANS((end_lng - start_lng) / 2)), 2)))) AS total_distance,
             SUM(EPOCH(ended_at - started_at) / 60.0) AS total_trip_mins
      FROM g
      WHERE nta IS NOT NULL
      GROUP BY 1, 2;
    """)


def build_gps(mode: str, side: str, era: str) -> None:
    """Citi Bike: same stages as build(), point-in-polygon instead of weights."""
    lat_col, lng_col = GPS_SIDES[side]
    src = sources(mode, era)
    if not src:
        print("  [%s/%s/%s] no source files" % (mode, side, era), flush=True)
        return
    outdir = os.path.join(OUTROOT, mode, side, era)
    os.makedirs(outdir, exist_ok=True)
    t0 = time.time()
    con = con_new()
    con.execute("INSTALL spatial; LOAD spatial;")
    load_nta_polygons(con)
    con.execute("CREATE OR REPLACE TABLE S (hr TIMESTAMP, nta VARCHAR, total_rides BIGINT, "
                "total_distance DOUBLE, total_trip_mins DOUBLE);")
    for ym, path, _ in src:
        map_month_gps(con, path, ym, lat_col, lng_col, "S")
    n_raw = con.execute("SELECT COUNT(*) FROM S").fetchone()[0]
    print("  [%s/%s/%s] %d month files -> %s (hour,NTA) buckets"
          % (mode, side, era, len(src), format(n_raw, ",")), flush=True)
    total = 0
    for ym, _, _ in src:
        out = os.path.join(outdir, "%s_%s_%s.parquet" % (mode, side, ym))
        w = ("WHERE EXTRACT(year FROM hr) = %d AND EXTRACT(month FROM hr) = %d"
             % (int(ym[:4]), int(ym[5:7])))
        n = reduce_gps(con, "S", out, w)
        total += n
        print("        %s  %9s rows  %6.1f MB"
              % (ym, format(n, ","), os.path.getsize(out) / 1e6), flush=True)
    print("        total %s rows" % format(total, ","), flush=True)
    con.close()
    print("  [%s/%s/%s] %.1f min" % (mode, side, era, (time.time() - t0) / 60), flush=True)


def reduce_gps(con, tmp_tbl, out_path, where_month="") -> int:
    """Means, then the R post-aggregation filter. No weight join to do."""
    con.execute(f"""
      COPY (
        WITH agg AS (
          SELECT nta,
                 CAST(hr AS DATE)      AS day,
                 EXTRACT(hour FROM hr) AS hour,
                 SUM(total_rides)::DOUBLE                 AS total_rides,
                 SUM(total_distance)  / SUM(total_rides)  AS average_distance,
                 SUM(total_trip_mins) / SUM(total_rides)  AS average_duration
          FROM {tmp_tbl}
          {where_month}
          GROUP BY 1, 2, 3
        )
        SELECT nta, day, hour, total_rides, average_distance, average_duration
        FROM agg WHERE average_distance > 0 AND average_duration > 0
      ) TO '{p(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    pf = pq.ParquetFile(out_path)
    if pf.schema_arrow != OUT_SCHEMA:
        raise RuntimeError("schema drift in %s: got %s, want %s"
                           % (out_path, pf.schema_arrow, OUT_SCHEMA))
    return pf.metadata.num_rows


def build(mode: str, side: str, era: str) -> None:
    zone_col = SIDES[side]
    src = sources(mode, era)
    if not src:
        print("  [%s/%s/%s] no source files" % (mode, side, era), flush=True)
        return
    outdir = os.path.join(OUTROOT, mode, side, era)
    os.makedirs(outdir, exist_ok=True)
    t0 = time.time()
    con = con_new()
    load_weights(con)
    con.execute("CREATE OR REPLACE TABLE S (hr TIMESTAMP, zone BIGINT, total_rides BIGINT, "
                "total_distance DOUBLE, total_trip_mins DOUBLE);")
    for ym, path, extra in src:
        map_month(con, mode, path, ym, extra, zone_col, "S")
    n_raw = con.execute("SELECT COUNT(*) FROM S").fetchone()[0]
    print("  [%s/%s/%s] %d month files -> %s (hour,zone) buckets"
          % (mode, side, era, len(src), format(n_raw, ",")), flush=True)

    if era == "2023-2025":
        out = os.path.join(outdir, "%s_%s_2023-2025.parquet" % (mode, side))
        n = reduce_and_assign(con, "S", out)
        print("        one file  %11s rows  %7.1f MB"
              % (format(n, ","), os.path.getsize(out) / 1e6), flush=True)
    else:
        total = 0
        for ym, _, _ in src:
            out = os.path.join(outdir, "%s_%s_%s.parquet" % (mode, side, ym))
            w = ("WHERE EXTRACT(year FROM hr) = %d AND EXTRACT(month FROM hr) = %d"
                 % (int(ym[:4]), int(ym[5:7])))
            n = reduce_and_assign(con, "S", out, w)
            total += n
            print("        %s  %9s rows  %6.1f MB"
                  % (ym, format(n, ","), os.path.getsize(out) / 1e6), flush=True)
        print("        total %s rows" % format(total, ","), flush=True)
    con.close()
    print("  [%s/%s/%s] %.1f min" % (mode, side, era, (time.time() - t0) / 60), flush=True)


def main() -> None:
    argv = sys.argv[1:]
    eras = ["2023-2025", "2026"]
    if "--era" in argv:
        i = argv.index("--era")
        eras = [argv[i + 1]]
        argv = argv[:i] + argv[i + 2:]      # drop the flag AND its value
    modes = [a for a in argv if not a.startswith("-")] or ALL_MODES
    bad = [m for m in modes if m not in ALL_MODES]
    if bad:
        raise SystemExit("unknown mode(s): %s; expected one of %s" % (bad, ALL_MODES))
    t0 = time.time()
    for mode in modes:
        gps = mode in GPS_MODES
        for side in (GPS_SIDES if gps else SIDES):
            for era in eras:
                if gps and era != "2026":
                    continue          # no local Citi Bike trip data before 2026
                print("\n=== %s / %s / %s ===" % (mode, side, era), flush=True)
                (build_gps if gps else build)(mode, side, era)
    print("\n[done] %.1f min total" % ((time.time() - t0) / 60), flush=True)


if __name__ == "__main__":
    main()
