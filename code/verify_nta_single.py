# -*- coding: utf-8 -*-
"""
Check the published single-sided NTA panels (nta_single_riderships/<mode>/<era>/)
against the raw trip files they were built from.

Each published row is one (nta, day, hour) carrying both directions:
pick_up_total_rides and drop_off_total_rides, plus each side's two means.

The strong check is ride conservation, run separately for each side. The weights
sum to 1 per zone, so distributing a zone's rides over its NTAs must preserve the
count exactly; for Citi Bike the endpoint lands in exactly one NTA. Any gap has
to be explained by the drop channels below, and nothing else:

  TLC modes (yellow / uber / lyft)
    * zone is NULL
    * zone not in the weight matrix. It covers 259 of the 265 TLC LocationIDs;
      missing are 1 (Newark Airport), 57 (Corona), 104 and 105 (Governors /
      Ellis / Liberty Island), 264 (Unknown) and 265 (Outside of NYC). Small on
      the pickup side (0.03-0.18%) but NOT small on the dropoff side: 5.2% of
      Uber and Lyft trips end outside the five boroughs or at EWR.
    * zone-hours removed by the post-aggregation filter

  Citi Bike
    * any of the four coordinates missing (the distance is the Haversine between
      the endpoints, so a trip missing either end has no distance to contribute)
    * endpoint outside every NTA polygon
    * NTA-hours removed by the post-aggregation filter -- round trips start and
      end at the same station, so their Haversine distance is 0

Expected counts are accumulated per raw FILE with that file's nominal month
pinned, exactly as the build does: counting the raw files as one pool would
include out-of-month strays, which the build drops on purpose so that a monthly
output never disagrees with its own filename.
"""
from __future__ import annotations

import glob
import os

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "nta_single_riderships")
DATA = os.path.join(ROOT, "data")
# 2023-2025 raw trips ({yellow,uber,lyft}/<mode>_YYYY-MM.parquet); configurable
HIST = os.environ.get("HISTORICAL_DATA_ROOT", os.path.join(DATA, "historical"))
WEIGHTS = os.path.join(ROOT, "reference", "taxi_nta_weight_long.parquet")
NTA_GEOJSON = os.path.join(ROOT, "reference", "nta_2020.geojson")

TLC_MODES = ["yellow", "uber", "lyft"]
ALL_MODES = TLC_MODES + ["citibike"]
SIDES = {"pick_up": "PULocationID", "drop_off": "DOLocationID"}
CB_SIDES = {"pick_up": ("start_lat", "start_lng"), "drop_off": ("end_lat", "end_lng")}
EXPECT_COLS = ["nta", "day", "hour",
               "pick_up_total_rides", "pick_up_average_distance", "pick_up_average_duration",
               "drop_off_total_rides", "drop_off_average_distance", "drop_off_average_duration"]
EXPECT_T = ["string", "date32[day]", "int64"] + ["double"] * 6
HAV = ("2*3958.8*ASIN(SQRT(POWER(SIN(RADIANS((end_lat-start_lat)/2)),2)"
       "+ COS(RADIANS(start_lat))*COS(RADIANS(end_lat))"
       "* POWER(SIN(RADIANS((end_lng-start_lng)/2)),2)))")
fails = []


def check(label, ok, detail=""):
    print("  [%s] %s%s" % ("PASS" if ok else "FAIL", label, ("  " + detail) if detail else ""),
          flush=True)
    if not ok:
        fails.append(label)


def p(x):
    return str(x).replace("\\", "/")


def lst(fs):
    return "[%s]" % ",".join("'%s'" % p(f) for f in fs)


def files_of(mode, era):
    return sorted(glob.glob(os.path.join(OUT, mode, era, "*.parquet")))


def raw_of(mode, era):
    """(paths, extra WHERE) for the raw trip files behind one mode and era."""
    if mode == "citibike":
        return sorted(glob.glob(os.path.join(DATA, "citibike", "citibike_2026-*.parquet"))), ""
    if era == "2023-2025":
        return sorted(glob.glob(os.path.join(HIST, mode, "%s_20*.parquet" % mode))), ""
    if mode == "yellow":
        return sorted(glob.glob(os.path.join(DATA, "tlc_yellow",
                                             "yellow_tripdata_2026-*.parquet"))), ""
    lic = "HV0003" if mode == "uber" else "HV0005"
    return (sorted(glob.glob(os.path.join(DATA, "tlc_fhvhv", "fhvhv_tripdata_2026-*.parquet"))),
            " AND hvfhs_license_num = '%s'" % lic)


def expected_files(mode, era):
    """Number of published files the build should have written, derived from the
    raw inputs on disk rather than a fixed count: 2023-2025 is one file when its
    raw months exist, 2026 is one file per raw month. Citi Bike is built for 2026
    only, so its 2023-2025 directory is expected to be empty."""
    if mode == "citibike" and era == "2023-2025":
        return 0
    raws, _ = raw_of(mode, era)
    if era == "2023-2025":
        return 1 if raws else 0
    return len(raws)


def raw_cols(path):
    n = set(pq.ParquetFile(path).schema_arrow.names)
    if "tpep_pickup_datetime" in n:
        return "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"
    return "pickup_datetime", "dropoff_datetime", "trip_miles"


con = duckdb.connect()
con.execute("SET memory_limit='8GB'; SET threads=6; SET enable_progress_bar=false;")

# ------------------------------------------------------------- 1. structure
print("\n1. published layout and 9-column schema")
for mode in ALL_MODES:
    for era in ("2023-2025", "2026"):
        d = os.path.join(OUT, mode, era)
        check("%-8s %-9s directory exists" % (mode, era), os.path.isdir(d))
        fs = files_of(mode, era)
        want = expected_files(mode, era)
        check("%-8s %-9s %d file(s), per raw inputs" % (mode, era, want), len(fs) == want,
              "%d found" % len(fs))
        if not fs:
            continue
        s = pq.ParquetFile(fs[0]).schema_arrow
        ok = list(s.names) == EXPECT_COLS and [str(t) for t in s.types] == EXPECT_T
        check("%-8s %-9s schema" % (mode, era), ok,
              "" if ok else str(list(zip(s.names, [str(t) for t in s.types]))))

# ------------------------------------------- 2. TLC ride conservation per side
print("\n2. ride conservation, TLC modes")
print("     mode   side     era        panel rides      raw trips  null-zone    unmapped   post-filter  residual")
for mode in TLC_MODES:
    for side, zone_col in SIDES.items():
        for era in ("2023-2025", "2026"):
            fs = files_of(mode, era)
            if not fs:
                continue
            panel = con.execute("SELECT SUM(%s_total_rides) FROM read_parquet(%s)"
                                % (side, lst(fs))).fetchone()[0]
            raws, extra = raw_of(mode, era)
            pu, do, dist = raw_cols(raws[0])
            total = nullz = unmapped = 0
            parts = []
            for rf in raws:
                ym = os.path.basename(rf).rsplit("_", 1)[1].replace(".parquet", "")
                yy, mm = int(ym[:4]), int(ym[5:7])
                pin = ("EXTRACT(year FROM %s) = %d AND EXTRACT(month FROM %s) = %d"
                       % (pu, yy, pu, mm))
                # raw files across the years do not all carry the same column set
                # (cbd_congestion_fee arrives in 2025), so SELECT * would not union
                parts.append("(SELECT %s, %s, %s, %s FROM read_parquet('%s') WHERE %s %s)"
                             % (pu, do, dist, zone_col, p(rf), pin, extra))
                t, z, u = con.execute(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE %s IS NULL), "
                    "COUNT(*) FILTER (WHERE %s IS NOT NULL AND %s NOT IN "
                    "(SELECT LocationID FROM read_parquet('%s'))) "
                    "FROM read_parquet('%s') WHERE %s %s"
                    % (zone_col, zone_col, zone_col, p(WEIGHTS), p(rf), pin, extra)).fetchone()
                total += t
                nullz += z
                unmapped += u
            pinned = " UNION ALL ".join(parts)
            dropped = con.execute(f"""
              WITH agg AS (
                SELECT date_trunc('hour', {pu}) AS hr, {zone_col} AS zone,
                       COUNT(*) AS n,
                       SUM({dist}) / COUNT(*) AS avg_d,
                       SUM(EPOCH({do} - {pu}) / 60.0) / COUNT(*) AS avg_t
                FROM ({pinned})
                WHERE {zone_col} IN (SELECT LocationID FROM read_parquet('{p(WEIGHTS)}'))
                GROUP BY 1, 2)
              SELECT COALESCE(SUM(n), 0) FROM agg WHERE NOT (avg_d > 0 AND avg_t > 0)
            """).fetchone()[0]
            resid = panel - (total - nullz - unmapped - dropped)
            print("     %-6s %-8s %-9s %14s %14s %10s %11s %11s %9.3f"
                  % (mode, side, era, format(int(round(panel)), ","), format(total, ","),
                     format(nullz, ","), format(unmapped, ","), format(int(dropped), ","), resid))
            check("%-6s %-8s %-9s rides conserved" % (mode, side, era), abs(resid) < 1.0,
                  "residual %.4f" % resid)

# ---------------------------------------- 3. Citi Bike conservation (GPS)
print("\n3. ride conservation, Citi Bike (point-in-polygon, no weight matrix)")
print("     side      panel rides      raw trips   no-coords   outside-NTA   post-filter  residual")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("CREATE OR REPLACE TABLE nta AS SELECT * FROM ST_Read('%s');" % p(NTA_GEOJSON))
cb_files = files_of("citibike", "2026")
cb_raw, _ = raw_of("citibike", "2026")
for side, (la, ln) in CB_SIDES.items():
    if not cb_files:
        continue
    panel = con.execute("SELECT SUM(%s_total_rides) FROM read_parquet(%s)"
                        % (side, lst(cb_files))).fetchone()[0]
    total = nocoord = 0
    parts = []
    full = ("start_lat IS NOT NULL AND start_lng IS NOT NULL "
            "AND end_lat IS NOT NULL AND end_lng IS NOT NULL")
    for rf in cb_raw:
        ym = os.path.basename(rf).rsplit("_", 1)[1].replace(".parquet", "")
        pin = ("EXTRACT(year FROM started_at) = %d AND EXTRACT(month FROM started_at) = %d"
               % (int(ym[:4]), int(ym[5:7])))
        t, nc = con.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE NOT (%s)) FROM read_parquet('%s') WHERE %s"
            % (full, p(rf), pin)).fetchone()
        total += t
        nocoord += nc
        parts.append("(SELECT started_at, ended_at, start_lat, start_lng, end_lat, end_lng "
                     "FROM read_parquet('%s') WHERE %s AND %s)" % (p(rf), pin, full))
    pinned = " UNION ALL ".join(parts)
    outside, dropped = con.execute(f"""
      WITH t AS ({pinned}),
      g AS (SELECT t.*, n.NTA2020 AS nta FROM t
            LEFT JOIN nta n ON ST_Intersects(n.geom, ST_Point(t.{ln}, t.{la}))),
      b AS (SELECT nta, date_trunc('hour', started_at) AS hr, COUNT(*) AS n,
                   SUM({HAV}) AS sd, SUM(EPOCH(ended_at - started_at)/60.0) AS st
            FROM g WHERE nta IS NOT NULL GROUP BY 1, 2)
      SELECT (SELECT COUNT(*) FROM g WHERE nta IS NULL),
             (SELECT COALESCE(SUM(n), 0) FROM b WHERE NOT (sd/n > 0 AND st/n > 0))
    """).fetchone()
    resid = panel - (total - nocoord - outside - dropped)
    print("     %-8s %14s %14s %11s %13s %13s %9.3f"
          % (side, format(int(round(panel)), ","), format(total, ","), format(nocoord, ","),
             format(int(outside), ","), format(int(dropped), ","), resid))
    check("citibike %-8s rides conserved" % side, abs(resid) < 1.0, "residual %.3f" % resid)

# --------------------------------------------- 4. join integrity and grid
print("\n4. join integrity, grid and nulls")
for mode in ALL_MODES:
    fs = files_of(mode, "2023-2025") + files_of(mode, "2026")
    if not fs:
        continue
    r = con.execute(
        "SELECT MIN(day), MAX(day), COUNT(DISTINCT hour), MIN(hour), MAX(hour), "
        "COUNT(DISTINCT nta), COUNT(*), "
        "COUNT(*) FILTER (WHERE pick_up_total_rides = 0), "
        "COUNT(*) FILTER (WHERE drop_off_total_rides = 0), "
        "COUNT(*) FILTER (WHERE pick_up_total_rides = 0 AND drop_off_total_rides = 0), "
        "COUNT(*) FILTER (WHERE nta IS NULL OR day IS NULL OR hour IS NULL), "
        "COUNT(*) FILTER (WHERE pick_up_total_rides > 0 AND pick_up_average_distance IS NULL), "
        "COUNT(*) FILTER (WHERE drop_off_total_rides > 0 AND drop_off_average_distance IS NULL) "
        "FROM read_parquet(%s)" % lst(fs)).fetchone()
    print("     %-8s %s .. %s  hours %d (%d-%d)  NTAs %d  rows %s   dropoff-only %s  pickup-only %s"
          % (mode, r[0], r[1], r[2], r[3], r[4], r[5], format(r[6], ","),
             format(r[7], ","), format(r[8], ",")))
    check("%-8s hours 0-23 complete" % mode, r[2] == 24 and r[3] == 0 and r[4] == 23)
    check("%-8s no all-zero rows (a row must have at least one side)" % mode, r[9] == 0,
          "%s found" % format(r[9], ","))
    check("%-8s keys never null" % mode, r[10] == 0, "%s found" % format(r[10], ","))
    check("%-8s a positive volume always has its means" % mode, r[11] == 0 and r[12] == 0,
          "pick_up %s, drop_off %s" % (format(r[11], ","), format(r[12], ",")))

print("\n" + "=" * 62)
if fails:
    print("%d check(s) FAILED:" % len(fails))
    for x in fails:
        print("   - %s" % x)
    raise SystemExit(1)
print("all checks passed.")
