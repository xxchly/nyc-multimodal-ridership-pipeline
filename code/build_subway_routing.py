# -*- coding: utf-8 -*-
"""
Build the subway NTA-pair routing lookup: (origin_nta, dest_nta, bucket) ->
avg_distance / avg_duration / avg_mph.

The MTA Subway OD Ridership Estimate carries ridership only; it has no trip
distance or duration. Those three columns are derived by routing over the GTFS
schedule, exactly as the 2023-2025 panels were built.

Algorithm (ported verbatim from the subway routing step of the earlier internal
pipeline that built the 2023-2025 panels, steps 5.2-5.7):
  5.2 stops -> parent_station (strip N/S platform suffix) -> point-in-polygon -> NTA
  5.3 trips -> service day-type -> one of 7 buckets by first departure hour
        Weekday{00-05,06-09,10-15,16-19,20-23} + Saturday_all + Sunday_all
  5.4 per bucket: directed graph; segment edge wtime = min(arr_{n+1} - dep_n),
        wdist = Haversine; transfer edges wtime = min_transfer_time, wdist = 0
  5.5 all-pairs Dijkstra by time; accumulate Haversine distance along the min-time path
  5.6 station service frequency within the bucket
  5.7 aggregate station pairs to NTA pairs, weighted by origin_freq * dest_freq

Difference from the 2023-2025 build: that one used the 2025-12-04 GTFS snapshot;
this uses the 2026-08-27 feed (service window 2026-05-26 .. 2026-10-31), chosen
because the data being routed is 2026.

Output: reference/subway_nta_routing_<feed>.parquet
"""
from __future__ import annotations

import math
import os
import sys
import time

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from shapely.geometry import Point

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GTFS = os.path.join(ROOT, "reference", "gtfs_subway_20260827")
NTA_GEOJSON = os.path.join(ROOT, "reference", "nta_2020.geojson")
OUT = os.path.join(ROOT, "reference", "subway_nta_routing_20260827.parquet")

R_EARTH_MI = 3958.8
WEEKDAY_BUCKETS = [(0, 5, "Weekday_00_05"), (6, 9, "Weekday_06_09"),
                   (10, 15, "Weekday_10_15"), (16, 19, "Weekday_16_19"),
                   (20, 23, "Weekday_20_23")]
BUCKETS = [b[2] for b in WEEKDAY_BUCKETS] + ["Saturday_all", "Sunday_all"]


def haversine_mi(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R_EARTH_MI * math.asin(min(1.0, math.sqrt(a)))


def gtfs_seconds(hms: str) -> float:
    """GTFS time HH:MM:SS -> seconds; HH may exceed 24."""
    try:
        h, m, s = (int(x) for x in str(hms).split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return np.nan


def parent_of(stop_id: str) -> str:
    """Strip trailing N/S direction suffix to collapse platforms to parent station."""
    sid = str(stop_id)
    return sid[:-1] if sid and sid[-1] in ("N", "S") else sid


def stations_to_nta() -> pd.DataFrame:
    stops = pd.read_csv(os.path.join(GTFS, "stops.txt"), dtype=str)
    stops["parent"] = stops["stop_id"].map(parent_of)
    stops["stop_lat"] = pd.to_numeric(stops["stop_lat"], errors="coerce")
    stops["stop_lon"] = pd.to_numeric(stops["stop_lon"], errors="coerce")
    st = (stops.dropna(subset=["stop_lat", "stop_lon"])
                .groupby("parent", as_index=False)
                .agg(lat=("stop_lat", "mean"), lon=("stop_lon", "mean")))
    nta = gpd.read_file(NTA_GEOJSON)[["NTA2020", "geometry"]].to_crs(4326)
    gdf = gpd.GeoDataFrame(st, geometry=[Point(xy) for xy in zip(st.lon, st.lat)], crs=4326)
    joined = gpd.sjoin(gdf, nta, how="left", predicate="within")
    joined = joined.drop_duplicates("parent")
    return joined[["parent", "lat", "lon", "NTA2020"]].rename(columns={"NTA2020": "nta"})


def trip_buckets() -> pd.DataFrame:
    trips = pd.read_csv(os.path.join(GTFS, "trips.txt"), dtype=str)
    cal = pd.read_csv(os.path.join(GTFS, "calendar.txt"), dtype=str)
    dow = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    for c in dow:
        cal[c] = pd.to_numeric(cal[c], errors="coerce").fillna(0).astype(int)

    def daytype(row) -> str:
        if row["saturday"] and not any(row[d] for d in dow[:5]):
            return "Saturday"
        if row["sunday"] and not any(row[d] for d in dow[:5]):
            return "Sunday"
        return "Weekday"

    cal["daytype"] = cal.apply(daytype, axis=1)
    trips = trips.merge(cal[["service_id", "daytype"]], on="service_id", how="left")

    st = pd.read_csv(os.path.join(GTFS, "stop_times.txt"), dtype=str)
    st["dep"] = st["departure_time"].map(gtfs_seconds)
    first = st.groupby("trip_id", as_index=False)["dep"].min()
    trips = trips.merge(first, on="trip_id", how="left")
    trips["first_hour"] = (trips["dep"] // 3600) % 24

    def bucket(row) -> str:
        if row["daytype"] == "Saturday":
            return "Saturday_all"
        if row["daytype"] == "Sunday":
            return "Sunday_all"
        h = row["first_hour"]
        for lo, hi, name in WEEKDAY_BUCKETS:
            if lo <= h <= hi:
                return name
        return "Weekday_00_05"

    trips["bucket"] = trips.apply(bucket, axis=1)
    return trips[["trip_id", "bucket"]]


def route_bucket(bucket, st, trips_b, transfers, coord):
    tids = set(trips_b.loc[trips_b.bucket == bucket, "trip_id"])
    sb = st[st.trip_id.isin(tids)].sort_values(["trip_id", "seq"])

    G = nx.DiGraph()
    freq = {}
    for tid, grp in sb.groupby("trip_id"):
        rows = grp.to_records(index=False)
        seen = set()
        for i in range(len(rows) - 1):
            u, v = parent_of(rows[i].stop_id), parent_of(rows[i + 1].stop_id)
            for p in (u, v):
                if p not in seen:
                    freq[p] = freq.get(p, 0) + 1
                    seen.add(p)
            dt = rows[i + 1].arr - rows[i].dep
            if u == v or not (dt > 0) or u not in coord or v not in coord:
                continue
            d = haversine_mi(*coord[u], *coord[v])
            for a, b in ((u, v), (v, u)):
                if not G.has_edge(a, b) or dt < G[a][b]["wtime"]:
                    G.add_edge(a, b, wtime=dt, wdist=d)

    for r in transfers.itertuples(index=False):
        a, b = parent_of(r.from_stop_id), parent_of(r.to_stop_id)
        if a in coord and b in coord and a != b:
            w = r.min_transfer_time if pd.notna(r.min_transfer_time) else 0.0
            G.add_edge(a, b, wtime=float(w), wdist=0.0)

    recs = []
    for s in G.nodes:
        dist_t, paths = nx.single_source_dijkstra(G, s, weight="wtime")
        for t, tt in dist_t.items():
            if t == s:
                continue
            path = paths[t]
            dd = sum(G[path[i]][path[i + 1]]["wdist"] for i in range(len(path) - 1))
            recs.append((s, t, tt, dd))
    routed = pd.DataFrame(recs, columns=["s", "t", "time_s", "dist_mi"])
    routed["bucket"] = bucket
    return routed, freq


def aggregate_to_nta(routed, freq, s2n) -> pd.DataFrame:
    nmap = dict(zip(s2n.parent, s2n.nta))
    r = routed.copy()
    r["o_nta"] = r.s.map(nmap)
    r["d_nta"] = r.t.map(nmap)
    r["fo"] = r.s.map(freq).fillna(0.0)
    r["fd"] = r.t.map(freq).fillna(0.0)
    r = r.dropna(subset=["o_nta", "d_nta"])
    r["w"] = r.fo * r.fd
    r["wt"] = r.time_s * r.w
    r["wdi"] = r.dist_mi * r.w
    g = (r.groupby(["o_nta", "d_nta", "bucket"], as_index=False)
           .agg(wt=("wt", "sum"), wdi=("wdi", "sum"), w=("w", "sum")))
    g = g[g.w > 0]
    g["avg_duration"] = g.wt / g.w / 60.0                # seconds -> minutes
    g["avg_distance"] = g.wdi / g.w
    g["avg_mph"] = 60.0 * g.avg_distance / g.avg_duration
    return g[["o_nta", "d_nta", "bucket", "avg_distance", "avg_duration", "avg_mph"]]


def main() -> None:
    t0 = time.time()
    print("[5.2] station -> NTA ...", flush=True)
    s2n = stations_to_nta()
    print("  %d parent stations, %d mapped to an NTA"
          % (len(s2n), s2n.nta.notna().sum()), flush=True)
    coord = {p: (la, lo) for p, la, lo in zip(s2n.parent, s2n.lat, s2n.lon)}

    print("[5.3] trip buckets ...", flush=True)
    trips_b = trip_buckets()
    print("  %d trips; per bucket: %s"
          % (len(trips_b), trips_b.bucket.value_counts().to_dict()), flush=True)

    st = pd.read_csv(os.path.join(GTFS, "stop_times.txt"), dtype=str)
    st["arr"] = st["arrival_time"].map(gtfs_seconds)
    st["dep"] = st["departure_time"].map(gtfs_seconds)
    st["seq"] = pd.to_numeric(st["stop_sequence"], errors="coerce")
    tp = os.path.join(GTFS, "transfers.txt")
    if os.path.exists(tp):
        transfers = pd.read_csv(tp, dtype={"from_stop_id": str, "to_stop_id": str})
        if "min_transfer_time" in transfers:
            transfers["min_transfer_time"] = pd.to_numeric(
                transfers["min_transfer_time"], errors="coerce")
    else:
        transfers = pd.DataFrame(columns=["from_stop_id", "to_stop_id", "min_transfer_time"])

    print("[5.4-5.7] per-bucket routing ...", flush=True)
    parts = []
    for b in BUCKETS:
        tb = time.time()
        routed, freq = route_bucket(b, st, trips_b, transfers, coord)
        agg = aggregate_to_nta(routed, freq, s2n)
        parts.append(agg)
        print("  %-16s %7d station pairs -> %6d NTA pairs   %.1fs"
              % (b, len(routed), len(agg), time.time() - tb), flush=True)
        del routed

    tbl = pd.concat(parts, ignore_index=True)
    tbl.to_parquet(OUT, engine="pyarrow", compression="zstd", index=False)
    print("\nwrote %s rows -> %s  (%.1f min)"
          % (format(len(tbl), ","), os.path.basename(OUT), (time.time() - t0) / 60), flush=True)
    print(tbl.groupby("bucket").agg(
        pairs=("o_nta", "size"), mi=("avg_distance", "mean"),
        min_=("avg_duration", "mean"), mph=("avg_mph", "mean")).to_string())


if __name__ == "__main__":
    main()
