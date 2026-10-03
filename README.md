# nyc-multimodal-ridership-pipeline

Download, verification, and panel-building code for a **verified multimodal NYC trip / OD
ridership collection, 2023–2026**, covering six modes:

| mode | source | grain of raw data |
|---|---|---|
| Subway | MTA Subway Origin-Destination Ridership Estimate | station-complex pair x hour (estimate) |
| Yellow taxi | NYC TLC trip records | individual trip |
| Green taxi | NYC TLC trip records | individual trip |
| Uber | NYC TLC High-Volume FHV (`fhvhv`) records, `HV0003` | individual trip |
| Lyft | NYC TLC High-Volume FHV (`fhvhv`) records, `HV0005` | individual trip |
| Citi Bike | Citi Bike System Data | individual trip |

Every raw file is fetched from its official source and then checked by an independent
verification script. The raw trips are aggregated to **NTA-pair x date x hour OD panels**
(17 columns, the same schema for every mode and era) and to **single-sided NTA x date x hour
panels** (pickup and dropoff side by side). The panels were built to evaluate NYC congestion
pricing, which started on 2025-01-05.

Author: **Luyang Han** ([xxchly.github.io](https://xxchly.github.io/))

> **This repository contains code and small reference files only.** No raw data and no
> built panels are included. The raw inputs are about 5.6 GB for 2026 alone and the OD panels
> about 19 GB. `data/`, `nta_od_riderships/` and `nta_single_riderships/` are empty
> placeholders. Rebuild them with the scripts below. `sample/` has one small built panel
> file so you can see the schema.

---

## Repository layout

```
nyc-multimodal-ridership-pipeline/
├── code/
│   ├── download_mta_od.py            subway OD (Socrata API, one resumable batch per timestamp)
│   ├── download_tlc_citibike.py      TLC yellow / green / fhvhv + Citi Bike (CSV -> Parquet)
│   ├── verify_data.py                subway OD vs live API (8 checks)
│   ├── verify_tlc_citibike.py        TLC byte sizes vs server, Citi Bike row recount
│   ├── build_subway_routing.py       GTFS all-pairs routing -> NTA-pair distance/time lookup
│   ├── build_nta_od_2026.py          2026 OD panels: yellow, uber, lyft, citibike, subway
│   ├── build_nta_od_green.py         green OD panel, 2023-01 .. 2026-07 in one pass
│   ├── verify_nta_od.py              2026 OD panels vs the 2023-2025 panels
│   ├── build_nta_single.py           single-sided panels, one per side (intermediate)
│   ├── build_nta_single_combined.py  joins pickup + dropoff sides -> published panels
│   ├── build_nta_single_subway.py    subway single-sided panel, collapsed from the OD panel
│   └── verify_nta_single.py          ride conservation vs raw trips
├── reference/                        small lookup files (tracked, see below)
├── sample/green_2026-01.parquet      one built OD panel file (62,477 rows, 1.2 MB)
├── data/                             raw inputs go here (empty, git-ignored)
├── nta_od_riderships/                OD panel output (empty, git-ignored)
├── nta_single_riderships/            single-sided panel output (empty, git-ignored)
├── requirements.txt
└── LICENSE                           MIT (code)
```

Every script finds the repository root through `__file__`, so the folder can be moved
anywhere without editing paths. The only external location is the optional
`HISTORICAL_DATA_ROOT` (see below).

### Reference files (`reference/`)

| file | content |
|---|---|
| `taxi_zone_lookup.csv`, `taxi_zones.zip` | TLC taxi-zone lookup table and shapefile (263 zones), as published by TLC |
| `nta_2020.geojson` | 2020 Neighborhood Tabulation Areas (262 polygons, EPSG:4326), NYC Dept. of City Planning |
| `cbd_flag.csv` | `NTA2020, NTAName, is_cbd`. The 17 NTAs flagged `True` form the Central Business District / Congestion Relief Zone |
| `taxi_nta_weight_long.parquet` | taxi zone -> NTA area-share matrix (1,220 rows, `LocationID, nta_code, proportion_normalized, ...`) |
| `subway_nta_routing_20260827.parquet` | output of `build_subway_routing.py` on the 2026-08-27 MTA GTFS feed: 116,900 rows of `(o_nta, d_nta, bucket) -> avg_distance, avg_duration, avg_mph` |

**Subway GTFS is not included.** `build_subway_routing.py` reads an unzipped static GTFS feed
from `reference/gtfs_subway_20260827/`. Download the MTA NYCT subway static GTFS from the
[MTA developer page](https://www.mta.info/developers)
(regular feed: `https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip`; supplemented feed:
`https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip`) and unzip it into that folder.
To use a different feed, rename the folder or edit `GTFS` / `OUT` at the top of the script.
MTA serves only the current feed, so the 2026-08-27 feed used here
(`feed_version 20260826-X-long-term-supplement-trip-ids`, service window 2026-05-26 to
2026-10-31) may no longer be available. That is why its routing output is shipped in
`reference/`, and you do not need GTFS to build the panels.

---

## Data sources

| mode | portal | endpoint used by the code |
|---|---|---|
| Subway OD | [MTA Subway Origin-Destination Ridership Estimate (NY Open Data, `28vm-gjqr`)](https://data.ny.gov/Transportation/MTA-Subway-Origin-Destination-Ridership-Estimate-B/28vm-gjqr/about_data) | `https://data.ny.gov/resource/28vm-gjqr` (Socrata SODA API) |
| Yellow / Green / HVFHV | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | `https://d37ci6vzurychx.cloudfront.net/trip-data/` |
| Taxi zones | same TLC page | `https://d37ci6vzurychx.cloudfront.net/misc/` |
| Citi Bike | [Citi Bike System Data](https://citibikenyc.com/system-data) | `https://s3.amazonaws.com/tripdata/` |
| NTA 2020 | [2020 Neighborhood Tabulation Areas (NYC Open Data, `9nt8-h7nd`)](https://data.cityofnewyork.us/City-Government/2020-Neighborhood-Tabulation-Areas-NTAs-/9nt8-h7nd) | already in `reference/` |
| Subway GTFS | [MTA developer resources](https://www.mta.info/developers) | see above |

TLC and Citi Bike need no key. The Socrata API works without a key too. If you set an app
token in the **environment variable `SOCRATA_APP_TOKEN`**, `download_mta_od.py` sends it as
`X-App-Token`, which raises the rate limit. No token is stored in the code.

### Snapshot built with this code (as of 2026-09)

| mode | raw files (2026) | coverage | rows |
|---|---|---|---:|
| Subway OD | `data/mta_subway_od/mta_subway_od_2026-MM.parquet` | 2026-01 .. 08 | 82,961,785 |
| Yellow | `data/tlc_yellow/yellow_tripdata_2026-MM.parquet` | 2026-01 .. 07 | 26,366,639 |
| Green | `data/tlc_green/green_tripdata_2026-MM.parquet` | 2026-01 .. 07 | 296,427 |
| Uber + Lyft | `data/tlc_fhvhv/fhvhv_tripdata_2026-MM.parquet` | 2026-01 .. 07 | 147,693,231 |
| Citi Bike | `data/citibike/citibike_2026-MM.parquet` | 2026-01 .. 08 | 30,160,678 |

---

## Per-mode caveats

**Subway OD covers one representative week per month, not consecutive days.** MTA publishes
one Monday–Sunday week per month (2026: 56 days x 24 h = 1,344 timestamps, 2026-01-05 to
2026-08-09). `estimated_average_ridership` is the *average estimated* ridership for that
weekday and hour in that month, not an actual count for that date. Origins come from entry
taps. Destinations are *inferred* by MTA's model, because the subway has no exit taps. The
downloader keeps 14 columns and drops `origin_point` / `destination_point`, which only repeat
the latitude/longitude columns as `POINT (lon lat)` strings.

**Uber and Lyft share one file.** TLC publishes all high-volume for-hire services as a single
`fhvhv_tripdata` file. Platforms are separated by `hvfhs_license_num`: `HV0003` = Uber,
`HV0005` = Lyft (2026-01..07: 106.6 M Uber trips, 72.2%, and 41.1 M Lyft trips, 27.8%).
`HV0002` (Juno) and `HV0004` (Via) no longer appear. The verification script checks that.

**TLC data needs taxi zones to be located.** TLC gives only `PULocationID` / `DOLocationID`
(taxi-zone ids) and no coordinates. Every spatial step relies on
`reference/taxi_zone_lookup.csv`, `taxi_zones.zip` and the zone -> NTA weight matrix.

**TLC month files contain a few out-of-month trips.** Yellow files carry 7–46 trips per month
whose pickup time falls outside the month in the file name. The cause is meter timestamp
errors in the published data, not the download. All builds pin each output to the month in
its file name and drop these strays. The HVFHV files have none.

**Citi Bike is converted from CSV to Parquet.** Citi Bike publishes monthly zips holding 2–6
CSV parts each. `download_tlc_citibike.py` streams them into one Parquet file per month and
keeps timestamps at **microsecond precision**, because the source has sub-second timestamps
and storing them to the second would lose data. Nulls are part of the source: e-bikes can be
parked away from a dock, so about 0.27% of trips have no end station or end coordinates.

---

## Verification design

The verification scripts never trust the downloaders' own bookkeeping. Each expected value is
re-derived from the source or along a separate code path.

* **`verify_data.py` (subway OD, 8 checks).** Re-queries the live Socrata API and checks:
  source not updated since download; total and per-month row counts; **per-month sums of
  `estimated_average_ridership`**; every timestamp the API lists is present, each with its
  exact row count; no duplicate `(timestamp, origin, destination)` keys; no nulls and sane
  value ranges; and the distinct origin/destination station sets. Matching sums plus zero
  duplicates rule out paging overlap, where the row count is right but the rows are wrong.
* **`verify_tlc_citibike.py`.** Yellow and HVFHV files are stored byte-for-byte as published,
  so each local size is compared with the server's current `Content-Length`. The script also
  counts out-of-month rows and checks the HVFHV license split. For Citi Bike, the set of local
  months is compared with a live listing of the S3 bucket. The Parquet row count is compared
  with a raw newline count of the CSVs inside the original zip, a code path independent of the
  pandas parse. That row-count check prints `[SKIP]` when the zips are not in
  `data/citibike/_zips/`.
* **`verify_nta_od.py`.** Checks that the 2026 OD panels stack onto the 2023–2025 panels:
  identical 17-column Arrow schema; one panel file per raw input month in `data/` (or, if the
  raw inputs are absent, a gap-free month sequence), each holding only its nominal month;
  `day_of_week` re-derived from `date`; NTA vocabulary and CBD flags consistent across eras;
  100% subway routing coverage; per-month magnitudes against the same months of 2025
  (informational); and a real `UNION ALL` of both eras.
* **`verify_nta_single.py`.** Checks the expected file count per mode and era, derived from
  the raw inputs present. It then checks **ride conservation** separately for each side.
  Weights sum to 1 per zone, so panel rides must equal raw trips minus three explicit loss
  channels: null zone, zone missing from the weight matrix, and post-aggregation filter. In the
  2026-09 build, all 12 TLC groups (3 modes x 2 sides x 2 eras) and both Citi Bike sides closed
  with **zero residual**.

No verification script hard-codes a snapshot count. Expected months and files come from the
live source or from the inputs on disk at run time.

---

## NTA OD panel schema (`nta_od_riderships/<mode>/...`)

Layout: `<mode>/2023-2025/<mode>_2023-2025.parquet` (one file per mode) and
`<mode>/2026/<mode>_2026-MM.parquet` (one file per month). Both eras use the same 17 columns
and Arrow types, so they concatenate directly.

| column | type | meaning |
|---|---|---|
| `mode` | string | `yellow` / `green` / `uber` / `lyft` / `citibike` / `subway` |
| `year`, `month` | int64 | |
| `date` | date32 | calendar date. **For subway, the representative-week date** |
| `day_of_week` | int64 | **1 = Monday … 7 = Sunday** |
| `hour_of_day` | int64 | 0–23 |
| `origin_nta`, `dest_nta` | string | NTA 2020 codes |
| `origin_in_cbd`, `dest_in_cbd` | bool | endpoint is one of the 17 CBD / CRZ NTAs |
| `total_rides` | double | see below |
| `log1p_total_rides` | double | `log1p(total_rides)` |
| `avg_distance` | double | miles |
| `avg_duration` | double | minutes |
| `avg_mph` | double | `60 * avg_distance / avg_duration` |
| `post` | bool | `date >= 2025-01-05` (congestion pricing start) |
| `treated` | bool | origin or destination in the CBD |

The same column has a different meaning in three places:

* **`total_rides`:** for yellow / green / uber / lyft it is a *fractional* area-weighted
  count. For Citi Bike it is an integer count. For subway it is MTA's *average estimated*
  ridership for that weekday and hour of the month.
* **Subway `date`** is the representative-week date: 252 distinct dates in 2023–2025 and 56 in
  2026. The other modes use real calendar dates.
* **Subway `avg_distance` / `avg_duration` / `avg_mph` are not observed.** They come from GTFS
  routing (`build_subway_routing.py`). The MTA OD estimate contains ridership only.

### How the OD panels are built

* **Yellow / green / uber / lyft: tensor-product area weighting.** Trips are bucketed by
  `(PU_zone, DO_zone, date, hour)` into a count plus mean distance and mean duration. The
  bucket is then spread over NTA pairs with weight `w = w_origin x w_dest` from the zone -> NTA
  area-share matrix. Distance and duration are ride-weighted. **Zone -> NTA shares below 0.01
  are dropped and the rest renormalised.** This threshold was recovered by reproducing the
  historical panels: on one test hour it gives 2,975 rows against the historical 2,974, with a
  0.03% ridership error. Keeping every non-zero share would produce about 4.2x the rows and a
  near-zero tail.
* **Citi Bike: point-in-polygon.** Real GPS coordinates are matched to NTA polygons with
  DuckDB `ST_Intersects`. Distance is Haversine.
* **Subway: point-in-polygon plus GTFS routing.** Station complexes are mapped to NTAs and
  ridership is summed per NTA pair. Distance and time come from all-pairs Dijkstra on 7 service
  graphs (weekday 00–05 / 06–09 / 10–15 / 16–19 / 20–23, Saturday, Sunday). Station pairs are
  aggregated to NTA pairs, weighted by station service frequency.
* **Cleaning (Stage 7).** Stage 7a applies physical bounds: distance 0.05–100 mi, duration
  0.5–300 min, speed 1–200 mph. Stage 7b then clips each mode at its 0.001 / 0.999 quantiles,
  **one column at a time** (distance, then duration, then speed, with each quantile recomputed
  on the rows that survived the previous cut). The thresholds are computed once over the
  whole mode, so every monthly file shares them. Green computes its thresholds over
  2023–2026 in one pass.

Panel size in the 2026-09 snapshot: 856.8 M rows / 19 GB across six modes. Uber accounts for
429 M of those rows and Lyft for 254 M.

---

## Single-sided panel schema (`nta_single_riderships/<mode>/...`)

Each row is one `(nta, day, hour)` with both directions side by side (9 columns):
`nta, day, hour, pick_up_total_rides, pick_up_average_distance, pick_up_average_duration,
drop_off_total_rides, drop_off_average_distance, drop_off_average_duration`.

* `build_nta_single.py` writes per-side intermediates to `_sides_staging/`, and
  `build_nta_single_combined.py` FULL-OUTER-joins them. A missing side gets 0 rides and NULL
  means.
* For yellow / uber / lyft, the method is a faithful port of an existing R-based recipe and is
  **deliberately different from the OD panels**. It keeps every non-zero weight (no 0.01
  threshold), filters on the means *after* aggregation (keeps zone-hours with mean distance > 0
  and mean duration > 0), and applies no Stage 7 cleaning. The two panel families are not
  directly comparable: 2026 OD-panel ridership is 93.8–94.6% of the pickup side here.
* For Citi Bike, each endpoint is matched to an NTA polygon, so counts are integers. The panel
  covers **2026 only**, because the 2023–2025 trip files were not rebuilt (about 19 GB on S3).
* For subway, `build_nta_single_subway.py` collapses the OD panel. `pick_up_*` is the origin
  side (outflow, from recorded entry taps) and `drop_off_*` is the destination side (inflow,
  inferred by MTA). This panel inherits the OD panel's Stage 7 cleaning.
* **About 5% of Uber and Lyft dropoffs cannot be mapped to an NTA.** The weight matrix
  covers 259 of 265 TLC zones and is missing 1 Newark Airport, 57 Corona, 104/105 islands,
  264 Unknown and 265 Outside of NYC. Measured losses are 0.03% on the pickup side and
  5.15% / 5.23% on the dropoff side for Uber / Lyft. The missing trips are not a random
  sample: they are trips that leave NYC. The same loss applies to the OD panels.

---

## How to run

```bash
pip install -r requirements.txt
export SOCRATA_APP_TOKEN=...            # optional

# 1. download (resumable and idempotent: finished files are skipped)
python code/download_mta_od.py          # subway OD 2026, about 20 min
python code/download_tlc_citibike.py    # yellow, green, fhvhv, citibike (2026) + taxi zones
python code/download_tlc_citibike.py yellow fhvhv        # a subset of groups

# 2. verify the raw downloads
python code/verify_data.py
python code/verify_tlc_citibike.py

# 3. build
python code/build_subway_routing.py     # optional: needs reference/gtfs_subway_20260827/
python code/build_nta_od_2026.py        # yellow uber lyft citibike subway (about 25 min)
python code/build_nta_od_green.py       # green 2023-2026, needs HISTORICAL_DATA_ROOT
python code/build_nta_single.py         # per-side intermediates (or: yellow / --era 2026)
python code/build_nta_single_combined.py
python code/build_nta_single_subway.py  # after the subway OD panel exists

# 4. verify the panels
python code/verify_nta_od.py            # needs the 2023-2025 OD panels in place
python code/verify_nta_single.py
```

**2023–2025 inputs.** The downloader fetches 2026 only (`YEAR = "2026"`). Scripts that also
build or check 2023–2025 (`build_nta_od_green.py`, `build_nta_single.py`,
`verify_nta_single.py`) read historical TLC trips from the environment variable
**`HISTORICAL_DATA_ROOT`**, which defaults to `data/historical/`:

```
$HISTORICAL_DATA_ROOT/
  yellow/yellow_YYYY-MM.parquet   needs tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, trip_distance
  green/green_YYYY-MM.parquet     needs lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID, trip_distance
  uber/uber_YYYY-MM.parquet       fhvhv rows with hvfhs_license_num = HV0003
  lyft/lyft_YYYY-MM.parquet       fhvhv rows with hvfhs_license_num = HV0005
                                  (pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, trip_miles, trip_time)
```

These are column subsets of the official TLC monthly files for 2023-01 to 2025-12, with Uber
and Lyft already split. You can produce them from the same TLC endpoint.

DuckDB downloads its `spatial` extension on first use (`INSTALL spatial`), so the first build
needs internet access. The builds set DuckDB to `memory_limit='8GB'` and `threads=6`. Edit
these in `con_new()` for smaller machines.

---

## Known gaps and limitations

1. **Months are not aligned across modes.** In the 2026-09 snapshot, subway and Citi Bike run
   through 2026-08 and TLC through 2026-07. Truncate all modes to 2026-07 before comparing
   them.
2. **The 2023–2025 OD panels for yellow / uber / lyft / citibike / subway are not rebuilt
   here.** They came from an earlier upstream pipeline (same 17-column schema, same method).
   `build_nta_od_2026.py` builds 2026 only, and `verify_nta_od.py` expects those historical
   panels to be present. Green is the exception: it is built for the full 2023–2026 period
   here.
3. **No 2026 FHV (livery) data.** Non-high-volume FHV records are not downloaded. They also
   carry no distance field.
4. **Subway routing differs between eras.** The 2026 routing uses the 2026-08-27 GTFS feed.
   The historical panels used a 2025-12-04 snapshot. No single feed covers all of 2026. The
   newer feed also moves one station into a new NTA (East Flatbush-Remsen Village, 0.10% of
   endpoints).
5. **Citi Bike reaches 8 new NTAs in 2026** (0.14% of endpoints). This reflects a real
   service-area expansion.
6. **`post` is always TRUE in 2026.** A difference-in-differences design needs the 2023–2024
   pre-period stacked in.
7. **Zone 56 (Corona) is ambiguous in the weight matrix.** The source has duplicate rows. The
   builds de-duplicate the long table and renormalise. This affects 0.1% of yellow trips and
   0.6% of ride-hail trips.
8. **`build_nta_od_green.py` builds the full period in one pass.** Without the 2023–2025
   green files it writes only the 2026 monthly files and prints a note. In that case its
   Stage 7b thresholds are computed over 2026 alone, so results differ slightly from a
   full-period build.
9. **`verify_tlc_citibike.py` checks yellow and HVFHV files but not green.** Green raw files
   are covered only indirectly, through the green OD panel build.

## License

Code: MIT (see `LICENSE`). The reference files are derived from public NYC TLC, NYC Department
of City Planning and MTA data. Data you download with these scripts is subject to each
publisher's terms of use.
