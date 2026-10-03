# data/ (empty on purpose)

Raw inputs are not distributed (about 5.6 GB for 2026). Rebuild them with:

```bash
python code/download_mta_od.py         # -> data/mta_subway_od/mta_subway_od_2026-MM.parquet
python code/download_tlc_citibike.py   # -> data/tlc_yellow/, data/tlc_green/, data/tlc_fhvhv/, data/citibike/
python code/verify_data.py
python code/verify_tlc_citibike.py
```

2023–2025 TLC trips for the historical builds go in `data/historical/{yellow,green,uber,lyft}/`,
or in the folder named by `HISTORICAL_DATA_ROOT`. See the main README.
