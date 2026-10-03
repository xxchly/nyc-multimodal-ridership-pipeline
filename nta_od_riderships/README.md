# nta_od_riderships/ (empty on purpose)

Output folder for the NTA-pair x date x hour OD panels (about 19 GB, not distributed).
Layout after a build: `<mode>/2026/<mode>_2026-MM.parquet`, plus `green/2023-2025/green_2023-2025.parquet`.

```bash
python code/build_nta_od_2026.py    # yellow uber lyft citibike subway (2026)
python code/build_nta_od_green.py   # green 2023-2026 (needs HISTORICAL_DATA_ROOT)
python code/verify_nta_od.py        # also needs <mode>/2023-2025/<mode>_2023-2025.parquet
```

The 17-column schema is documented in the main README. `sample/green_2026-01.parquet` is one built file.
