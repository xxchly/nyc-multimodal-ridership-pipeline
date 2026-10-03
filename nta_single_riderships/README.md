# nta_single_riderships/ (empty on purpose)

Output folder for the single-sided NTA x date x hour panels (about 1.1 GB, not distributed).
Layout after a build: `<mode>/2023-2025/<mode>_2023-2025.parquet` and `<mode>/2026/<mode>_2026-MM.parquet`.

```bash
python code/build_nta_single.py            # per-side intermediates -> _sides_staging/
python code/build_nta_single_combined.py   # yellow uber lyft citibike
python code/build_nta_single_subway.py     # subway, collapsed from nta_od_riderships/subway/
python code/verify_nta_single.py
```

The 9-column schema is documented in the main README.
