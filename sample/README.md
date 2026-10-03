# sample/

`green_2026-01.parquet` is one unmodified file from the built OD panel
(`nta_od_riderships/green/2026/green_2026-01.parquet`): green taxi, January 2026, 62,477 rows,
the 17-column NTA OD schema described in the main README.

```python
import pandas as pd
df = pd.read_parquet("sample/green_2026-01.parquet")
```
