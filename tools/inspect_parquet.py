from pathlib import Path

import pandas as pd


files = list(
    Path("bronze/orders")
    .rglob("*.parquet")
)

print(
    f"Files found: {len(files)}"
)

for file in files:
    df = pd.read_parquet(file)

    print(
        "\n",
        file,
    )

    print(
        f"Rows: {len(df):,}"
    )

    print(
        df.head()
    )