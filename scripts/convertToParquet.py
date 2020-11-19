import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import os

chd = os.chdir(f"{os.environ['HOME']}/projects/aws2open/")

mydf = pd.read_json("json/keys.json", orient="records", dtype="dict")
table_from_pandas = pa.Table.from_pandas(mydf)
pq.write_table(table_from_pandas, "parquet/keys.parquet")
