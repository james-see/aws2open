import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.io.json import json_normalize

import os
chd = os.chdir(f"{os.environ['HOME']}/projects/aws2open/")