import json
import os

import dagster as dg
from dagster_duckdb import DuckDBResource

defs = dg.Definitions(
    assets=[],
    resources={"duckdb": DuckDBResource(database="data/mydb.duckdb")},
)
