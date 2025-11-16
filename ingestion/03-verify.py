#!/usr/bin/env -S uv run

import os

import pandas as pd
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()

    warehouse_id = os.getenv("WAREHOUSE_ID")
    w = WorkspaceClient()

    res = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement="SELECT * FROM dz.dz.data",
    )

    if res.result and res.result.data_array:
        column_names = [col.name for col in res.manifest.schema.columns]
        df = pd.DataFrame(res.result.data_array, columns=column_names)
        print(df)
    else:
        print("No data returned or an error occurred.")
