#!/usr/bin/env -S uv run -- python

import os

import pandas as pd
from databricks import sql
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()

    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )

    cursor = connection.cursor()

    cursor.execute("SELECT * from dz.dz.data")
    df = cursor.fetchall_arrow().to_pandas()


    print(df)

    cursor.close()
    connection.close()
