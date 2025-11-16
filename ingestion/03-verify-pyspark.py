#!/usr/bin/env -S uv run -- python
from dotenv import load_dotenv
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

spark: SparkSession = DatabricksSession.builder.getOrCreate()

df = spark.sql("SELECT * FROM dz.dz.data")

df.show()
