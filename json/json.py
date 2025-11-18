# Databricks notebook source
spark.read.json ('/Volumes/dz/dz/dz-vol-json').display()

# COMMAND ----------

files = dbutils.fs.ls("/Volumes/dz/dz/dz-vol-json")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from text.`/Volumes/dz/dz/dz-vol-json`

# COMMAND ----------



df = spark.read.text("/Volumes/dz/dz/dz-vol-json")

df.display(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import input_file_name,col
df = spark.read.text("/Volumes/dz/dz/dz-vol-json")

df_with_path = df.select("*", col("_metadata.file_path").alias("source_path"))
df_with_path.display()


# COMMAND ----------

df = spark.read.json("/Volumes/dz/dz/dz-vol-json")
df.write.saveAsTable("dz.dz.data_json_bronze", mode="overwrite")

spark.table("dz.dz.data_json_bronze").display()

# COMMAND ----------

from pyspark.sql.functions import col,unbase64
df = spark.table("dz.dz.data_json_bronze").select(
    col("key"),
    unbase64("key").alias("decoded_key"),
    )
df.show()
df.display()

# COMMAND ----------

from pyspark.sql.functions import col,unbase64

df = spark.table("dz.dz.data_json_bronze").select(
    col("key"),
    unbase64("key").alias("decoded_key"),
    col("value"),
    unbase64("value" ).alias("decoded_value"),
    )
    
df.show()
df.display()

# COMMAND ----------

from pyspark.sql.functions import col,unbase64

df = spark.table("dz.dz.data_json_bronze").select(
    col("key"),
    unbase64("key").cast("string").alias("decoded_key"),
    col("value"),
    unbase64("value" ).cast("string").alias("decoded_value"),
    )
    
df.show()
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC explain extended table dz.dz.data_json_bronze

# COMMAND ----------

from pyspark.sql.functions import col,unbase64
bronze_table = "dz.dz.data_json_bronze"
decoded_table = f"{bronze_table}_decoded"

df = spark.table(bronze_table).select(
    
    unbase64("key").cast("string").alias("decoded_key"),
    col("offset"),
    col("timestamp"),
    col("topic"),
    col("partition"),
    unbase64("value" ).cast("string").alias("decoded_value"),
    )
    


df.write.saveAsTable(decoded_table, mode="overwrite")

spark.table(decoded_table).display()