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

# COMMAND ----------


from pyspark.sql.functions import col,unbase64,get_json_object
bronze_table = "dz.dz.data_json_bronze"
decoded_table = f"{bronze_table}_decoded"


df=spark.table(decoded_table).select (
    get_json_object(col("decoded_value"), "$.order_id").alias("order_id"),
    get_json_object(col("decoded_value"), "$.customer.name").alias("customer_name"),

).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC     decoded_value:order_id,
# MAGIC     decoded_value:customer:name
# MAGIC from
# MAGIC     dz.dz.data_json_bronze_decoded

# COMMAND ----------

spark.sql("""
select 
    decoded_value:order_id,
    decoded_value:customer:name
from
    dz.dz.data_json_bronze_decoded
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select schema_of_json ('
# MAGIC {
# MAGIC   "order_id": 2001,
# MAGIC   "customer": {
# MAGIC     "id": 101,
# MAGIC     "name": "Alice",
# MAGIC     "vip": true
# MAGIC   },
# MAGIC   "items": [
# MAGIC     {
# MAGIC       "product": "Laptop",
# MAGIC       "price": 1200,
# MAGIC       "qty": 1
# MAGIC     },
# MAGIC     {
# MAGIC       "product": "Mouse",
# MAGIC       "price": 25,
# MAGIC       "qty": 2
# MAGIC     }
# MAGIC   ],
# MAGIC   "discount": 0.1,
# MAGIC   "tags": [
# MAGIC     "electronics",
# MAGIC     "bundle"
# MAGIC   ],
# MAGIC   "shipping": {
# MAGIC     "method": "express",
# MAGIC     "address": "123 Main St"
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC ') schema

# COMMAND ----------

