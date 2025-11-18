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

# MAGIC %sql
# MAGIC select
# MAGIC     decoded_value,
# MAGIC     from_json(decoded_value, 'STRUCT<customer: STRUCT<id: BIGINT, name: STRING, vip: BOOLEAN>, discount: DOUBLE, items: ARRAY<STRUCT<price: BIGINT, product: STRING, qty: BIGINT>>, order_id: BIGINT, shipping: STRUCT<address: STRING, method: STRING>, tags: ARRAY<STRING>>') as parsed_value
# MAGIC from
# MAGIC     dz.dz.data_json_bronze_decoded

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     decoded_value,
# MAGIC     from_json(decoded_value, 'STRUCT<customer: STRUCT<id: BIGINT, name: STRING, vip: BOOLEAN>, discount: DOUBLE, items: ARRAY<STRUCT<price: BIGINT, product: STRING, qty: BIGINT>>, order_id: BIGINT, shipping: STRUCT<address: STRING, method: STRING>, tags: ARRAY<STRING>>') as parsed_value
# MAGIC from
# MAGIC     dz.dz.data_json_bronze_decoded
# MAGIC where
# MAGIC     from_json(decoded_value, 'STRUCT<customer: STRUCT<id: BIGINT, name: STRING, vip: BOOLEAN>, discount: DOUBLE, items: ARRAY<STRUCT<price: BIGINT, product: STRING, qty: BIGINT>>, order_id: BIGINT, shipping: STRUCT<address: STRING, method: STRING>, tags: ARRAY<STRING>>') is null
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     decoded_value,
# MAGIC     from_json(decoded_value, 'STRUCT<customer: STRUCT<id: BIGINT, name: STRING, vip: BOOLEAN>, discount: DOUBLE, items: ARRAY<STRUCT<price: BIGINT, product: STRING, qty: BIGINT>>, order_id: BIGINT, shipping: STRUCT<address: STRING, method: STRING>, tags: ARRAY<STRING>>') as parsed_value
# MAGIC from
# MAGIC     dz.dz.data_json_bronze_decoded;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists dz.dz.data_json_bronze_decoded_struct ;
# MAGIC create or replace table dz.dz.data_json_bronze_decoded_struct 
# MAGIC as 
# MAGIC select
# MAGIC     * except (decoded_value),
# MAGIC     from_json(decoded_value, 'STRUCT<customer: STRUCT<id: BIGINT, name: STRING, vip: BOOLEAN>, discount: DOUBLE, items: ARRAY<STRUCT<price: BIGINT, product: STRING, qty: BIGINT>>, order_id: BIGINT, shipping: STRUCT<address: STRING, method: STRING>, tags: ARRAY<STRING>>') as parsed_value
# MAGIC from
# MAGIC     dz.dz.data_json_bronze_decoded;
# MAGIC
# MAGIC

# COMMAND ----------

spark.table("dz.dz.data_json_bronze_decoded_struct").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC parsed_value.customer.name,
# MAGIC parsed_value.tags,
# MAGIC array_size(parsed_value.tags) tags_size
# MAGIC from dz.dz.data_json_bronze_decoded_struct

# COMMAND ----------

_sqldf.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC parsed_value.customer.name,
# MAGIC explode(parsed_value.tags),
# MAGIC array_size(parsed_value.tags) tags_size
# MAGIC from dz.dz.data_json_bronze_decoded_struct

# COMMAND ----------

_sqldf.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC parsed_value.customer.name,
# MAGIC explode_outer(parsed_value.tags),
# MAGIC array_size(parsed_value.tags) tags_size
# MAGIC from dz.dz.data_json_bronze_decoded_struct

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dz.dz.data_json_variant 
# MAGIC as
# MAGIC select
# MAGIC     * except (decoded_value),
# MAGIC     parse_json(decoded_value) value_variant
# MAGIC from
# MAGIC     dz.dz.data_json_bronze_decoded;
# MAGIC
# MAGIC select * from dz.dz.data_json_variant;
# MAGIC