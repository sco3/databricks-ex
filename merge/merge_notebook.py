# Databricks notebook source


source_df = spark.read.csv("/Volumes/dz/dz/dz-vol-merge/source_table.csv", header=True)

source_df.write.saveAsTable("dz.dz.source_table", mode="overwrite")

spark.read.table("dz.dz.source_table").display()


# COMMAND ----------



target_df = spark.read.csv("/Volumes/dz/dz/dz-vol-merge/target_table.csv", header=True)
target_df.write.saveAsTable("dz.dz.target_table", mode="overwrite")

spark.read.table("dz.dz.target_table").display()


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dz.dz.target_table target_table
# MAGIC USING dz.dz.source_table source_table
# MAGIC ON source_table.id = target_table.id
# MAGIC WHEN MATCHED AND source_table.status = 'update' THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN MATCHED AND source_table.status = 'delete' THEN
# MAGIC   DELETE
# MAGIC WHEN NOT MATCHED AND source_table.status = 'new' THEN
# MAGIC   INSERT (id, name, value) VALUES (source_table.id, source_table.name, source_table.value)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dz.dz.target_table