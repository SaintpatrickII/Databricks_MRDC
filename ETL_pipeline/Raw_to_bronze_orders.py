# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

host = dbutils.secrets.get(scope = "mrdc", key = "host")
user = dbutils.secrets.get(scope = "mrdc", key = "user")
password = dbutils.secrets.get(scope = "mrdc", key = "password")
database = dbutils.secrets.get(scope = "mrdc", key = "database")
port = dbutils.secrets.get(scope = "mrdc", key = "port")


# COMMAND ----------

# MAGIC %md
# MAGIC # Database connection

# COMMAND ----------

driver = "org.postgresql.Driver"

database_host = host
database_port = port
database_name = database
table = "orders_table"
user_name = user
password = password

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

remote_table = (spark.read
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", table)
  .option("user", user_name)
  .option("password", password)
  .load()
)
display(type(remote_table))


# COMMAND ----------

# Move table into global temp view
remote_table.createOrReplaceTempView("orders_temp_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note as a global temp view, without the global_temp. attribute this table won't be recognised
# MAGIC SELECT * FROM orders_temp_table
# MAGIC limit 10

# COMMAND ----------

# Note as a global temp view, without the global_temp. attribute this table won't be recognised
spark.sql("SELECT * FROM global_temp.orders_temp_table").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CTAS - remove PII & Misc Columns

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_raw
# MAGIC COMMENT "CONTAINS PII"
# MAGIC AS SELECT * EXCEPT(rt.level_0,rt.first_name, rt.last_name, rt.`1`) FROM orders_temp_table rt
# MAGIC
# MAGIC -- CTAS Statement from view, removing PII

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_raw

# COMMAND ----------

# read in temp view & assert datatypes
orders_df = spark.read.table("orders_temp_table")

orders_df_schema = orders_df.printSchema()
print(type(orders_df))


# COMMAND ----------

print(type(orders_df))
orders_df = orders_df.withColumn("card_number", orders_df["card_number"].cast(IntegerType()))
# orders_df = orders_df.withColumn("card_number", len(orders_df["card_number"]) < 20)
orders_df = orders_df.filter(F.length("card_number") < 20)
# orders_df = orders_df.printSchema()
orders_df.show()
