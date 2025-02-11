# Databricks notebook source
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
remote_table.createOrReplaceGlobalTempView("remote_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note as a gloval temp view, without the global_temp. attribute this table won't be recognised
# MAGIC SELECT * FROM remote_table
# MAGIC limit 10

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.remote_table").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CTAS - remove PII & Misc Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_raw
# MAGIC COMMENT "CONTAINS PII"
# MAGIC AS SELECT * EXCEPT(rt.level_0,rt.first_name, rt.last_name, rt.`1`) FROM global_temp.remote_table rt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_raw
