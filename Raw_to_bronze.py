# Databricks notebook source
host = dbutils.secrets.get(scope = "mrdc", key = "host")
user = dbutils.secrets.get(scope = "mrdc", key = "user")
password = dbutils.secrets.get(scope = "mrdc", key = "password")
database = dbutils.secrets.get(scope = "mrdc", key = "database")
port = dbutils.secrets.get(scope = "mrdc", key = "port")


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
# MAGIC
# MAGIC SELECT * FROM remote_table
# MAGIC limit 10

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.remote_table").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE orders_raw
# MAGIC AS SELECT * FROM global_temp.remote_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_raw
