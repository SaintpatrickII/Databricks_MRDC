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
display(remote_table)
