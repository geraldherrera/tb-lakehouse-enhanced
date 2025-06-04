# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion in the Bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the JDBC connection to the Azure SQL Database (Source)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("loading bronze layer").getOrCreate()
jdbcHostname = "sql-datasource-dev-ghe.database.windows.net"
jdbcDatabase = "sqldb-adventureworks-dev-ghe"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

username = dbutils.secrets.get(scope="kv-jdbc", key="sql-username")
password = dbutils.secrets.get(scope="kv-jdbc", key="sql-password")

connectionProperties = {
    "user" : username,
    "password" : password,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog detection (Unity Catalog)

# COMMAND ----------

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
unity_catalogs = [c for c in catalogs if c != "hive_metastore"]

if len(unity_catalogs) == 1:
    default_catalog = unity_catalogs[0]
else:
    default_catalog = next((c for c in unity_catalogs if c.startswith("dbw_")), "hive_metastore")

dbutils.widgets.text("my_catalog", default_catalog, "My Catalog")
catalog = dbutils.widgets.get("my_catalog")

bronze_schema = "bronze"

# COMMAND ----------

# Set current catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {bronze_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of SalesOrderDetail

# COMMAND ----------

SalesOrderDetail = spark.read.jdbc(url=jdbcUrl, table="SalesLT.SalesOrderDetail", properties=connectionProperties)
display(SalesOrderDetail)

# COMMAND ----------

SalesOrderDetail.write.mode("overwrite").saveAsTable("bronze.SalesOrderDetail")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of SalesOrderHeader

# COMMAND ----------

SalesOrderHeader = spark.read.jdbc(url=jdbcUrl, table="SalesLT.SalesOrderHeader", properties=connectionProperties)
display(SalesOrderHeader)

SalesOrderHeader.write.mode("overwrite").saveAsTable("bronze.SalesOrderHeader")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of Product

# COMMAND ----------

Product = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Product", properties=connectionProperties)
display(Product)

Product.write.mode("overwrite").saveAsTable("bronze.Product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of ProductCategory

# COMMAND ----------

ProductCategory = spark.read.jdbc(url=jdbcUrl, table="SalesLT.ProductCategory", properties=connectionProperties)
display(ProductCategory)

ProductCategory.write.mode("overwrite").saveAsTable("bronze.ProductCategory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of Address
# MAGIC
# MAGIC

# COMMAND ----------

Address = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Address", properties=connectionProperties)
display(Address)

Address.write.mode("overwrite").saveAsTable("bronze.Address")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion of Customer

# COMMAND ----------

Customer = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Customer", properties=connectionProperties)
display(Customer)

Customer.write.mode("overwrite").saveAsTable("bronze.Customer")