# Databricks notebook source
# MAGIC %md
# MAGIC # Code to test the SCD2 in Silver

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

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

dbutils.widgets.text("my_catalog", default_catalog, "My catalog")
catalog = dbutils.widgets.get("my_catalog")

bronze_schema = "bronze"
silver_schema = "silver"

# COMMAND ----------

# Set current catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {bronze_schema}")

# COMMAND ----------

# Simulate an UPDATE in source
spark.sql("SELECT * FROM address WHERE City = 'Bothell'").show()
spark.sql("""
UPDATE address
SET PostalCode = '12345', ModifiedDate = current_timestamp()
WHERE City = 'Bothell'
""")

# COMMAND ----------

# Simulate a DELETE in source
spark.sql("SELECT * FROM address WHERE City = 'Surrey'").show()
spark.sql("DELETE FROM address WHERE City = 'Surrey'")

# COMMAND ----------

# Simulate an INSERT in source
spark.sql("SELECT * FROM bronze.Address ORDER BY AddressID DESC").show()

# COMMAND ----------

# Simulate INSERT+DELETE via PK modification
spark.sql("""
UPDATE bronze.Address
SET AddressID = 11383
WHERE AddressID = 1105
""")

# COMMAND ----------

# Run ETL externally before continuing this test

# COMMAND ----------

# Check results in Silver after ETL execution
spark.sql(f"USE SCHEMA {silver_schema}")
spark.sql("SELECT * FROM address WHERE city = 'Bothell' ORDER BY address_id, _tf_valid_from").show()
spark.sql("SELECT * FROM address WHERE city = 'Surrey' ORDER BY address_id, _tf_valid_from").show()
spark.sql("SELECT * FROM address WHERE address_id IN (11383, 1105)").show()