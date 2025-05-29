# Databricks notebook source
# MAGIC %md
# MAGIC # Initialization the pipeline

# COMMAND ----------

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
unity_catalogs = [c for c in catalogs if c != "hive_metastore"]

if len(unity_catalogs) == 1:
    default_catalog = unity_catalogs[0]
else:
    # Select the first catalog starting with "dbw_"
    default_catalog = next((c for c in unity_catalogs if c.startswith("dbw_")), "hive_metastore")

# Set the default catalog
dbutils.widgets.text("my_catalog", default_catalog, "Detected Catalog")
catalog = dbutils.widgets.get("my_catalog")


# COMMAND ----------

# MAGIC %md
# MAGIC Detection and selection of the catalog

# COMMAND ----------

bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"
logs_schema = "logs"


# COMMAND ----------

# MAGIC %md
# MAGIC Function to create a schema if it doesn't exist in the catalog

# COMMAND ----------

def create_schema_if_not_exists(schema_name):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
    print(f"Schema {catalog}.{schema_name} ready.")


# COMMAND ----------

# MAGIC %md
# MAGIC Création des schémas Bronze, Silver, Gold et Logs

# COMMAND ----------

create_schema_if_not_exists(bronze_schema)
create_schema_if_not_exists(silver_schema)
create_schema_if_not_exists(gold_schema)
create_schema_if_not_exists(logs_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC Generic function to create a log table if it doesn't exist

# COMMAND ----------

def create_log_table_if_needed(table_name, schema):
    full_table_name = f"{catalog}.{logs_schema}.{table_name}"
    if not spark._jsparkSession.catalog().tableExists(full_table_name):
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(full_table_name)
        print(f"Table {full_table_name} created.")
    else:
        print(f"Table {full_table_name} already exists.")


# COMMAND ----------

# MAGIC %md
# MAGIC Log schemas used for each layer

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

bronze_log_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("message", StringType(), True)
])

silver_log_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("rows_inserted", IntegerType(), True),
    StructField("message", StringType(), True)
])

gold_log_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("rows_inserted", IntegerType(), True),
    StructField("message", StringType(), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC Creation of log tables for Bronze, Silver, and Gold

# COMMAND ----------

create_log_table_if_needed("bronze_processing_log", bronze_log_schema)
create_log_table_if_needed("silver_processing_log", silver_log_schema)
create_log_table_if_needed("gold_processing_log", gold_log_schema)