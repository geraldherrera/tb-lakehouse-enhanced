# Databricks notebook source
# MAGIC %md
# MAGIC # ETL - Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC JDBC connection configuration

# COMMAND ----------

jdbc_hostname = "sql-datasource-dev-ghe.database.windows.net"
jdbc_port = 1433
jdbc_database = "sqldb-adventureworks-dev-ghe"
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database}"

username = dbutils.secrets.get(scope="kv-jdbc", key="sql-username")
password = dbutils.secrets.get(scope="kv-jdbc", key="sql-password")

connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic detection of the first catalog starting by dbw_

# COMMAND ----------

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
unity_catalogs = [c for c in catalogs if c != "hive_metastore"]

if len(unity_catalogs) == 1:
    default_catalog = unity_catalogs[0]
else:
    default_catalog = next((c for c in unity_catalogs if c.startswith("dbw_")), "hive_metastore")

dbutils.widgets.text("my_catalog", default_catalog, "Catalog detected")
catalog = dbutils.widgets.get("my_catalog")

bronze_schema = "bronze"
logs_schema = "logs"
log_table = "bronze_processing_log"

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic retrieval of tables to ingest

# COMMAND ----------

tables_df = spark.read.jdbc(
    url=jdbc_url,
    table="INFORMATION_SCHEMA.TABLES",
    properties=connection_properties
)

tables_to_ingest = (
    tables_df
    .filter("TABLE_SCHEMA = 'SalesLT'")
    .filter("TABLE_TYPE = 'BASE TABLE'")
    .filter(~tables_df["TABLE_NAME"].isin(["ErrorLog", "BuildVersion"]))
    .select("TABLE_NAME")
    .rdd.flatMap(lambda x: x)
    .collect()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Function to log ingestions

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row

def log_ingestion_result(table_name, status, message):
    full_log_table = f"{catalog}.{logs_schema}.{log_table}"
    log_row = Row(
        table_name=table_name,
        timestamp=datetime.now(),
        status=status,
        message=message[:5000]
    )
    spark.createDataFrame([log_row]) \
        .write.mode("append") \
        .format("delta") \
        .saveAsTable(full_log_table)


# COMMAND ----------

# MAGIC %md
# MAGIC Ingestion function for the Bronze layer

# COMMAND ----------

from pyspark.sql.functions import trim, col, current_timestamp, regexp_replace
import re

# Converts a CamelCase name to snake_case. Example: ProductModelDescription → product_model_description
def to_snake_case(name: str) -> str:
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def ingest_table(table_name, source_schema="SalesLT"):
    try:

        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"{source_schema}.{table_name}",
            properties=connection_properties
        )

        # Renaming columns to snake_case
        for field in df.schema.fields:
            new_name = to_snake_case(field.name)
            if field.name != new_name:
                df = df.withColumnRenamed(field.name, new_name)

        # Cleaning string-type columns
        for field in df.schema.fields:
            if field.dataType.simpleString() == 'string':
                clean_col = trim(col(field.name))
                clean_col = regexp_replace(clean_col, "[\\u00A0\\r\\n]", "")
                df = df.withColumn(field.name, clean_col)

        # Adding the ingestion_timestamp column
        df = df.withColumn("ingestion_timestamp", current_timestamp())

        # Converting table name to snake_case
        schema_snake = source_schema.lower()
        table_snake = to_snake_case(table_name)
        bronze_table_name = f"bronze_{schema_snake}_{table_snake}"
        full_table_name = f"{catalog}.{bronze_schema}.{bronze_table_name}"

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_name)

        log_ingestion_result(bronze_table_name, "OK", "Successful ingestion")

    except Exception as e:
        log_ingestion_result(table_name, "KO", str(e))

for table in tables_to_ingest:
    ingest_table(table)