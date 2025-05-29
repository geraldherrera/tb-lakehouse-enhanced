# Databricks notebook source
# MAGIC %md
# MAGIC # ETL - Silver layer

# COMMAND ----------

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
unity_catalogs = [c for c in catalogs if c != "hive_metastore"]

if len(unity_catalogs) == 1:
    default_catalog = unity_catalogs[0]
else:

    default_catalog = next((c for c in unity_catalogs if c.startswith("dbw_")), "hive_metastore")

dbutils.widgets.text("my_catalog", default_catalog, "Detected Catalog")
catalog = dbutils.widgets.get("my_catalog")
    
dbutils.widgets.text("my_schema", "silver", "Silver Schema")

silver_schema = dbutils.widgets.get("my_schema")
bronze_schema = "bronze"
logs_schema = "logs"
log_table = "silver_processing_log"


# COMMAND ----------

# MAGIC %md
# MAGIC Retrieval of all existing Bronze tables

# COMMAND ----------

bronze_tables = [
    row.tableName for row in spark.sql(f"SHOW TABLES IN {catalog}.{bronze_schema}").collect()
    if row.tableName.startswith("bronze_")
]

# COMMAND ----------

# MAGIC %md
# MAGIC Log function

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

def log_silver_processing_result(table_name, status, rows_inserted=None, message=None):
    schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("rows_inserted", IntegerType(), True),
        StructField("message", StringType(), True)
    ])

    data = [{
        "table_name": table_name,
        "timestamp": datetime.now(),
        "status": status,
        "rows_inserted": int(rows_inserted) if rows_inserted is not None else None,
        "message": message[:5000] if message else None
    }]

    df_log = spark.createDataFrame(data, schema=schema)
    df_log.write.mode("append").format("delta").saveAsTable(f"{catalog}.{logs_schema}.{log_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC SCD2 function

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit, col, row_number, coalesce
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def process_scd2(table_bronze):
    try:
        # Construction of Silver and Bronze table names
        table_suffix = table_bronze.replace("bronze_", "")
        table_silver = f"silver_{table_suffix}"

        full_bronze = f"{catalog}.{bronze_schema}.{table_bronze}"
        full_silver = f"{catalog}.{silver_schema}.{table_silver}"

        bronze_df = spark.table(full_bronze)

        colonnes_techniques = ["ingestion_timestamp", "valid_from", "valid_to", "is_current", "hash"]
        columns_to_hash = [c for c in bronze_df.columns if c not in colonnes_techniques]

        primary_keys = [c for c in columns_to_hash if c.lower().endswith("id") and c.lower() != "rowguid"]
        if not primary_keys:
            message = f"No primary key detected for {table_bronze}. Table ignored."
            log_silver_processing_result(table_silver, "KO", message=message)
            return

        # Hash calculation based only on business columns
        bronze_hashed = bronze_df.withColumn("hash", sha2(concat_ws("||", *columns_to_hash), 256))

        # Initialization of Silver tables if they don't exist
        if not spark._jsparkSession.catalog().tableExists(full_silver):
            silver_initial = bronze_hashed \
                .withColumn("valid_from", current_timestamp()) \
                .withColumn("valid_to", lit(None).cast("timestamp")) \
                .withColumn("is_current", lit(True))

            silver_initial.write.format("delta").saveAsTable(full_silver)
            log_silver_processing_result(
                table_silver, "OK",
                rows_inserted=silver_initial.count(),
                message="Silver table initialized"
            )
            return

        silver_df = spark.table(full_silver).filter("is_current = true")

        # Creating a Cipher key for matching
        for pk in primary_keys:
            bronze_hashed = bronze_hashed.withColumn(f"_pk_{pk}", coalesce(col(pk).cast("string"), lit("__NULL__")))
            silver_df = silver_df.withColumn(f"_pk_{pk}", coalesce(col(pk).cast("string"), lit("__NULL__")))

        join_condition = [col(f"src._pk_{pk}") == col(f"tgt._pk_{pk}") for pk in primary_keys]
        joined_df = bronze_hashed.alias("src").join(
            silver_df.alias("tgt"),
            on=join_condition,
            how="left"
        )

        changes_df = joined_df.filter("tgt.hash IS NULL OR src.hash != tgt.hash") \
                              .select("src.*")

        if changes_df.isEmpty():
            log_silver_processing_result(
                table_silver, "OK",
                rows_inserted=0,
                message="No change detected"
            )
            return

        # Security: keep only one version per key combination
        window_spec = Window.partitionBy(*primary_keys).orderBy("hash")
        changes_df = changes_df.withColumn("row_num", row_number().over(window_spec)) \
                               .filter("row_num = 1") \
                               .drop("row_num")

        insert_count = changes_df.count()

        silver_delta = DeltaTable.forName(spark, full_silver)

        merge_condition = " AND ".join([f"tgt.{pk} = src.{pk}" for pk in primary_keys])

        silver_delta.alias("tgt").merge(
            source=changes_df.alias("src"),
            condition=merge_condition
        ).whenMatchedUpdate(
            condition="tgt.is_current = true",
            set={
                "valid_to": current_timestamp(),
                "is_current": lit(False)
            }
        ).execute()

        changes_to_insert = changes_df \
            .withColumn("valid_from", current_timestamp()) \
            .withColumn("valid_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True))

        cols_to_keep = [c for c in changes_to_insert.columns if not c.startswith("_pk_")]
        changes_to_insert = changes_to_insert.select(*cols_to_keep)

        changes_to_insert.write.format("delta").mode("append").saveAsTable(full_silver)

        log_silver_processing_result(
            table_silver, "OK",
            rows_inserted=insert_count,
            message="SCD2 applied"
        )

    except Exception as e:
        table_suffix = table_bronze.replace("bronze_", "")
        table_silver = f"silver_{table_suffix}"
        log_silver_processing_result(table_silver, "KO", message=str(e))

for table in bronze_tables:
    process_scd2(table)