# Databricks notebook source
# MAGIC %md
# MAGIC # Test bronze layer : Ingestion error detection

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

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
unity_catalogs = [c for c in catalogs if c != "hive_metastore"]

if len(unity_catalogs) == 1:
    default_catalog = unity_catalogs[0]
else:
    default_catalog = next((c for c in unity_catalogs if c.startswith("dbw_")), "hive_metastore")

dbutils.widgets.text("my_catalog", default_catalog, "Detected Catalog")
catalog = dbutils.widgets.get("my_catalog")

bronze_schema = "bronze"


# COMMAND ----------

# MAGIC %md
# MAGIC Retrieves all columns from a table in the source database.

# COMMAND ----------

from pyspark.sql.functions import col

def get_columns_for_table(table_name: str, schema: str = "SalesLT") -> list:
    cols_df = spark.read.jdbc(
        url=jdbc_url,
        table="INFORMATION_SCHEMA.COLUMNS",
        properties=connection_properties
    ).filter(
        (col("TABLE_SCHEMA") == schema) &
        (col("TABLE_NAME") == table_name)
    ).orderBy("ORDINAL_POSITION")

    return [row["COLUMN_NAME"] for row in cols_df.collect()]


# COMMAND ----------

# MAGIC %md
# MAGIC Function that converts CamelCase names to snake_case for table and column names in the Bronze layer

# COMMAND ----------

import re

def to_snake_case(name: str) -> str:
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


# COMMAND ----------

# MAGIC %md
# MAGIC Retrieves the columns that make up the primary key of a SQL Server table.

# COMMAND ----------

def get_primary_keys(table_name: str, schema: str = "SalesLT") -> list:
    key_usage_df = spark.read.jdbc(
        url=jdbc_url,
        table="INFORMATION_SCHEMA.KEY_COLUMN_USAGE",
        properties=connection_properties
    ).filter(
        (col("TABLE_SCHEMA") == schema) &
        (col("TABLE_NAME") == table_name)
    )

    constraints_df = spark.read.jdbc(
        url=jdbc_url,
        table="INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
        properties=connection_properties
    ).filter(
        (col("TABLE_SCHEMA") == schema) &
        (col("TABLE_NAME") == table_name) &
        (col("CONSTRAINT_TYPE") == "PRIMARY KEY")
    )

    primary_keys_df = key_usage_df.join(
        constraints_df,
        on="CONSTRAINT_NAME",
        how="inner"
    ).orderBy("ORDINAL_POSITION")

    return [row["COLUMN_NAME"] for row in primary_keys_df.collect()]


# COMMAND ----------

# MAGIC %md
# MAGIC Function to retrieve table names

# COMMAND ----------

source_tables_df = spark.read.jdbc(
    url=jdbc_url,
    table="INFORMATION_SCHEMA.TABLES",
    properties=connection_properties
).filter("TABLE_SCHEMA = 'SalesLT' AND TABLE_TYPE = 'BASE TABLE'")

source_table_names = [row["TABLE_NAME"] for row in source_tables_df.collect()]


# COMMAND ----------

# MAGIC %md
# MAGIC Detection of tables present in the Bronze layer

# COMMAND ----------

bronze_tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{bronze_schema}")
bronze_table_names = [row["tableName"] for row in bronze_tables_df.collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC Building a table for testing purposes

# COMMAND ----------

tables_to_test = []

for table_name in source_table_names:
    table_snake = to_snake_case(table_name)
    bronze_table_name = f"bronze_saleslt_{table_snake}"

    if bronze_table_name in bronze_table_names:
        primary_keys = get_primary_keys(table_name)
        if not primary_keys:
            print(f"No primary key detected for {table_name}, table skipped")
            continue

        columns = get_columns_for_table(table_name)

        tables_to_test.append({
            "source": f"SalesLT.{table_name}",
            "bronze": bronze_table_name,
            "primary_keys_source": primary_keys,
            "primary_keys_bronze": [to_snake_case(pk) for pk in primary_keys],
            "columns_source": columns,
            "columns_bronze": [to_snake_case(c) for c in columns]
        })
    else:
        print(f"Table not found in the Bronze layer : {bronze_table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC Test function to compare source and Bronze on count and random sample values

# COMMAND ----------

import random
from pyspark.sql.functions import col, trim, regexp_replace
from functools import reduce

def test_table_sample(source_table, bronze_table, primary_keys_source, primary_keys_bronze, columns_source, columns_bronze):
    try:
        print(f"\nTesting table : {source_table} ➜ {bronze_table}")

        count_source_total = spark.read.jdbc(
            url=jdbc_url,
            table=source_table,
            properties=connection_properties
        ).count()

        count_bronze_total = spark.read.table(f"{catalog}.{bronze_schema}.{bronze_table}").count()

        source_ids_df = spark.read.jdbc(
            url=jdbc_url,
            table=source_table,
            properties=connection_properties
        ).select(*primary_keys_source).distinct()

        all_rows = source_ids_df.collect()
        if not all_rows:
            print(f"Table : {source_table}\n- No data available in the source.\nStatut : Test ignored\n")
            return

        sample_rows = random.sample(all_rows, min(25, len(all_rows)))

        def format_condition(row):
            return "(" + " AND ".join([f"{k} = {repr(row[k])}" for k in primary_keys_source]) + ")"

        where_clause = " OR ".join([format_condition(r) for r in sample_rows])
        query = f"(SELECT * FROM {source_table} WHERE {where_clause}) AS src_sample"

        source_sample = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

        for field in source_sample.schema.fields:
            if field.dataType.simpleString() == "string":
                source_sample = source_sample.withColumn(
                    field.name,
                    regexp_replace(trim(col(field.name)), "[\\u00A0\\r\\n]", "")
                )

        bronze_df = spark.read.table(f"{catalog}.{bronze_schema}.{bronze_table}")
        bronze_sample = bronze_df

        for i, bronze_key in enumerate(primary_keys_bronze):
            sample_values = [r[primary_keys_source[i]] for r in sample_rows]
            bronze_sample = bronze_sample.filter(col(bronze_key).isin(sample_values))

        join_expr = reduce(lambda a, b: a & b, [
            col(f"src.{primary_keys_source[i]}") == col(f"brz.{primary_keys_bronze[i]}")
            for i in range(len(primary_keys_source))
        ])

        joined_df = source_sample.alias("src").join(
            bronze_sample.alias("brz"),
            on=join_expr,
            how="inner"
        )

        mismatches = []
        for i, source_col in enumerate(columns_source):
            bronze_col = columns_bronze[i]
            if bronze_col in bronze_sample.columns:
                diff_df = joined_df.filter(col(f"src.{source_col}") != col(f"brz.{bronze_col}"))
                count_diff = diff_df.count()
                if count_diff > 0:
                    mismatches.append((source_col, count_diff))
                    print(f"Divergence detected on column : {source_col} ({count_diff} ligne(s))")

                    diff_df.select(
                        *[col(f"src.{k}").alias(f"{k}_source") for k in primary_keys_source],
                        col(f"src.{source_col}").alias(f"{source_col}_source"),
                        col(f"brz.{bronze_col}").alias(f"{bronze_col}_bronze")
                    ).show(5, truncate=False)

        print(f"\nResume : {source_table}")
        print(f"- Total rows source : {count_source_total}")
        print(f"- Total rows Bronze : {count_bronze_total}")
        print(f"- Tested column : {len(columns_source)}")
        print(f"- Divergent columns : {len(mismatches)}")


        assert count_source_total == count_bronze_total, (
            f"Error : Volume discrepancy between source ({count_source_total}) and Bronze ({count_bronze_total}) for {source_table}"
        )

        assert len(mismatches) == 0, (
            f"Error : {len(mismatches)} Divergent column(s) detected in {source_table} : " + ", ".join(col for col, _ in mismatches)
        )

        print("Status : test successfully passed\n")

    except Exception as e:
        print(f"Table : {source_table}\n- Error whille testing : {str(e)}\nStatut : Failed\n")
        raise e


# COMMAND ----------

# MAGIC %md
# MAGIC Exécution du test pour toutes les tables

# COMMAND ----------

for t in tables_to_test:
    try:
        test_table_sample(
            source_table=t["source"],
            bronze_table=t["bronze"],
            primary_keys_source=t["primary_keys_source"],
            primary_keys_bronze=t["primary_keys_bronze"],
            columns_source=t["columns_source"],
            columns_bronze=t["columns_bronze"]
        )
    except Exception as e:
        print(f"Error while testing {t['source']} : {str(e)}")