# Databricks notebook source
# MAGIC %md
# MAGIC # Loading the Fact tables in the Gold layer

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, year, month, dayofmonth, coalesce, expr, col

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

silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

# Set current catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {gold_schema}")

# COMMAND ----------

# Define load_date
load_date = current_timestamp()

# COMMAND ----------

# Create _tmp_fact_sales temp view
tmp_fact_sales = (
    spark.table("silver.sales_order_detail").alias("sod")
    .join(
        spark.table("silver.sales_order_header").alias("soh"),
        (col("sod.sales_order_id") == col("soh.sales_order_id")) & col("soh._tf_valid_to").isNull(),
        how="left_outer"
    )
    .join(
        spark.table("silver.customer").alias("c"),
        (col("soh.customer_id") == col("c.customer_id")) & col("c._tf_valid_to").isNull(),
        how="left_outer"
    )
    .join(
        spark.table("gold.dim_customer").alias("cust"),
        col("c.customer_id") == col("cust.cust_customer_id"),
        how="left_outer"
    )
    .join(
        spark.table("silver.address").alias("a"),
        (col("soh.bill_to_address_id") == col("a.address_id")) & col("a._tf_valid_to").isNull(),
        how="left_outer"
    )
    .join(
        spark.table("gold.dim_geography").alias("geo"),
        col("a.address_id") == col("geo.geo_address_id"),
        how="left_outer"
    )
    .filter(col("sod._tf_valid_to").isNull())
    .select(
        col("soh.sales_order_id").cast("int").alias("sales_order_id"),
        col("sod.sales_order_detail_id").cast("int").alias("sales_order_detail_id"),
        (10000 * year("soh.order_date") + 100 * month("soh.order_date") + dayofmonth("soh.order_date")).alias("_tf_dim_calendar_id"),
        coalesce(col("cust._tf_dim_customer_id"), expr("-9")).alias("_tf_dim_customer_id"),
        coalesce(col("geo._tf_dim_geography_id"), expr("-9")).alias("_tf_dim_geography_id"),
        coalesce(col("sod.order_qty").cast("smallint"), expr("0")).alias("sales_order_qty"),
        coalesce(col("sod.unit_price").cast("decimal(19,4)"), expr("0")).alias("sales_unit_price"),
        coalesce(col("sod.unit_price_discount").cast("decimal(19,4)"), expr("0")).alias("sales_unit_price_discount"),
        coalesce(col("sod.line_total").cast("decimal(38,6)"), expr("0")).alias("sales_line_total")
    )
)

tmp_fact_sales.createOrReplaceTempView("_tmp_fact_sales")

# COMMAND ----------

# Merge into fact_sales
spark.sql("""
MERGE INTO gold.fact_sales AS tgt
USING _tmp_fact_sales AS src
ON tgt.sales_order_detail_id = src.sales_order_detail_id AND tgt.sales_order_id = src.sales_order_id
WHEN MATCHED AND (
    tgt._tf_dim_calendar_id != src._tf_dim_calendar_id OR
    tgt._tf_dim_customer_id != src._tf_dim_customer_id OR
    tgt._tf_dim_geography_id != src._tf_dim_geography_id OR
    tgt.sales_order_qty != src.sales_order_qty OR
    tgt.sales_unit_price != src.sales_unit_price OR
    tgt.sales_unit_price_discount != src.sales_unit_price_discount OR
    tgt.sales_line_total != src.sales_line_total
) THEN
  UPDATE SET
    _tf_dim_calendar_id = src._tf_dim_calendar_id,
    _tf_dim_customer_id = src._tf_dim_customer_id,
    _tf_dim_geography_id = src._tf_dim_geography_id,
    tgt.sales_order_qty = src.sales_order_qty,
    tgt.sales_unit_price = src.sales_unit_price,
    tgt.sales_unit_price_discount = src.sales_unit_price_discount,
    tgt.sales_line_total = src.sales_line_total,
    tgt._tf_update_date = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    sales_order_id,
    sales_order_detail_id,
    _tf_dim_calendar_id,
    _tf_dim_customer_id,
    _tf_dim_geography_id,
    sales_order_qty,
    sales_unit_price,
    sales_unit_price_discount,
    sales_line_total,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.sales_order_id,
    src.sales_order_detail_id,
    src._tf_dim_calendar_id,
    src._tf_dim_customer_id,
    src._tf_dim_geography_id,
    src.sales_order_qty,
    src.sales_unit_price,
    src.sales_unit_price_discount,
    src.sales_line_total,
    current_timestamp(),
    current_timestamp()
  )
""")