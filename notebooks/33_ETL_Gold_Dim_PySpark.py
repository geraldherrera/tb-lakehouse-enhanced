# Databricks notebook source
# MAGIC %md
# MAGIC # Loading the Dim tables in the Gold layer

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, coalesce

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

# Load dim_geography
src_geo = (
    spark.table("silver.address")
    .filter(col("_tf_valid_to").isNull())
    .selectExpr(
        "CAST(address_id AS INT) AS geo_address_id",
        "COALESCE(TRY_CAST(address_line1 AS STRING), 'N/A') AS geo_address_line_1",
        "COALESCE(TRY_CAST(address_line2 AS STRING), 'N/A') AS geo_address_line_2",
        "COALESCE(TRY_CAST(city AS STRING), 'N/A') AS geo_city",
        "COALESCE(TRY_CAST(state_province AS STRING), 'N/A') AS geo_state_province",
        "COALESCE(TRY_CAST(country_region AS STRING), 'N/A') AS geo_country_region",
        "COALESCE(TRY_CAST(postal_code AS STRING), 'N/A') AS geo_postal_code"
    )
)
src_geo.createOrReplaceTempView("src_geo")

# COMMAND ----------

spark.sql("""
MERGE INTO gold.dim_geography AS tgt
USING src_geo AS src
ON tgt.geo_address_id = src.geo_address_id
WHEN MATCHED AND (
    tgt.geo_address_line_1 != src.geo_address_line_1 OR
    tgt.geo_address_line_2 != src.geo_address_line_2 OR
    tgt.geo_city != src.geo_city OR
    tgt.geo_state_province != src.geo_state_province OR
    tgt.geo_country_region != src.geo_country_region OR
    tgt.geo_postal_code != src.geo_postal_code
) THEN
  UPDATE SET
    tgt.geo_address_line_1 = src.geo_address_line_1,
    tgt.geo_address_line_2 = src.geo_address_line_2,
    tgt.geo_city = src.geo_city,
    tgt.geo_state_province = src.geo_state_province,
    tgt.geo_country_region = src.geo_country_region,
    tgt.geo_postal_code = src.geo_postal_code,
    tgt._tf_update_date = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    geo_address_id,
    geo_address_line_1,
    geo_address_line_2,
    geo_city,
    geo_state_province,
    geo_country_region,
    geo_postal_code,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.geo_address_id,
    src.geo_address_line_1,
    src.geo_address_line_2,
    src.geo_city,
    src.geo_state_province,
    src.geo_country_region,
    src.geo_postal_code,
    current_timestamp(),
    current_timestamp()
  )
""")

# COMMAND ----------

# Load dim_customer
src_cust = (
    spark.table("silver.customer")
    .filter(col("_tf_valid_to").isNull())
    .selectExpr(
        "CAST(customer_id AS INT) AS cust_customer_id",
        "COALESCE(TRY_CAST(title AS STRING), 'N/A') AS cust_title",
        "COALESCE(TRY_CAST(first_name AS STRING), 'N/A') AS cust_first_name",
        "COALESCE(TRY_CAST(middle_name AS STRING), 'N/A') AS cust_middle_name",
        "COALESCE(TRY_CAST(last_name AS STRING), 'N/A') AS cust_last_name",
        "COALESCE(TRY_CAST(suffix AS STRING), 'N/A') AS cust_suffix",
        "COALESCE(TRY_CAST(company_name AS STRING), 'N/A') AS cust_company_name",
        "COALESCE(TRY_CAST(sales_person AS STRING), 'N/A') AS cust_sales_person",
        "COALESCE(TRY_CAST(email_address AS STRING), 'N/A') AS cust_email_address",
        "COALESCE(TRY_CAST(phone AS STRING), 'N/A') AS cust_phone"
    )
)
src_cust.createOrReplaceTempView("src_cust")

# COMMAND ----------

spark.sql("""
MERGE INTO gold.dim_customer AS tgt
USING src_cust AS src
ON tgt.cust_customer_id = src.cust_customer_id
WHEN MATCHED AND (
    tgt.cust_title != src.cust_title OR
    tgt.cust_first_name != src.cust_first_name OR
    tgt.cust_middle_name != src.cust_middle_name OR
    tgt.cust_last_name != src.cust_last_name OR
    tgt.cust_suffix != src.cust_suffix OR
    tgt.cust_company_name != src.cust_company_name OR
    tgt.cust_sales_person != src.cust_sales_person OR
    tgt.cust_email_address != src.cust_email_address OR
    tgt.cust_phone != src.cust_phone
) THEN
  UPDATE SET
    tgt.cust_title = src.cust_title,
    tgt.cust_first_name = src.cust_first_name,
    tgt.cust_middle_name = src.cust_middle_name,
    tgt.cust_last_name = src.cust_last_name,
    tgt.cust_suffix = src.cust_suffix,
    tgt.cust_company_name = src.cust_company_name,
    tgt.cust_sales_person = src.cust_sales_person,
    tgt.cust_email_address = src.cust_email_address,
    tgt.cust_phone = src.cust_phone,
    tgt._tf_update_date = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    cust_customer_id,
    cust_title,
    cust_first_name,
    cust_middle_name,
    cust_last_name,
    cust_suffix,
    cust_company_name,
    cust_sales_person,
    cust_email_address,
    cust_phone,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.cust_customer_id,
    src.cust_title,
    src.cust_first_name,
    src.cust_middle_name,
    src.cust_last_name,
    src.cust_suffix,
    src.cust_company_name,
    src.cust_sales_person,
    src.cust_email_address,
    src.cust_phone,
    current_timestamp(),
    current_timestamp()
  )
""")