# Databricks notebook source
# MAGIC %md
# MAGIC # Test SCD2 : Verification of historization in the Silver layer after Bronze modifications

# COMMAND ----------

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
unity_catalogs = [c for c in catalogs if c != "hive_metastore"]

if len(unity_catalogs) == 1:
    default_catalog = unity_catalogs[0]
else:
    default_catalog = next((c for c in unity_catalogs if c.startswith("dbw_")), "hive_metastore")

dbutils.widgets.text("my_catalog", default_catalog, "Detected Catalog")
catalog = dbutils.widgets.get("my_catalog")
    
dbutils.widgets.text("my_schema", "silver", "Schema Silver")
silver_schema = dbutils.widgets.get("my_schema")
bronze_schema = "bronze"

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {bronze_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC Alter bronze layer data

# COMMAND ----------

modifications = [
    f"UPDATE {bronze_schema}.bronze_saleslt_address SET country_region = 'Kanada' WHERE country_region = 'Canada'",
    f"UPDATE {bronze_schema}.bronze_saleslt_customer SET title = 'Monsieur' WHERE title = 'Mr.'",
    f"UPDATE {bronze_schema}.bronze_saleslt_customer_address SET address_type = 'Second Office' WHERE address_type = 'Main Office'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product SET color = 'Noir' WHERE color = 'Black'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product_category SET name = 'Gants' WHERE name = 'Gloves'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product_description SET description = 'Description alteree' WHERE description = 'Chromoly steel.'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product_model SET name = 'Nouveau model' WHERE name = 'LL Road Frame'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product_model_product_description SET culture = 'eng' WHERE culture = 'en'",
    f"UPDATE {bronze_schema}.bronze_saleslt_sales_order_detail SET order_qty = 10 WHERE order_qty = 1",
    f"UPDATE {bronze_schema}.bronze_saleslt_sales_order_header SET purchase_order_number = 'PO1995219999' WHERE purchase_order_number = 'PO19952192051'"
]

for query in modifications:
    spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Run Silver Layer notebook

# COMMAND ----------

dbutils.notebook.run("3.0_ETL_Silver_Layer", 600)

# COMMAND ----------

spark.sql(f"USE SCHEMA {silver_schema}")

assertions = [
    ("silver_saleslt_address", "country_region = 'Kanada'"),
    ("silver_saleslt_customer", "title = 'Monsieur'"),
    ("silver_saleslt_customer_address", "address_type = 'Second Office'"),
    ("silver_saleslt_product", "color = 'Noir'"),
    ("silver_saleslt_product_category", "name = 'Gants'"),
    ("silver_saleslt_product_description", "description = 'Description alteree'"),
    ("silver_saleslt_product_model", "name = 'Nouveau model'"),
    ("silver_saleslt_product_model_product_description", "culture = 'eng'"),
    ("silver_saleslt_sales_order_detail", "order_qty = 10"),
    ("silver_saleslt_sales_order_header", "purchase_order_number = 'PO1995219999'")
]

for table, condition in assertions:
    full_table = f"{catalog}.{silver_schema}.{table}"
    df = spark.sql(f"SELECT * FROM {full_table} WHERE {condition} AND valid_to IS NULL")
    assert df.count() > 0, f"SCD2 failed in {table} : No active row with condition `{condition}`"
    print(f"SCD2 OK in {table} : {df.count()} Active row(s) found for`{condition}`")


# COMMAND ----------

# MAGIC %md
# MAGIC Restore Bronze layer data

# COMMAND ----------

spark.sql(f"USE SCHEMA {bronze_schema}")

restauration = [
    f"UPDATE {bronze_schema}.bronze_saleslt_address SET country_region = 'Canada' WHERE country_region = 'Kanada'",
    f"UPDATE {bronze_schema}.bronze_saleslt_customer SET title = 'Mr.' WHERE title = 'Monsieur'",
    f"UPDATE {bronze_schema}.bronze_saleslt_customer_address SET address_type = 'Main Office' WHERE address_type = 'Second Office'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product SET color = 'Black' WHERE color = 'Noir'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product_category SET name = 'Gloves' WHERE name = 'Gants'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product_description SET description = 'Chromoly steel.' WHERE description = 'Description alteree'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product_model SET name = 'LL Road Frame' WHERE name = 'Nouveau model'",
    f"UPDATE {bronze_schema}.bronze_saleslt_product_model_product_description SET culture = 'en' WHERE culture = 'eng'",
    f"UPDATE {bronze_schema}.bronze_saleslt_sales_order_detail SET order_qty = 1 WHERE order_qty = 10",
    f"UPDATE {bronze_schema}.bronze_saleslt_sales_order_header SET purchase_order_number = 'PO19952192051' WHERE purchase_order_number = 'PO1995219999'"
]

for query in restauration:
    spark.sql(query)

print("Restore done.")

# COMMAND ----------

# MAGIC %md
# MAGIC Run Silver Layer notebook

# COMMAND ----------

dbutils.notebook.run("3.0_etl_silver_layer", 600)