# Databricks notebook source
# MAGIC %md
# MAGIC # Creating the lakehouse catalog
# MAGIC
# MAGIC ## WARNING !!! the following cell drops the existing lakehouse layers

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

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {catalog}.bronze CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {catalog}.silver CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {catalog}.gold CASCADE")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the Medallion Architecture

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.gold")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating tables in Silver layer
# MAGIC
# MAGIC We add for each table an incremental surrogate key together with technical fields columns. 

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.address (
# MAGIC     _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL, -- Incremental surrogate key
# MAGIC
# MAGIC     -- Source table columns
# MAGIC     address_id INT,
# MAGIC     address_line1 STRING,
# MAGIC     address_line2 STRING,
# MAGIC     city STRING,
# MAGIC     state_province STRING,
# MAGIC     country_region STRING,
# MAGIC     postal_code STRING,
# MAGIC     rowguid CHAR(36),
# MAGIC     modified_date TIMESTAMP,
# MAGIC
# MAGIC     -- Technical columns
# MAGIC     _tf_valid_from TIMESTAMP, -- Start of record validity
# MAGIC     _tf_valid_to TIMESTAMP, -- End of record validity (NULL indicates current record)
# MAGIC     _tf_create_date TIMESTAMP,
# MAGIC     _tf_update_date TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.customer (
# MAGIC     _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL, -- Incremental surrogate key
# MAGIC
# MAGIC     -- Source table columns
# MAGIC     customer_id INT,
# MAGIC     name_style BOOLEAN,
# MAGIC     title STRING,
# MAGIC     first_name STRING,
# MAGIC     middle_name STRING,
# MAGIC     last_name STRING,
# MAGIC     suffix STRING,
# MAGIC     company_name STRING,
# MAGIC     sales_person STRING,
# MAGIC     email_address STRING,
# MAGIC     phone STRING,
# MAGIC     password_hash STRING,
# MAGIC     password_salt STRING,
# MAGIC     rowguid CHAR(36),
# MAGIC     modified_date TIMESTAMP,
# MAGIC
# MAGIC     -- Technical columns
# MAGIC     _tf_valid_from TIMESTAMP, -- Start of record validity
# MAGIC     _tf_valid_to TIMESTAMP, -- End of record validity (NULL indicates current record)
# MAGIC     _tf_create_date TIMESTAMP,
# MAGIC     _tf_update_date TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.sales_order_header (
# MAGIC     _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL, -- Incremental surrogate key
# MAGIC
# MAGIC     -- Source table columns
# MAGIC     sales_order_id INT,
# MAGIC     revision_number SMALLINT,
# MAGIC     order_date TIMESTAMP,
# MAGIC     due_date TIMESTAMP,
# MAGIC     ship_date TIMESTAMP,
# MAGIC     status SMALLINT,
# MAGIC     online_order_flag BOOLEAN,
# MAGIC     sales_order_number STRING,
# MAGIC     purchase_order_number STRING,
# MAGIC     account_number STRING,
# MAGIC     customer_id INT,
# MAGIC     ship_to_address_id INT,
# MAGIC     bill_to_address_id INT,
# MAGIC     ship_method STRING,
# MAGIC     credit_card_approval_code STRING,
# MAGIC     sub_total DECIMAL(19,4),
# MAGIC     tax_amt DECIMAL(19,4),
# MAGIC     freight DECIMAL(19,4),
# MAGIC     total_due DECIMAL(19,4),
# MAGIC     comment STRING,
# MAGIC     rowguid CHAR(36),
# MAGIC     modified_date TIMESTAMP,
# MAGIC
# MAGIC     -- Technical columns
# MAGIC     _tf_valid_from TIMESTAMP, -- Start of record validity
# MAGIC     _tf_valid_to TIMESTAMP, -- End of record validity (NULL indicates current record)
# MAGIC     _tf_create_date TIMESTAMP,
# MAGIC     _tf_update_date TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.sales_order_detail (
# MAGIC     _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL, -- Incremental surrogate key
# MAGIC
# MAGIC     -- Source table columns
# MAGIC     sales_order_id INT,
# MAGIC     sales_order_detail_id INT,
# MAGIC     order_qty SMALLINT,
# MAGIC     product_id INT,
# MAGIC     unit_price DECIMAL(19,4),
# MAGIC     unit_price_discount DECIMAL(19,4),
# MAGIC     line_total DECIMAL(38,6),
# MAGIC     rowguid CHAR(36),
# MAGIC     modified_date TIMESTAMP,
# MAGIC
# MAGIC     -- Technical columns
# MAGIC     _tf_valid_from TIMESTAMP, -- Start of record validity
# MAGIC     _tf_valid_to TIMESTAMP, -- End of record validity (NULL indicates current record)
# MAGIC     _tf_create_date TIMESTAMP,
# MAGIC     _tf_update_date TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a managed table in our Unity Catalog
# MAGIC CREATE OR REPLACE TABLE gold.dim_calendar AS
# MAGIC -- CTE to simplify our SQL
# MAGIC WITH calendar_dates AS (
# MAGIC     SELECT
# MAGIC         explode(array_dates) AS calendar_date
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             SEQUENCE(
# MAGIC                 MAKE_DATE(2000, 01, 01), -- Start date
# MAGIC                 MAKE_DATE(2030, 01, 01), -- End date
# MAGIC                 INTERVAL 1 DAY           -- Incremental step
# MAGIC             ) AS array_dates
# MAGIC     )
# MAGIC )
# MAGIC -- The SQL transforming our main calendar_date into all the columns we will be requiring
# MAGIC SELECT
# MAGIC     -- Calendar table primary key
# MAGIC     10000 * YEAR(calendar_date) + 100 * MONTH(calendar_date) + DAY(calendar_date) AS _tf_dim_calendar_id, 
# MAGIC
# MAGIC     TO_DATE(calendar_date) AS cal_date,
# MAGIC     YEAR(calendar_date) AS cal_year,
# MAGIC     MONTH(calendar_date) AS cal_month,
# MAGIC     DAY(calendar_date) AS calendar_day_of_month,
# MAGIC     DATE_FORMAT(calendar_date, 'EEEE MMMM dd yyyy') AS cal_date_full,
# MAGIC     DATE_FORMAT(calendar_date, 'EEEE') AS cal_day_name,
# MAGIC     CASE
# MAGIC         WHEN DATE_ADD(calendar_date, (WEEKDAY(calendar_date) + 1) - 1) = calendar_date THEN TO_DATE(calendar_date)
# MAGIC         ELSE DATE_ADD(calendar_date, -(WEEKDAY(calendar_date)))
# MAGIC     END AS cal_week_start, -- Start of week date
# MAGIC     DATE_ADD(
# MAGIC         CASE
# MAGIC             WHEN DATE_ADD(calendar_date, (WEEKDAY(calendar_date) + 1) - 1) = calendar_date THEN TO_DATE(calendar_date)
# MAGIC             ELSE DATE_ADD(calendar_date, -(WEEKDAY(calendar_date)))
# MAGIC         END,
# MAGIC         6
# MAGIC     ) AS cal_week_end, -- End of week date
# MAGIC     WEEKDAY(calendar_date) + 1 AS cal_week_day,
# MAGIC     WEEKOFYEAR(calendar_date) AS cal_week_of_year,
# MAGIC     DATE_FORMAT(calendar_date, 'MMMM yyyy') AS cal_month_year,
# MAGIC     DATE_FORMAT(calendar_date, 'MMMM') AS cal_month_name,
# MAGIC     DATE_ADD(LAST_DAY(ADD_MONTHS(calendar_date, -1)), 1) AS cal_first_day_of_month,
# MAGIC     LAST_DAY(calendar_date) AS cal_last_day_of_month,
# MAGIC     CASE
# MAGIC         WHEN MONTH(calendar_date) IN (1, 2, 3) THEN 1
# MAGIC         WHEN MONTH(calendar_date) IN (4, 5, 6) THEN 2
# MAGIC         WHEN MONTH(calendar_date) IN (7, 8, 9) THEN 3
# MAGIC         ELSE 4
# MAGIC     END AS cal_fiscal_quarter,
# MAGIC     YEAR(DATE_ADD(calendar_date, 89)) AS cal_fiscal_year,
# MAGIC
# MAGIC     -- Technical columns
# MAGIC     current_timestamp() AS _tf_create_date,
# MAGIC     current_timestamp() AS _tf_update_date
# MAGIC FROM calendar_dates;
# MAGIC
# MAGIC -- modify our calendar table in order to set a PK (primary key)
# MAGIC ALTER TABLE gold.dim_calendar
# MAGIC ALTER COLUMN _tf_dim_calendar_id SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.dim_calendar ADD PRIMARY KEY (_tf_dim_calendar_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_geography (
# MAGIC   -- Incremental surrogate key
# MAGIC   _tf_dim_geography_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC
# MAGIC   -- Attributes
# MAGIC   geo_address_id INT,
# MAGIC   geo_address_line_1 STRING,
# MAGIC   geo_address_line_2 STRING,
# MAGIC   geo_city STRING,
# MAGIC   geo_state_province STRING,
# MAGIC   geo_country_region STRING,
# MAGIC   geo_postal_code STRING,
# MAGIC
# MAGIC   -- Technical columns
# MAGIC   _tf_create_date TIMESTAMP,
# MAGIC   _tf_update_date TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_geography (
# MAGIC     _tf_dim_geography_id, 
# MAGIC     geo_address_id, 
# MAGIC     geo_address_line_1, 
# MAGIC     geo_address_line_2, 
# MAGIC     geo_city, 
# MAGIC     geo_state_province, 
# MAGIC     geo_country_region, 
# MAGIC     geo_postal_code, 
# MAGIC     _tf_create_date, 
# MAGIC     _tf_update_date)
# MAGIC
# MAGIC VALUES (-9, 0, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_customer (
# MAGIC   -- Incremental surrogate key
# MAGIC   _tf_dim_customer_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC
# MAGIC   -- Attributes
# MAGIC   cust_customer_id INT,
# MAGIC   cust_title STRING,
# MAGIC   cust_first_name STRING,
# MAGIC   cust_middle_name STRING,
# MAGIC   cust_last_name STRING,
# MAGIC   cust_suffix STRING,
# MAGIC   cust_company_name STRING,
# MAGIC   cust_sales_person STRING,
# MAGIC   cust_email_address STRING,
# MAGIC   cust_phone STRING,
# MAGIC
# MAGIC   -- Technical columns
# MAGIC   _tf_create_date TIMESTAMP,
# MAGIC   _tf_update_date TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_customer (
# MAGIC   _tf_dim_customer_id,
# MAGIC   cust_customer_id,
# MAGIC   cust_title,
# MAGIC   cust_first_name,
# MAGIC   cust_middle_name,
# MAGIC   cust_last_name,
# MAGIC   cust_suffix,
# MAGIC   cust_company_name,
# MAGIC   cust_sales_person,
# MAGIC   cust_email_address,
# MAGIC   cust_phone,
# MAGIC   _tf_create_date,
# MAGIC   _tf_update_date)
# MAGIC
# MAGIC VALUES (-9, 0, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', current_timestamp(), current_timestamp());
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_sales (
# MAGIC   -- Incremental surrogate key
# MAGIC   _tf_fact_sales_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL, 
# MAGIC
# MAGIC   -- Source id
# MAGIC   sales_order_id INT,
# MAGIC   sales_order_detail_id INT,
# MAGIC
# MAGIC   -- Foreign keys to dimensions
# MAGIC   _tf_dim_calendar_id INT 
# MAGIC   REFERENCES gold.dim_calendar(_tf_dim_calendar_id),
# MAGIC   _tf_dim_customer_id BIGINT 
# MAGIC   REFERENCES gold.dim_customer(_tf_dim_customer_id),
# MAGIC   _tf_dim_geography_id BIGINT 
# MAGIC   REFERENCES gold.dim_geography(_tf_dim_geography_id),
# MAGIC   
# MAGIC   -- Measures
# MAGIC   sales_order_qty SMALLINT,
# MAGIC   sales_unit_price DECIMAL(19, 4),
# MAGIC   sales_unit_price_discount DECIMAL(19, 4),
# MAGIC   sales_line_total DECIMAL(38, 6),
# MAGIC   
# MAGIC   -- Technical columns
# MAGIC   _tf_create_date TIMESTAMP,
# MAGIC   _tf_update_date TIMESTAMP
# MAGIC );