# Databricks notebook source
# MAGIC %md
# MAGIC # ETL - Gold layer

# COMMAND ----------

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
unity_catalogs = [c for c in catalogs if c != "hive_metastore"]

if len(unity_catalogs) == 1:
    default_catalog = unity_catalogs[0]
else:
    default_catalog = next((c for c in unity_catalogs if c.startswith("dbw_")), "hive_metastore")

dbutils.widgets.text("my_catalog", default_catalog, "Detected catalog")
catalog = dbutils.widgets.get("my_catalog")
    
dbutils.widgets.text("my_schema", "gold", "Gold Schema")

gold_schema = dbutils.widgets.get("my_schema")
silver_schema = "silver"
logs_schema = "logs"
log_table = "gold_processing_log"

# COMMAND ----------

# MAGIC %md
# MAGIC Function to log the transition to Gold

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

def log_gold_processing_result(table_name, status, rows_inserted=None, message=None):
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
        "message": message[:5000] if message else "Ingested successfully"
    }]

    df_log = spark.createDataFrame(data, schema=schema)
    df_log.write.mode("append").format("delta").saveAsTable(f"{catalog}.{logs_schema}.{log_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC DDL

# COMMAND ----------

ddl_statements = [

    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_calendar (
      date_key               INT PRIMARY KEY,
      full_date              DATE,
      year                   INT,
      quarter                INT,
      month                  INT,
      month_name_en          STRING,
      month_name_fr          STRING,
      day                    INT,
      day_of_week            INT,
      day_of_week_name_en    STRING,
      day_of_week_name_fr    STRING,
      week_of_year           INT,
      is_weekend             BOOLEAN
    ) USING DELTA
    PARTITIONED BY (year, month)
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_customer (
      customer_key   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      customer_id    INT    NOT NULL,
      title            STRING,
      first_name       STRING,
      last_name        STRING,
      company_name     STRING,
      email_address    STRING,
      phone            STRING,
      address_line1    STRING,
      city             STRING,
      state_province   STRING,
      country_region   STRING,
      postal_code      STRING
    ) USING DELTA
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_address (
      address_key    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      address_id     INT    NOT NULL,
      address_line1  STRING,
      city           STRING,
      state_province STRING,
      country_region STRING,
      postal_code    STRING
    ) USING DELTA
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.dim_product (
      product_key                       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      product_id                        INT    NOT NULL,
      name                              STRING,
      product_number                    STRING,
      color                             STRING,
      size                              STRING,
      standard_cost                     DECIMAL(19,4),
      list_price                        DECIMAL(19,4),
      product_category_id               INT,
      product_category_name             STRING,
      parent_product_category_id        INT,
      parent_product_category_name      STRING,
      product_model_id                  INT,
      product_model_name                STRING,
      product_description               STRING
    ) USING DELTA
    """,

    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.fact_sales (
      sales_order_line_key  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      sales_order_id        INT,
      sales_order_detail_id INT,
      customer_key          BIGINT,
      product_key           BIGINT,
      order_date_key        INT,
      due_date_key          INT,
      ship_date_key         INT,
      ship_to_address_key   BIGINT,
      bill_to_address_key   BIGINT,
      ship_method           STRING,
      order_qty             INT,
      unit_price            DECIMAL(19,4),
      unit_price_discount   DECIMAL(19,4),
      line_total            DECIMAL(38,6),
      tax_amt               DECIMAL(19,4),
      freight               DECIMAL(19,4),
      gross_amount          DECIMAL(19,4),
      discount_amount       DECIMAL(19,4),
      load_date             TIMESTAMP
    ) USING DELTA
    PARTITIONED BY (order_date_key)
    """
]

for ddl in ddl_statements:
    spark.sql(ddl)

# COMMAND ----------

# MAGIC %md
# MAGIC DML dimensions (SCD1)

# COMMAND ----------

def ingest_dim(table_name, query):
    full_table_name = f"{catalog}.{gold_schema}.{table_name}"
    try:
        spark.sql(f"TRUNCATE TABLE {full_table_name}")
        df = spark.sql(query)
        count = df.count()
        df.write.mode("append").saveAsTable(full_table_name)
        log_gold_processing_result(table_name, "OK", rows_inserted=count)
    except Exception as e:
        log_gold_processing_result(table_name, "KO", message=str(e))

queries = {
    "dim_calendar": f"""
        SELECT
          CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
          date AS full_date,
          year(date) AS year,
          quarter(date) AS quarter,
          month(date) AS month,
          date_format(date, 'MMMM') AS month_name_en,
          CASE month(date)
            WHEN 1 THEN 'janvier' WHEN 2 THEN 'février' WHEN 3 THEN 'mars'
            WHEN 4 THEN 'avril' WHEN 5 THEN 'mai' WHEN 6 THEN 'juin'
            WHEN 7 THEN 'juillet' WHEN 8 THEN 'août' WHEN 9 THEN 'septembre'
            WHEN 10 THEN 'octobre' WHEN 11 THEN 'novembre' WHEN 12 THEN 'décembre'
          END AS month_name_fr,
          day(date) AS day,
          dayofweek(date) AS day_of_week,
          date_format(date, 'EEEE') AS day_of_week_name_en,
          CASE dayofweek(date)
            WHEN 1 THEN 'dimanche' WHEN 2 THEN 'lundi' WHEN 3 THEN 'mardi'
            WHEN 4 THEN 'mercredi' WHEN 5 THEN 'jeudi'
            WHEN 6 THEN 'vendredi' WHEN 7 THEN 'samedi'
          END AS day_of_week_name_fr,
          weekofyear(date) AS week_of_year,
          CASE WHEN dayofweek(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
        FROM (
          SELECT sequence(
            TO_DATE('2000-01-01'),
            TO_DATE('2030-12-31'),
            INTERVAL 1 DAY
          ) AS date_array
        ) LATERAL VIEW explode(date_array) AS date
    """,
    "dim_customer": f"""
        SELECT
          c.customer_id,
          c.title,
          c.first_name,
          c.last_name,
          c.company_name,
          c.email_address,
          c.phone,
          a.address_line1,
          a.city,
          a.state_province,
          a.country_region,
          a.postal_code
        FROM {catalog}.{silver_schema}.silver_saleslt_customer c
        LEFT JOIN {catalog}.{silver_schema}.silver_saleslt_customer_address ca
          ON ca.customer_id = c.customer_id AND ca.is_current = true
        LEFT JOIN {catalog}.{silver_schema}.silver_saleslt_address a
          ON a.address_id = ca.address_id AND a.is_current = true
        WHERE c.is_current = true
    """,
    "dim_address": f"""
        SELECT
          address_id,
          address_line1,
          city,
          state_province,
          country_region,
          postal_code
        FROM {catalog}.{silver_schema}.silver_saleslt_address
        WHERE is_current = true
    """,
    "dim_product": f"""
        SELECT
          p.product_id,
          p.name,
          p.product_number,
          p.color,
          p.size,
          p.standard_cost,
          p.list_price,
          c.product_category_id,
          c.name AS product_category_name,
          c.parent_product_category_id,
          pc.name AS parent_product_category_name,
          m.product_model_id,
          m.name AS product_model_name,
          d.description AS product_description
        FROM {catalog}.{silver_schema}.silver_saleslt_product p
        LEFT JOIN {catalog}.{silver_schema}.silver_saleslt_product_category c
          ON p.product_category_id = c.product_category_id AND c.is_current = true
        LEFT JOIN {catalog}.{silver_schema}.silver_saleslt_product_category pc
          ON c.parent_product_category_id = pc.product_category_id AND pc.is_current = true
        LEFT JOIN {catalog}.{silver_schema}.silver_saleslt_product_model m
          ON p.product_model_id = m.product_model_id AND m.is_current = true
        LEFT JOIN {catalog}.{silver_schema}.silver_saleslt_product_model_product_description mpd
          ON m.product_model_id = mpd.product_model_id AND mpd.is_current = true AND mpd.culture = 'en'
        LEFT JOIN {catalog}.{silver_schema}.silver_saleslt_product_description d
          ON mpd.product_description_id = d.product_description_id AND d.is_current = true
        WHERE p.is_current = true
    """
}

for table_name, sql_query in queries.items():
    ingest_dim(table_name, sql_query)


# COMMAND ----------

# MAGIC %md
# MAGIC DML facts (SCD1)

# COMMAND ----------

def ingest_fact_sales():
    table_name = "fact_sales"
    full_table_name = f"{catalog}.{gold_schema}.{table_name}"
    try:
        spark.sql(f"TRUNCATE TABLE {full_table_name}")

        query = f"""
        SELECT
          h.sales_order_id,
          d.sales_order_detail_id,
          c.customer_key,
          p.product_key,
          CAST(date_format(h.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
          CAST(date_format(h.due_date, 'yyyyMMdd') AS INT) AS due_date_key,
          CAST(date_format(h.ship_date, 'yyyyMMdd') AS INT) AS ship_date_key,
          sa.address_key AS ship_to_address_key,
          ba.address_key AS bill_to_address_key,
          h.ship_method AS ship_method,
          d.order_qty,
          d.unit_price,
          d.unit_price_discount,
          d.line_total,
          CAST(d.order_qty * d.unit_price AS DECIMAL(19,4)) AS gross_amount,
          CAST((d.order_qty * d.unit_price) - d.line_total AS DECIMAL(19,4)) AS discount_amount,
          h.tax_amt,
          h.freight,
          current_timestamp() AS load_date
        FROM {catalog}.{silver_schema}.silver_saleslt_sales_order_detail d
        JOIN {catalog}.{silver_schema}.silver_saleslt_sales_order_header h
          ON d.sales_order_id = h.sales_order_id AND h.is_current = true
        JOIN {catalog}.{gold_schema}.dim_customer c
          ON h.customer_id = c.customer_id
        JOIN {catalog}.{gold_schema}.dim_product p
          ON d.product_id = p.product_id
        JOIN {catalog}.{gold_schema}.dim_address sa
          ON h.ship_to_address_id = sa.address_id
        JOIN {catalog}.{gold_schema}.dim_address ba
          ON h.bill_to_address_id = ba.address_id
        WHERE d.is_current = true
        """

        df = spark.sql(query)
        inserted_count = df.count()
        df.write.mode("append").saveAsTable(full_table_name)
        log_gold_processing_result(
            table_name, "OK",
            rows_inserted=inserted_count,
            message="Ingestion réussie"
        )

    except Exception as e:
        log_gold_processing_result(table_name, "KO", message=str(e))

ingest_fact_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC Views

# COMMAND ----------

def create_gold_view(view_name: str, sql_body: str):
    try:
        spark.sql(f"CREATE OR REPLACE VIEW {catalog}.{gold_schema}.{view_name} AS {sql_body}")
        log_gold_processing_result(view_name, "OK", message="Vue créée avec succès")
    except Exception as e:
        log_gold_processing_result(view_name, "KO", message=str(e))

fs = f"{catalog}.{gold_schema}.fact_sales"
dp = f"{catalog}.{gold_schema}.dim_product"
dc = f"{catalog}.{gold_schema}.dim_customer"
cal = f"{catalog}.{gold_schema}.dim_calendar"

views = {
    # Dashboard 1 – Vue d’ensemble commerciale
    "vw_total_revenue": f"""
        SELECT SUM(line_total) AS total_revenue FROM {fs}
    """,

    "vw_total_orders": f"""
        SELECT COUNT(DISTINCT sales_order_id) AS total_orders FROM {fs}
    """,

    "vw_clients_active": f"""
        SELECT COUNT(DISTINCT customer_key) AS clients_active FROM {fs}
    """,

    "vw_monthly_revenue_fr": f"""
        SELECT dc.year, dc.month, dc.month_name_fr AS month_name, SUM(fs.line_total) AS total_revenue
        FROM {fs} fs
        JOIN {cal} dc ON fs.order_date_key = dc.date_key
        GROUP BY dc.year, dc.month, dc.month_name_fr
        ORDER BY dc.year, dc.month
    """,

    "vw_monthly_revenue_en": f"""
        SELECT dc.year, dc.month, dc.month_name_en AS month_name, SUM(fs.line_total) AS total_revenue
        FROM {fs} fs
        JOIN {cal} dc ON fs.order_date_key = dc.date_key
        GROUP BY dc.year, dc.month, dc.month_name_en
        ORDER BY dc.year, dc.month
    """,

    "vw_revenue_by_category": f"""
        SELECT dp.product_category_name, SUM(fs.line_total) AS total_revenue
        FROM {fs} fs
        JOIN {dp} dp ON fs.product_key = dp.product_key
        GROUP BY dp.product_category_name
    """,

    "vw_revenue_by_region": f"""
        SELECT dc.state_province, SUM(fs.line_total) AS total_revenue
        FROM {fs} fs
        JOIN {dc} dc ON fs.customer_key = dc.customer_key
        GROUP BY dc.state_province
    """,

    # Dashboard 2 – Produits et performance catalogue
    "vw_total_units_sold": f"""
        SELECT SUM(order_qty) AS total_units_sold FROM {fs}
    """,

    "vw_top_products_by_volume": f"""
        SELECT dp.name AS product_name, SUM(fs.order_qty) AS total_units_sold
        FROM {fs} fs
        JOIN {dp} dp ON fs.product_key = dp.product_key
        GROUP BY dp.name
        ORDER BY total_units_sold DESC
        LIMIT 10
    """,

    "vw_products_sold_summary": f"""
        SELECT dp.name AS product_name,
               dp.product_category_name,
               SUM(fs.line_total) AS total_revenue,
               SUM(fs.order_qty) AS total_quantity
        FROM {fs} fs
        JOIN {dp} dp ON fs.product_key = dp.product_key
        GROUP BY dp.name, dp.product_category_name
    """,

    "vw_sales_by_product_month_fr": f"""
        SELECT dp.name AS product_name, dc.year, dc.month, dc.month_name_fr AS month_name,
               SUM(fs.line_total) AS total_revenue
        FROM {fs} fs
        JOIN {dp} dp ON fs.product_key = dp.product_key
        JOIN {cal} dc ON fs.order_date_key = dc.date_key
        GROUP BY dp.name, dc.year, dc.month, dc.month_name_fr
    """,

    "vw_sales_by_product_month_en": f"""
        SELECT dp.name AS product_name, dc.year, dc.month, dc.month_name_en AS month_name,
               SUM(fs.line_total) AS total_revenue
        FROM {fs} fs
        JOIN {dp} dp ON fs.product_key = dp.product_key
        JOIN {cal} dc ON fs.order_date_key = dc.date_key
        GROUP BY dp.name, dc.year, dc.month, dc.month_name_en
    """,

    "vw_unsold_products": f"""
        SELECT p.product_id, p.name, p.product_category_name
        FROM {dp} p
        LEFT JOIN {fs} fs ON fs.product_key = p.product_key
        WHERE fs.product_key IS NULL
    """,

    # Dashboard 3 – Analyse client
    "vw_orders_per_customer": f"""
        SELECT dc.customer_id, dc.first_name, dc.last_name, dc.country_region, dc.state_province,
               COUNT(DISTINCT fs.sales_order_id) AS total_orders
        FROM {fs} fs
        JOIN {dc} dc ON fs.customer_key = dc.customer_key
        GROUP BY dc.customer_id, dc.first_name, dc.last_name, dc.country_region, dc.state_province
        ORDER BY total_orders DESC
    """,

    "vw_customer_value_summary": f"""
        SELECT dc.customer_id, dc.first_name, dc.last_name,
               COUNT(DISTINCT fs.sales_order_id) AS total_orders,
               SUM(fs.line_total) AS total_revenue
        FROM {fs} fs
        JOIN {dc} dc ON fs.customer_key = dc.customer_key
        GROUP BY dc.customer_id, dc.first_name, dc.last_name
    """
}

for view_name, sql_query in views.items():
    create_gold_view(view_name, sql_query)