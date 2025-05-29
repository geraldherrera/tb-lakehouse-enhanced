# Databricks notebook source
from pyspark.sql.functions import expr

df = spark.table("samples.bakehouse.media_customer_reviews")

df_with_sentiment = df.select(
    "review",
    expr("""ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        "Evaluate if the review is positive or negative. Only answer with 'positive' or 'negative'" || review
    ) AS sentiment""")
)

df_with_sentiment.show(10)