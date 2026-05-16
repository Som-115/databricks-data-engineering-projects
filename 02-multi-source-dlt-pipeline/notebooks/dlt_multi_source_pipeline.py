# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Project Overview

# COMMAND ----------

# MAGIC %md
# MAGIC #### Objective

# COMMAND ----------

# MAGIC %md
# MAGIC Build a multi-source Delta Live Tables (DLT) pipeline using Bronze, Silver, and Gold architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC Pipeline Flow
# MAGIC
# MAGIC Users CSV + Orders CSV
# MAGIC         ↓
# MAGIC      Bronze
# MAGIC         ↓
# MAGIC      Silver
# MAGIC         ↓
# MAGIC       Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Imports & Setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Required Libraries

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create Catalog & Schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ecommerce_dlt_db;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use database

# COMMAND ----------

# MAGIC %sql
# MAGIC USE workspace.ecommerce_dlt_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Load Users Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Users Dataset Path

# COMMAND ----------

users_path = "/Volumes/workspace/ecommerce_dlt_db/source_files/users.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Load Orders Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Orders Dataset Path

# COMMAND ----------

orders_path = "/Volumes/workspace/ecommerce_dlt_db/source_files/orders.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze Users Table

# COMMAND ----------

@dp.table(
    name = "users_bronze",
    comment = "Raw data from users table"
)

def users_bronze():
    return(
        spark.read.format("csv")
            .option("header", "true")
                .load(users_path)
    )






# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze Orders Table

# COMMAND ----------

@dp.table(
    name = "orders_bronze",
    comment = "Raw data from orders table"
)

def orders_bronze():
    return(
        spark.read.format("csv")
            .option("header", "true")
                .load(orders_path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver Users Table

# COMMAND ----------

@dp.table(
    name = "silver_users",
    comment = "cleaned silver table for users"
)

@dp.expect_or_drop(
    "valid user Id",
    "user_id is not NULL"
)

def silver_users():
    return(
        dp.read("users_bronze")
            .dropDuplicates()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver Orders Table

# COMMAND ----------

@dp.table(
    name = "silver_orders",
    comment = "cleaned orders table"
)

@dp.expect_or_drop(
    "valid amount",
    "amount > 0"
)

def silver_orders():
    return(
        dp.read("orders_bronze")
            .withColumn("amount", col("amount").cast("int"))
            .dropDuplicates()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Join Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Joined Customer Orders Table

# COMMAND ----------

@dp.table(
    name = "silver_customers_orders",
    comment = "Joined users and orders dataset"
)

def silver_customers_orders():
    users_df = dp.read("silver_users")
    orders_df = dp.read("silver_orders")
    return(
        users_df.join(orders_df, on = "user_id", how = "inner")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Gold Layer Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer Spending Analytics

# COMMAND ----------

@dp.table(
    name = "gold_customer_spending",
    comment = "customer_spending_analytics"
)

def gold_customer_spending():
    df = dp.read("silver_customers_orders")

    return(
        df.groupBy("user_id", "name")\
            .agg(
                count("order_id").alias("total_orders"),
                sum("amount").alias("total_spent")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. DLT Expectations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Expectations Used

# COMMAND ----------

# MAGIC %md
# MAGIC 1. user_id should not be null
# MAGIC
# MAGIC 2. amount should be greater than 0
# MAGIC
# MAGIC 3. duplicate records are removed in silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Pipeline Execution

# COMMAND ----------

# MAGIC %md
# MAGIC #### Steps to Execute Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Go to Jobs & Pipelines
# MAGIC
# MAGIC 2. Create ETL Pipeline
# MAGIC
# MAGIC 3. Select this notebook as source
# MAGIC
# MAGIC 4. Choose target schema
# MAGIC
# MAGIC 5. Run pipeline
# MAGIC
# MAGIC 6. Observe DAG and lineage
