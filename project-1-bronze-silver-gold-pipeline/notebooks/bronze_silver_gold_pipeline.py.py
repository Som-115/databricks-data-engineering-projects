# Databricks notebook source
# MAGIC %md
# MAGIC ### Create database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS project_db;
# MAGIC USE project_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ### SetUp

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, count

spark = SparkSession.builder.getOrCreate()

print("Set completed.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract API Data

# COMMAND ----------

url = "https://jsonplaceholder.typicode.com/posts"

response = requests.get(url)
data = response.json()

print("Total record: ", len(data))
print("Sample data: ", data[:2])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create DataFrames

# COMMAND ----------

df = spark.createDataFrame(data)
# df.printSchema()
df.show()
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore Data

# COMMAND ----------

df.printSchema()
df.show(5)
print("total rows: ", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Bronze

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("bronze_posts")

print("Bronze layer created ")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Bronze

# COMMAND ----------

df_bronze = spark.read.table("bronze_posts")
df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze Validation

# COMMAND ----------

df_bronze.printSchema()
print("Bronze count: ", df_bronze.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform Data

# COMMAND ----------

df_silver = df_bronze.select(
    col("userId").alias("user_id"),
    col("id").alias("post_id"),
    col("title"),
    col("body")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### View before cleaning 

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove duplicates 

# COMMAND ----------

# df_silver.count()
df_silver = df_silver.dropDuplicates()
print("After dedup: ", df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add Derived Column

# COMMAND ----------

df_silver = df_silver.withColumn("title_length", length(col("title")))

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Null Check

# COMMAND ----------

from pyspark.sql.functions import sum

columns = df_silver.columns
df_silver.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver Table

# COMMAND ----------

df_silver.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("silver_posts")

print("Silver table is created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Silver

# COMMAND ----------

df_silver_read = spark.read.table("silver_posts")
df_silver_read.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation

# COMMAND ----------

df_gold = df_silver_read.groupby("user_id")\
    .agg(count("post_id").alias("total_posts"))

df_gold.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sort Output 

# COMMAND ----------

df_gold.orderBy(col("user_id").asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Gold Table

# COMMAND ----------

df_gold.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable("gold_posts_summary")

print("gold table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Gold Table

# COMMAND ----------

df_gold_read = spark.read.table("gold_posts_summary")

df_gold_read.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
