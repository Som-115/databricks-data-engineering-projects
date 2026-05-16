# Databricks DLT Medallion Pipeline

## Project Overview

This project demonstrates an end-to-end Data Engineering pipeline using Databricks Lakeflow / Delta Live Tables (DLT) with the Medallion Architecture (Bronze, Silver, Gold).

The pipeline ingests multiple CSV datasets, performs data cleaning and transformations, joins datasets, and generates aggregated business insights.

---

# Architecture

Bronze Layer → Raw Data Ingestion  
Silver Layer → Data Cleaning & Validation  
Gold Layer → Business Aggregations & Analytics

---

# Technologies Used

- Databricks
- Delta Live Tables (DLT)
- PySpark
- Unity Catalog
- Databricks Volumes
- Medallion Architecture
- Data Quality Expectations

---

# Dataset Information

## Users Dataset
Contains customer details such as:
- user_id
- name
- email
- city

## Orders Dataset
Contains transaction/order details such as:
- order_id
- user_id
- amount
- product

---

# Pipeline Flow

## Bronze Layer
Raw CSV files are ingested into Bronze tables.

Tables:
- users_bronze
- orders_bronze

Operations:
- Read CSV files from Databricks Volumes
- Preserve raw source data

---

## Silver Layer
Data cleansing and validation are performed.

Tables:
- silver_users
- silver_orders

Operations:
- Remove duplicates
- Data quality checks using DLT expectations
- Data type conversions

Example Expectations:
- user_id IS NOT NULL
- amount > 0

---

## Join Transformations

Table:
- silver_customers_orders

Operations:
- Join users and orders datasets using user_id

---

## Gold Layer

Table:
- gold_customer_spending

Operations:
- Customer-level aggregations
- Total orders calculation
- Total spending calculation

Business Insights:
- Customer spending analysis
- Order analytics

---

# Pipeline Graph

The pipeline DAG demonstrates dependency flow between Bronze, Silver, and Gold layers.

---

# Project Structure

```text
databricks-dlt-medallion-pipeline/
│
├── notebooks/
│   └── dlt_multi_source_pipeline.py
│
├── datasets/
│   ├── users.csv
│   └── orders.csv
│
├── screenshots/
│   ├── pipeline_graph.png
│   └── tables_information.png
│
└── README.md