# Crypto Streaming Lakehouse

A real-time data pipeline on Azure: live crypto prices from Coinbase are
streamed through Azure Event Hubs into Databricks, processed with Spark
Structured Streaming using the medallion architecture (bronze, silver,
gold), and shown on a live dashboard.



## Tech Stack
Python, Azure Event Hubs, Azure Databricks, Spark Structured Streaming,
Delta Lake, Databricks SQL
