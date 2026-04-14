# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC ### Project: S&P 500 Medallion Pipeline
# MAGIC **Goal:** This notebook ingests raw CSV files from the landing zone (Volumes) and persists them into **Delta Lake** format. 
# MAGIC In the Bronze layer, we maintain **data fidelity**, meaning we do not apply any transformations or cleaning yet. This serves as our "Source of Truth."

# COMMAND ----------

# Ingesting the main stock price dataset
# We use header=True because the CSV contains column names
# We use inferSchema=True to let Spark identify initial data types# Read the raw CSV
raw_stocks_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/workspace/default/interview_prep/week3/all_stocks_5yr.csv")) 

# Write to Delta Lake
# Using 'overwrite' mode to ensure the notebook is idempotent (can be run multiple times)
raw_stocks_df.write.format("delta").mode("overwrite").saveAsTable("bronze_sp500_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesting Metadata (Constituents)
# MAGIC To add business value in the later stages, we need to map stock tickers (like 'AAPL') to their specific sectors (like 'Technology'). 
# MAGIC Here we ingest the `constituents.csv` file which acts as our lookup table.

# COMMAND ----------

# Read the constituents CSV
raw_constituents_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/workspace/default/interview_prep/week3/constituents.csv"))

# Rename columns to replace spaces and hyphens with underscores for Delta compatibility
raw_constituents_df = raw_constituents_df.toDF(*[col.replace(" ", "_").replace("-", "_") for col in raw_constituents_df.columns])

# Write to Delta (Bronze)
raw_constituents_df.write.format("delta").mode("overwrite").saveAsTable("bronze_sp500_constituents")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Validation
# MAGIC A quick check to ensure the Bronze tables were created successfully and contain the expected number of records.

# COMMAND ----------

# Display a sample of the raw data
display(spark.table("bronze_sp500_prices").limit(5))

# Count rows to verify ingestion scale
print(f"Total rows in Bronze Price Table: {spark.table('bronze_sp500_prices').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Modular Transformation Pipeline
# MAGIC ### Project: S&P 500 Medallion Architecture
# MAGIC
# MAGIC **Objective:** To transform raw, unvalidated Bronze data into a clean, enriched, and analysis-ready Silver table. This notebook follows **production-grade engineering principles** to ensure the pipeline is scalable and maintainable.
# MAGIC
# MAGIC
# MAGIC 1. **Functional Encapsulation:** By wrapping the transformation logic in `clean_stock_data`, we make the code **unit-testable** and decoupled from the data sources.
# MAGIC 2. **Type Hinting:** We use Python type hints (`DataFrame`) to improve code readability and allow for better IDE support/debugging.
# MAGIC 3. **Data Enrichment (Joining):** We perform a relational join between price data and sector metadata to unlock high-level business insights in the Gold layer.
# MAGIC 4. **Schema Enforcement:** We explicitly cast columns to `double` and `long` to ensure mathematical precision and prevent schema drift.
# MAGIC 5. **Feature Engineering:** We pre-calculate `daily_return` at this stage to standardize how performance is measured across the organization.

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# Load the Bronze tables
prices_df = spark.table("bronze_sp500_prices")
constituents_df = spark.table("bronze_sp500_constituents")

# COMMAND ----------

from pyspark.sql import DataFrame

def clean_stock_data(prices_df: DataFrame, constituents_df: DataFrame) -> DataFrame:
    """
    Cleans raw stock prices and enriches with sector metadata.
    Includes type casting and feature engineering for daily returns.
    """
    # Define business logic expressions
    daily_return_expr = (col("close") - col("open")) / col("open")
    
    return (prices_df
        .join(constituents_df, prices_df.Name == constituents_df.Symbol, "inner")
        .select(
            to_date(col("date")).alias("date"),
            col("Name").alias("ticker"),
            col("GICS_Sector").alias("sector"),
            col("open").cast("double"),
            col("high").cast("double"),
            col("low").cast("double"),
            col("close").cast("double"),
            col("volume").cast("long"),
            daily_return_expr.alias("daily_return")
        )
        .filter(col("close").isNotNull())
    )

# COMMAND ----------

# Load Data from the Bronze Layer
prices_raw = spark.table("bronze_sp500_prices")
constituents_raw = spark.table("bronze_sp500_constituents")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution & Persistence (The Write)
# MAGIC Now that my logic is encapsulated in `clean_stock_data`, I invoke the function to process my Bronze tables. 
# MAGIC I will then write the resulting DataFrame to the **Delta Lake** format.
# MAGIC
# MAGIC **Engineering Highlights:**
# MAGIC * **Idempotency:** Using `.mode("overwrite")` allows me to re-run this notebook without manual cleanup.
# MAGIC * **Schema Evolution:** I included `.option("overwriteSchema", "true")` to ensure that if I add features later, the table updates gracefully.
# MAGIC * **Table Metadata:** I added a description to the table in the Catalog for better data discoverability.

# COMMAND ----------

# Invoke the transformation function
silver_df = clean_stock_data(prices_raw, constituents_raw)

# Write the cleaned data to the Silver layer
(silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("description", "Enriched S&P 500 prices with sectors and daily returns")
    .saveAsTable("silver_sp500_cleaned"))

# Verify the final table exists and look at the results
display(spark.table("silver_sp500_cleaned").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Advanced Analytics & Window Functions
# MAGIC ### Project: S&P 500 Medallion Pipeline
# MAGIC
# MAGIC **Objective:** Transform cleaned Silver data into specialized "Gold" tables for business intelligence. 
# MAGIC
# MAGIC **Senior Engineering Implementation Details:**
# MAGIC * **Window Specifications:** I defined `WindowSpec` objects to handle time-series calculations (Moving Averages and Volatility) without collapsing the dataset.
# MAGIC * **Complex Aggregations:** Creating a sector-level performance table for high-level executive reporting.
# MAGIC * **Optimization:** Using partitioning to ensure that these final analytical tables are lightning-fast for BI tools like PowerBI or Tableau.

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, avg, stddev, last

# Load the Silver table
silver_df = spark.table("silver_sp500_cleaned")

# 1. Define the Window Specification for Moving Averages
# Partition by ticker, sort by date, and look at the last 7 rows (including current)
window_7day = (Window.partitionBy("ticker")
               .orderBy("date")
               .rowsBetween(-6, 0))

# 2. Define the Window Specification for Volatility (Standard Deviation)
# We look at a wider window (e.g., 30 days) to see price stability
window_30day = (Window.partitionBy("ticker")
                .orderBy("date")
                .rowsBetween(-29, 0))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 1: `gold_stock_metrics`
# MAGIC This table calculates rolling technical indicators (7-day Moving Average and 30-day Volatility) for every stock. 
# MAGIC This demonstrates an understanding of **analytical SQL patterns** applied in Spark.

# COMMAND ----------

gold_stock_metrics = (silver_df
    .withColumn("moving_avg_7d", avg(col("close")).over(window_7day))
    .withColumn("volatility_30d", stddev(col("daily_return")).over(window_30day))
    .select("date", "ticker", "sector", "close", "moving_avg_7d", "volatility_30d")
)

# Persist the first Gold table
(gold_stock_metrics.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_stock_metrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 2: `gold_sector_performance`
# MAGIC This table provides a monthly snapshot of how different sectors (Technology, Energy, etc.) are performing. 
# MAGIC It aggregates millions of rows into a high-level summary.

# COMMAND ----------

from pyspark.sql.functions import month, year

gold_sector_performance = (silver_df
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .groupBy("year", "month", "sector")
    .agg(
        avg("daily_return").alias("avg_monthly_return"),
        stddev("daily_return").alias("sector_volatility")
    )
    .orderBy("year", "month", "avg_monthly_return", ascending=False)
)

# Persist the second Gold table
(gold_sector_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_sector_performance"))

# Final view of the Gold data
display(spark.table("gold_sector_performance").limit(10))

# COMMAND ----------

# Vacuum removes files no longer committed to the Delta table 
# and are older than the retention threshold (default is 7 days).
spark.sql("VACUUM gold_sector_performance")

# COMMAND ----------

