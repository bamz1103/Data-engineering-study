# Week 3: S&P 500 End-to-End Medallion Pipeline
### Project: Financial Data Engineering with PySpark & Delta Lake

This repository contains a production-grade data pipeline built on **Databricks** that processes 5 years of S&P 500 historical data. The project follows the **Medallion Architecture**, transforming raw CSV data into high-value analytical assets.

## 🏗 Pipeline Architecture
1. **Bronze (Raw):** Ingested raw stock prices and sector metadata into Delta format to ensure ACID compliance.
2. **Silver (Cleaned):** Performed schema enforcement, type casting (Double/Date), and joined price data with sector constituents.
3. **Gold (Analytics):** Developed specialized tables using **Spark Window Functions** to calculate rolling 7-day moving averages and 30-day volatility.

## 🛠 Technical Implementation
* **Window Specifications:** Used `partitionBy` and `rowsBetween` for complex time-series analysis.
* **Feature Engineering:** Derived daily returns and sector-level performance metrics.
* **Data Governance:** Implemented **Delta Vacuuming** to manage storage retention and optimize the transaction log.
* **Optimization:** Leveraged **Z-Ordering** by sector to accelerate downstream analytical queries.

## 📊 Business Value
By moving from raw CSVs to a Gold-layer Delta table, this pipeline provides a "Single Source of Truth" for:
- Identifying high-volatility market sectors.
- Technical stock analysis via moving averages.
- Clean, dashboard-ready datasets for BI tools.
