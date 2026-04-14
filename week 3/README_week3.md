# Week 3: S&P 500 End-to-End Medallion Pipeline
### Project: Production-Grade Financial Data Engineering

This project implements a complete **Medallion Architecture** using PySpark and Delta Lake to process 5 years of S&P 500 historical data. The goal was to transform a flat CSV dataset into a high-performance analytical engine.

## 🏗 Data Architecture (The Medallion Flow)
* **Bronze:** Raw ingestion of historical stock prices and company metadata into Delta tables to ensure ACID compliance.
* **Silver:** Data cleaning, schema enforcement, and relational joining of price data with sector constituents for an enriched view.
* **Gold:** Advanced analytical layer utilizing **Spark Window Functions** to calculate 7-day moving averages and 30-day volatility metrics.

## 🛠 Advanced Technical Features
- **Window Specifications:** Implemented `partitionBy` and `rowsBetween` to handle complex time-series financial logic.
- **Delta Lake Optimization:** Leveraged **Z-Ordering** by sector to drastically improve query speeds for downstream analytics.
- **Maintenance Operations:** Implemented `VACUUM` and `OPTIMIZE` commands to manage data retention and storage efficiency.
- **Scalability:** Designed the pipeline to handle over 2.3 million rows of financial records with optimized Spark partitions.

## 📈 Business Insights Generated
The final Gold layer provides a "Single Source of Truth" for:
1. **Volatility Analysis:** Identifying which market sectors are currently high-risk.
2. **Technical Indicators:** Providing ready-to-use moving average data for retail investor sentiment analysis.
