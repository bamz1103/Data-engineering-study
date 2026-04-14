# Week 1: Data Profiling & Statistics with PySpark
### Project: Automated Summary Statistics Pipeline

This project focuses on the "Exploratory Data Analysis" (EDA) phase of the data engineering lifecycle. Using the PySpark DataFrame API, I developed a modular approach to calculating high-level descriptive statistics for large-scale datasets.

## 🛠 Technical Implementation: `summary_stats.py`
The core of this week's work involved building a script that automates the calculation of key data quality and business metrics:

- **Descriptive Analytics:** Implemented automated calculations for Mean, Standard Deviation, Min, Max, and Count across all numerical features.
- **Scalable Profiling:** Designed the logic to handle large datasets where traditional tools like Excel or Pandas would hit memory limits.
- **Data Quality Checks:** Used Spark's aggregation engine to identify distribution spreads and potential outliers in the source data.

## 🚀 Key Learning Milestones
- **Aggregation Logic:** Mastering `groupBy` and `agg` functions for multi-column statistics.
- **Spark Optimization:** Understanding how Spark distributes the calculation of summary statistics across a cluster.
- **Clean Code:** Transitioning from notebook cells to a standalone, reusable Python script.
