# sql-data_warehouse-project
builds a data wharehouse usinf sql server ,including ETL process,data modeling and analystics

# Data Warehouse and Analytics Project

This project presents an **end-to-end Data Warehouse solution** that integrates CRM and ERP data using the **Medallion Architecture (Bronze → Silver → Gold)**.  
It focuses on building a **clean, reliable, and analytics-ready data foundation** for downstream reporting and business intelligence.

The solution demonstrates how raw operational data can be incrementally refined into structured, trusted datasets through layered transformations and modeling.

This project involves:
1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights


##  High‑Level Architecture
This diagram provides a high-level view of the overall system design and how data moves from source systems to analytical consumption layers.

At this level, the focus is on showing the separation of concerns between raw ingestion, data transformation, and business consumption, as well as how the data warehouse serves multiple downstream use cases such as BI and analytics.
![High-Level Architecture](documents/assets/Architecture.png)


##  Data Flow Across Layers
This diagram illustrates how CRM and ERP tables progress through the Bronze, Silver, and Gold layers within the data warehouse.

It highlights how data evolves across layers—from raw source-aligned tables to cleaned and standardized datasets, and finally into analytics-optimized structures designed for reporting and analysis.

![High-Level Architecture](assets/data_flow.png)


## Data Integration Overview
This diagram highlights how CRM transactional and master data is enriched and aligned with ERP reference and customer data to create unified datasets.

It emphasizes the integration logic between systems, ensuring consistency across customers, products, and categories while maintaining a single analytical view across enterprise data sources.

![High-Level Architecture](assets/data_integration.png)


## ⭐ Sales Data Mart (Gold Layer)
This diagram represents the final **Sales Data Mart**, modeled using a star schema and optimized for analytical queries and reporting.

The model is designed to support efficient querying, dimensional analysis, and aggregation by separating descriptive attributes into dimensions and measurable business metrics into a central fact table.

![High-Level Architecture](assets/data_model.png)




##  Author
**ملك وسام ممدوح محمد القباني**  
Student –facualty of computers and data science ,alexandria university 
Alexandria, Egypt







