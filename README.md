# Data Warehouse and Analytics Project

## Welcome to the Data Warehouse and Analytics Project repository!

This project demonstrates a complete end-to-end data warehousing and analytics solution — from building a structured data warehouse to generating actionable business insights.

Designed as a portfolio project, it highlights modern industry best practices in data engineering, data modeling, and analytics.

## Data Architecture

This project follows the Medallion Architecture approach using three structured layers:

### Bronze Layer

- Stores raw data as-is from source systems

- Data is ingested from CSV files into a SQL Server database

- No transformation is applied at this stage

 ### Silver Layer

- Performs data cleansing and validation

- Standardizes and normalizes data

- Resolves data quality issues

- Prepares structured datasets for analytics

### Gold Layer

- Contains business-ready data

- Modeled into a Star Schema

- Includes fact and dimension tables

- Optimized for reporting and analytical queries

## Project Overview

This project includes:

### Data Architecture

Designing a modern data warehouse using the Medallion Architecture (Bronze, Silver, Gold layers).

### ETL Pipelines

Extracting, transforming, and loading data from source systems into the warehouse.

### Data Modeling

Developing fact and dimension tables optimized for analytical performance.

### Analytics & Reporting

Creating SQL-based reports and dashboards to generate actionable insights.

## Skills Demonstrated

### This repository is ideal for showcasing expertise in:

- SQL Development

- Data Architecture

- Data Engineering

- ETL Pipeline Development

- Data Modeling

- Data Analytics

##  Tools & Resources
### Datasets

CSV files used as source systems (ERP and CRM data).

### SQL Server Express

Lightweight database server used to host the data warehouse.

### SQL Server Management Studio (SSMS)

GUI tool for managing and querying SQL Server databases.

### Git & GitHub

Version control and project management.

### DrawIO

Used for designing data architecture diagrams and models.

### Notion

Project template and structured project phases documentation.

## Project Requirements
### Part 1: Building the Data Warehouse (Data Engineering)
### Objective

Develop a modern data warehouse using SQL Server to consolidate sales data and enable analytical reporting.

### Specifications

- Data Sources: Import data from two systems (ERP and CRM) provided as CSV files

- Data Quality: Clean and resolve inconsistencies before analysis

- Integration: Merge both sources into a unified analytical data model

- Scope: Focus only on the latest dataset (no historization required)

- Documentation: Provide clear data model documentation for business and analytics stakeholders

## Part 2: BI – Analytics & Reporting (Data Analysis)
### Objective

### Develop SQL-based analytics to deliver insights into:

- Customer Behavior

- Product Performance

-- Sales Trends

These insights empower stakeholders with meaningful business metrics for strategic decision-making.

## Expected Outcomes

### By completing this project, you will have:

- Designed a structured data warehouse

- Built ETL pipelines

- Created a star schema data model

- Developed analytical SQL queries

- Generated actionable business insights
