# E-Commerce Data Pipeline Architecture

## Overview

This document describes the architecture of the e-commerce analytics platform designed to process, store, and visualize sales data efficiently.

## System Components

### 1. Data Generation Layer

- Generates synthetic e-commerce data using Python Faker
- Outputs CSV files for customers, products, transactions

### 2. Data Ingestion Layer

- Loads CSV data into PostgreSQL staging schema
- Batch ingestion using Python and psycopg2

### 3. Data Storage Layer

- Staging Schema: Raw data
- Production Schema: Cleaned and normalized (3NF)
- Warehouse Schema: Star schema optimized for analytics

### 4. Data Processing Layer

- Data quality validation
- SCD Type 2 implementation for dimensions
- Aggregation tables for performance

### 5. Data Serving Layer

- Analytical SQL queries
- Pre-computed aggregates

### 6. Visualization Layer

- Power BI / Tableau dashboards
- Interactive analytics with filters

### 7. Orchestration Layer

- Central pipeline orchestrator
- Sequential execution of ETL steps

## Data Models

### Staging Model

- Mirrors raw CSV structure
- Minimal constraints

### Production Model

- Normalized schema
- Enforced data integrity

### Warehouse Model

- Star schema
- Fact and dimension tables
- Aggregates for performance

## Deployment Architecture

- Docker-based PostgreSQL deployment
- Local execution environment
