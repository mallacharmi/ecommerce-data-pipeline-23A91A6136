# E-Commerce Data Pipeline Project

**Student Name:** Malla Charmi  
**Roll Number:** 23A91A6136  
**Submission Date:** 26-12-2025

---

## 📌 Project Overview

This project implements an end-to-end E-Commerce Data Analytics Platform using a modern data engineering pipeline.  
It covers data generation, ingestion, transformation, warehousing, analytics, testing, and business intelligence dashboards.

The system is designed to simulate real-world e-commerce data processing using batch ETL pipelines and analytical dashboards.

---

## 📐 Project Architecture

The architecture follows a layered data engineering approach to ensure scalability, data quality, and analytical performance.

### 🔄 Data Flow

Raw Data → Staging → Production → Warehouse → Analytics → BI Dashboard

```
[Raw CSV Files]
        ↓
[Staging Schema]
        ↓
[Production Schema]
        ↓
[Warehouse Schema]
        ↓
[Aggregated Analytics Tables]
        ↓
[Power BI / Tableau Dashboard]
```

---

## 🧪 Technology Stack

- **Data Generation:** Python (Faker)
- **Database:** PostgreSQL
- **ETL & Transformations:** Python (Pandas, SQLAlchemy)
- **Orchestration:** Python Pipeline Orchestrator
- **BI & Visualization:** Power BI Desktop / Tableau Public
- **Containerization:** Docker & Docker Compose
- **Testing:** Pytest
- **Version Control:** Git

---

## 🛠️ Prerequisites

Ensure the following tools are installed:

- Python 3.8 or higher
- PostgreSQL 12 or higher
- Docker & Docker Compose
- Git
- Power BI Desktop (Free) **or** Tableau Public

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/mallacharmi/ecommerce-data-pipeline-23A91A6136.git
cd ecommerce-data-pipeline
```

### 2️⃣ Create Python Environment

```bash
python -m venv venv
source venv/bin/activate
```

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Start PostgreSQL using Docker

```bash
docker-compose up -d
```

### 5️⃣ Verify Database Connection

```bash
psql -h localhost -U admin -d ecommerce_db
```

---

## 📂 Project Structure

```text
ecommerce-data-pipeline/
│
├── data/
│   ├── raw/
│   ├── processed/
│
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   └── pipeline_orchestrator.py
│
├── dashboards/
│   ├── powerbi/
│   ├── tableau/
│   └── screenshots/
│
├── tests/
├── docs/
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ▶️ Running the Pipeline

### 🔹 Full Pipeline Execution

```bash
python scripts/pipeline_orchestrator.py
```

### 🔹 Individual Pipeline Steps

```bash
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
```

---

## 🧪 Running Tests

```bash
pytest tests/ -v
```

---

## 📊 Power BI Dashboard

- Tool: Power BI Desktop
- Pages: 4 (Executive, Product, Customer, Trends)
- Data Source: PostgreSQL (warehouse schema)
- File: dashboards/powerbi/ecommerce_analytics.pbix
- Export: dashboards/powerbi/dashboard_export.pdf
- Screenshots: dashboards/screenshots/

---

## 🗄️ Database Schemas

### 🔹 Staging Schema

- staging.customers
- staging.products
- staging.transactions
- staging.transaction_items

### 🔹 Production Schema

- production.customers
- production.products
- production.transactions
- production.transaction_items

### 🔹 Warehouse Schema

- warehouse.dim_customers
- warehouse.dim_products
- warehouse.dim_date
- warehouse.dim_payment_method
- warehouse.fact_sales
- warehouse.agg_daily_sales
- warehouse.agg_product_performance
- warehouse.agg_customer_metrics

---

## 📈 Key Insights from Analytics

- Clothing is the top-performing product category by total revenue.
- Monthly revenue shows steady growth with seasonal variations.
- Customers aged 26–35 contribute the highest share of revenue.
- Top 10 states generate a major portion of total sales.
- Digital payment methods dominate customer transactions.

---

## ⚠️ Challenges & Solutions

1. **Performance issues with large datasets**

   - Solved by creating warehouse-level aggregate tables.

2. **Incorrect aggregation in Power BI visuals**

   - Fixed by using product-level fields and proper scatter chart configuration.

3. **Deprecated map visuals in Power BI**

   - Replaced with bar charts for geographic analysis.

4. **Data quality issues in raw CSV files**
   - Addressed through validation and cleaning in production schema.

---

## 🚀 Future Enhancements

- Real-time data streaming using Apache Kafka
- Cloud deployment on AWS / GCP / Azure
- Machine learning models for sales forecasting
- Real-time alerting and monitoring system

---

## 📞 Contact

**Name:** Malla Charmi  
**Roll Number:** 23A91A6136  
**Email:** 23a91a6136@aec.edu.in
