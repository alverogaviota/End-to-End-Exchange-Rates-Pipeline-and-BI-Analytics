# End-to-End Exchange Rates Pipeline & BI Analytics (Frankfurter)

The **Frankfurter Exchange Rates Pipeline** is a production-ready, end-to-end ELT (Extract, Load, Transform) data engineering pipeline built to ingest, process, and USD/IDR rates.

Utilizing data from the open-source **Frankfurter API** (backed by European Central Bank data), this project demonstrates modern data lakehouse architecture principles using the **Medallion Architecture (Bronze, Silver, Gold)** managed via **Databricks Unity Catalog** and orchestrated with **Apache Airflow**.

By separating raw data ingestion from analytical modeling, this project enables robust data governance, lineage tracking, scalable currency conversion analysis, and near-real-time business intelligence dashboarding.

## Table of Contents

- [Features](#features)
- [Tech Stack](#techstack)

## Features

- **Automated Daily API Ingestion:** Scheduled extraction of raw FX exchange rate data from the Frankfurter API directly into storage.
- **Medallion Architecture Implementation:**
  - 🟤 **Bronze Layer (Raw):** Ingests raw JSON payloads append-only with execution timestamp metadata and source tracking.
  - 🥈 **Silver Layer (Cleansed & Enriched):** Flattens nested JSON currency schema, cleans data types, handles missing records, and enforces schema validation.
  - 🥇 **Gold Layer (Business Aggregations):** Computes key metrics such as moving averages, daily percentage changes, volatility measures, and cross-currency conversion matrices.
- **Unified Governance with Unity Catalog:** Implements centralized data governance, metadata management, access controls, and table lineage across all medallion layers.
- **Orchestration & Workflow Automation:** Automated end-to-end pipeline execution and monitoring using **Apache Airflow**.


- Live Source Ingestion
- Scheduled Pipeline
- Real Time Dashboards

### Tech Stack

- **Databricks**
- **Python**
- **SQL**
- **Unity Catalog**
- **Apache Airflow**

---

Project by [alverogaviota](https://github.com/alverogaviota)