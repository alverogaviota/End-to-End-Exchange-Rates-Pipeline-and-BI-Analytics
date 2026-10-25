# End-to-End Exchange Rates Pipeline & BI Analytics (Frankfurter)

The **Frankfurter Exchange Rates Pipeline** is an end-to-end ELT (Extract, Load, Transform) data engineering pipeline built to ingest, process, and USD/IDR rates.

Utilizing data from the open-source **Frankfurter API** (backed by European Central Bank data), this project demonstrates modern data lakehouse architecture principles using the **Medallion Architecture (Bronze, Silver, Gold)** managed via **Databricks Unity Catalog** and orchestrated with **Apache Airflow**.

By separating raw data ingestion from analytical modeling, this project enables robust data governance, lineage tracking, scalable currency conversion analysis, and near-real-time business intelligence dashboarding.

## Table of Contents
- [Arhictecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)

## Architecture
flowchart LR
    API["Data Source<br/>Frankfurter API<br/>USD/IDR Exchange Rates"]:::external
    Airflow["Orchestration<br/>Apache Airflow"]:::orchestration

    subgraph Lakehouse ["Lakehouse — Databricks Unity Catalog"]
        direction LR
        Bronze[("🟤 Bronze<br/>Raw")]:::bronze
        Silver[("🥈 Silver<br/>Cleansed")]:::silver
        Gold[("🥇 Gold<br/>Aggregated Metrics")]:::gold
        Bronze -->|Cleanse| Silver
        Silver -->|Aggregate| Gold
    end

    Dashboards["Consumption<br/>Databricks AI/BI Dashboards"]:::external

    API -->|Extract| Airflow
    Airflow -->|Load raw data| Bronze
    Gold -->|Query metrics| Dashboards

    classDef external fill:#f0fdfa,stroke:#2dd4bf
    classDef orchestration fill:#f5f3ff,stroke:#a78bfa
    classDef bronze fill:#fff7ed,stroke:#fb923c
    classDef silver fill:#f0f9ff,stroke:#38bdf8
    classDef gold fill:#fefce8,stroke:#facc15


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
