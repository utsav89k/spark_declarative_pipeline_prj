# Project_SDP — Spark Declarative Pipeline 

A production-style, end-to-end **Medallion Architecture** data pipeline built using **Databricks Spark Declarative Pipelines (SDP)**. This project ingests multi-source streaming data from **Amazon S3**, applies layered transformations, and delivers a fully modelled **Gold Layer** with SCD Type 2 history tracking and a One Big Table (OBT) for analytics.

---

## 📌 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Pipeline Lineage](#pipeline-lineage)
- [Project Structure](#project-structure)
- [Layers](#layers)
  - [Bronze Layer](#-bronze-layer)
  - [Silver Layer](#-silver-layer)
  - [Gold Layer](#-gold-layer)
- [Tech Stack](#tech-stack)
- [Key Concepts Used](#key-concepts-used)
- [How to Run](#how-to-run)

---

## Overview

This pipeline processes **music streaming event data** — tracking what songs users listen to, on which devices, for how long, and from where. The pipeline is fully incremental, automatically handling schema evolution, deduplication, and historical change tracking using SCD Type 2.

---

## Architecture

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold):

```
Amazon S3 (Raw CSV Files)
        ↓
  🥉 BRONZE LAYER      →  Raw streaming ingestion (multi-source)
        ↓
  🥈 SILVER LAYER      →  Cleaned & transformed data
        ↓
  🥇 GOLD LAYER        →  Dimensional model + SCD Type 2 + OBT
```

---

## Pipeline Lineage

The image below shows the full end-to-end pipeline lineage as seen in Databricks:

![Pipeline Lineage](Project_SDP/pipeline_lineage.png)

> Each node represents a table/view in the pipeline. Arrows show data flow and dependencies — managed automatically by Spark Declarative Pipelines.

---

## Project Structure

```
Project_SDP/
│
├── transformations/              # All SDP pipeline transformation files
│   ├── DimArtist.py              # Gold - Artist dimension (SCD Type 2)
│   ├── DimDate.py                # Gold - Date dimension
│   ├── DimTrack.py               # Gold - Track dimension (SCD Type 2)
│   ├── DimUser.py                # Gold - User dimension (SCD Type 2)
│   ├── FactStream.py             # Bronze + Silver + Gold Fact streaming table
│   └── OBT.py                   # Gold - One Big Table (Materialized View)
│
├── explorations/                 # Notebooks for testing and exploration
│   ├── Testing the Bronze Layer.ipynb
│   ├── Testing Silver Layer.ipynb
│   ├── Testing Gold Layer.ipynb
│   ├── Remove.ipynb
│   └── sample_exploration.py
│
├── utilities/                    # Helper utilities
│
└── README.md                     # Project documentation
```

---

## Layers

### 🥉 Bronze Layer

**Purpose:** Raw ingestion of streaming CSV data from Amazon S3.

- Reads data incrementally using `spark.readStream`
- Two separate sources merged into a single streaming table using `append_flow`
- Schema enforced at ingestion time
- No transformations — raw data preserved as-is

---

### 🥈 Silver Layer

**Purpose:** Cleaned, validated, and enriched data ready for dimensional modelling.

- Built as **Materialized Views** on top of Bronze tables
- Business transformations applied (e.g. `is_carryable` column derived from `device_type`)
- Columns selected and reordered for downstream consumption
- Auto-refreshes incrementally when Bronze data changes



---

### 🥇 Gold Layer

**Purpose:** Fully modelled dimensional data ready for BI, dashboards, and analytics.

#### Dimension Tables (SCD Type 2)
All dimensions are managed using `create_auto_cdc_flow` with `stored_as_scd_type=2`, preserving full historical changes:

| Table | Description |
|---|---|
| `DimArtist` | Artist details including genre |
| `DimTrack` | Track metadata — album, duration, category, release year |
| `DimDate` | Date dimension for time-based analysis |
| `DimUser` | User profile — country, subscription type |

#### Fact Table
| Table | Description |
|---|---|
| `FacttStream` | Streaming events with SCD Type 2 history tracking |

#### One Big Table (OBT)
| Table | Description |
|---|---|
| `OBT` | Materialized View joining all Gold tables for easy analytics consumption |



---

## Tech Stack

| Technology | Purpose |
|---|---|
| **Databricks** | Cloud data platform |
| **Spark Declarative Pipelines (SDP)** | Pipeline orchestration & incremental processing |
| **Apache Spark** | Distributed data processing |
| **Delta Lake** | ACID transactions, schema evolution, time travel |
| **Amazon S3** | Raw data storage (CSV source files) |
| **Python** | Transformation logic |
| **Unity Catalog** | Data governance & cataloging |

---

## Key Concepts Used

- ✅ **Medallion Architecture** — Bronze, Silver, Gold layering
- ✅ **Spark Declarative Pipelines (SDP)** — `append_flow`, `materialized_view`, `create_streaming_table`
- ✅ **Multi-source Streaming Ingestion** — Two S3 sources merged into one Bronze table
- ✅ **SCD Type 2** — Full historical change tracking via `create_auto_cdc_flow`
- ✅ **Incremental Processing** — Automatic, no manual checkpoint management
- ✅ **One Big Table (OBT)** — Denormalized Gold table for analytics
- ✅ **Unity Catalog** — Centralized governance across all layers

---

## How to Run

1. **Set up Databricks workspace** with Unity Catalog enabled
2. **Upload CSV source files** to your S3 bucket
3. **Create the pipeline** in Databricks → Jobs & Pipelines → Create Pipeline
4. **Point source code** to `transformations/` folder
5. **Set default catalog** to `declarative_catalog`
6. Click **Start** to run the pipeline
7. For a clean run from scratch, click **Full Refresh All**

---

> Built with ❤️ using Databricks Spark Declarative Pipelines
