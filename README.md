# 🚀 OCI AI Data Platform — Data Flow Workshop

Hands-on workshop for building a **serverless data platform on Oracle Cloud Infrastructure (OCI)** using **Apache Spark and OCI Data Flow**.

The workshop demonstrates how to integrate **OCI Data Science, OCI Data Flow, OCI Object Storage, and Oracle Autonomous Database** to build a complete data engineering pipeline following a **Bronze → Silver → Gold** architecture.

---

## 🎯 Workshop Objectives

During this workshop you will learn how to:

- ☁️ Prepare an OCI environment for data engineering workloads
- 📓 Configure **OCI Data Science Notebook Sessions**
- ⚡ Create and use **OCI Data Flow Spark Sessions**
- 🐍 Develop data pipelines using **PySpark**
- 📦 Read and write data in **OCI Object Storage**
- 🗂️ Create optimized **Parquet datasets**
- 🔍 Perform exploratory analysis and data-quality checks with Spark SQL
- 🔗 Join and transform datasets using Spark
- 🗄️ Integrate Spark with **Oracle Autonomous Database**
- 🥉 Build a **Bronze** ingestion layer
- 🥈 Build a **Silver** transformation layer
- 🥇 Build a **Gold** analytical layer
- 🔎 Debug Spark applications using Data Flow Runs and Spark UI

---

# 🏗️ Architecture

The workshop builds the following logical architecture:

```text
                  ┌──────────────────────┐
                  │    Source Datasets   │
                  │      CSV / API       │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │    OCI Data Flow     │
                  │    Apache Spark      │
                  │      PySpark         │
                  └──────────┬───────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │      OCI Object Storage      │
              │                              │
              │       🥉 BRONZE LAYER        │
              │       Parquet datasets       │
              └──────────────┬───────────────┘
                             │
                             │ Spark
                             ▼
              ┌──────────────────────────────┐
              │  Oracle Autonomous Database  │
              │                              │
              │       🥈 SILVER LAYER        │
              │  Cleansed & joined datasets  │
              └──────────────┬───────────────┘
                             │
                             │ Spark
                             ▼
              ┌──────────────────────────────┐
              │  Oracle Autonomous Database  │
              │                              │
              │        🥇 GOLD LAYER         │
              │ Analytics & aggregations     │
              └──────────────────────────────┘
```

OCI Data Science provides the development environment while OCI Data Flow provides the serverless Apache Spark runtime used by the workshop.

---

# 🧰 Technologies

| Technology | Purpose |
|---|---|
| **OCI Data Science** | Notebook and development environment |
| **OCI Data Flow** | Serverless Apache Spark execution |
| **Apache Spark / PySpark** | Distributed data processing |
| **Spark SQL** | Data exploration and analytics |
| **OCI Object Storage** | Data lake storage |
| **Apache Parquet** | Columnar storage format |
| **Oracle Autonomous Database** | Silver and Gold data storage |
| **Python** | Pipeline development |

---

# 🗂️ Repository Structure

```text
workshop-adex-dataflow/
│
├── 0-pt-config-lab/
│   ├── images/
│   ├── 0-pt-config-lab.md
│   └── 0-pt-config-lab-credit-card.md
│
├── lab/
│   ├── arquivos_csv/
│   │   ├── orders.csv
│   │   └── customers.csv
│   │
│   ├── images/
│   └── lab.md
│
├── index.html
├── manifest.json
└── README.md
```

The complete hands-on instructions are available in:

👉 **[`lab/lab.md`](./lab/lab.md)**

---

# 🧱 Data Pipeline

The workshop follows a simplified Medallion Architecture.

## 🥉 Bronze Layer — Raw Data

The first Spark application ingests the source datasets and stores them as Parquet files in OCI Object Storage.

```text
CSV
 │
 ▼
PySpark
 │
 ▼
OCI Object Storage
 │
 └── parquet/
      ├── orders/
      └── customers/
```

Example OCI paths:

```text
oci://data_bronze@<namespace>/parquet/orders
oci://data_bronze@<namespace>/parquet/customers
```

The Bronze layer introduces:

- PySpark DataFrames
- CSV ingestion
- Schema inference
- OCI Object Storage
- Parquet
- Distributed Spark processing

---

## 🔍 Bronze Data Exploration

After ingestion, Spark SQL is used to inspect and validate the datasets.

The exercises include:

- Schema inspection
- Dataset sampling
- Row counts
- Null-value analysis
- Minimum, maximum, and average values
- Duplicate detection
- Basic data-quality validation

Example:

```python
orders_df = spark.read.parquet(orders_path)

orders_df.createOrReplaceTempView("orders")

spark.sql("""
SELECT
    COUNT(*) AS row_count,
    MIN(ORDER_TOTAL) AS min_order_total,
    MAX(ORDER_TOTAL) AS max_order_total,
    AVG(ORDER_TOTAL) AS avg_order_total
FROM orders
""").show()
```

---

# 🥈 Silver Layer — Transformation

The Silver application reads the Bronze Parquet datasets and performs transformations using Spark.

The pipeline combines:

```text
ORDERS
   │
   ├──── CUSTOMER_ID ────┐
   │                     │
CUSTOMERS                 │
   │                     │
   └─────────────────────┘
             │
             ▼
          Spark JOIN
             │
             ▼
      CUSTOMERS_ORDERS
             │
             ▼
   Autonomous Database
```

Transformations include:

- Joining orders and customers
- Selecting required attributes
- Data-type conversions
- Timestamp normalization
- Date normalization
- Schema validation

The resulting Silver dataset is written to Oracle Autonomous Database.

---

# 🥇 Gold Layer — Analytics

The Gold application reads the Silver dataset from Autonomous Database and generates business-level aggregations.

Example aggregation:

```python
df_gold = (
    df_silver
    .groupBy("CUSTOMER_CLASS")
    .agg(
        F.count("ORDER_ID").alias("TOTAL_ORDERS"),
        F.round(
            F.sum("ORDER_TOTAL"),
            2
        ).alias("TOTAL_SALES"),
        F.round(
            F.avg("ORDER_TOTAL"),
            2
        ).alias("AVG_ORDER_VALUE")
    )
)
```

The resulting analytical dataset provides metrics such as:

```text
CUSTOMER_CLASS
TOTAL_ORDERS
TOTAL_SALES
AVG_ORDER_VALUE
```

and is written back to Autonomous Database for analytical consumption.

---

# ⚡ OCI Data Flow

OCI Data Flow provides the Apache Spark execution environment used throughout the workshop.

A Spark session is created from OCI Data Science and configured with resources for the driver and executors.

Example:

```python
command = prepare_command(
    {
        "displayName": "App Data Science Session",
        "language": "PYTHON",
        "sparkVersion": "3.5.0",
        "numExecutors": 1,
        "driverShape": "VM.Standard.E5.Flex",
        "executorShape": "VM.Standard.E5.Flex",
        "type": "SESSION"
    }
)
```

Notebook cells can then execute remotely against Data Flow using:

```python
%%spark
```

Execution time can also be measured with:

```python
%%time
%%spark
```

---

# ☁️ OCI Object Storage

The workshop uses separate Object Storage buckets for different responsibilities.

| Bucket | Purpose |
|---|---|
| `spark_lib` | Libraries, archives and Autonomous Database wallet |
| `spark_logs` | OCI Data Flow execution logs |
| `spark_apps` | Spark/Data Flow applications |
| `data_bronze` | Bronze Parquet datasets |

You must replace:

```text
TROCAR_AQUI_PELO_SEU_NAMESPACE
```

with your OCI Object Storage namespace when executing the exercises.

For example:

```python
base_path = (
    "oci://data_bronze@"
    "TROCAR_AQUI_PELO_SEU_NAMESPACE/parquet"
)
```

---

# 🗄️ Autonomous Database Integration

The workshop demonstrates bidirectional communication between Spark and Oracle Autonomous Database.

Spark can read from Autonomous Database using the Oracle Spark datasource:

```python
df = (
    spark.read
    .format("oracle")
    .option("walletUri", wallet_uri)
    .option("connectionId", alh_tns)
    .option("dbtable", source_table)
    .option("user", alh_user)
    .option("password", password)
    .load()
)
```

and write Spark DataFrames back to the database:

```python
(
    df.write
    .format("oracle")
    .mode("overwrite")
    .option("walletUri", wallet_uri)
    .option("connectionId", alh_tns)
    .option("dbtable", target_table)
    .option("user", alh_user)
    .option("password", password)
    .save()
)
```

> ⚠️ For environments beyond a disposable workshop, do not store database passwords directly in notebooks or source code. Use an appropriate secrets-management mechanism.

---

# 📋 Prerequisites

Before starting the workshop you should have access to an OCI tenancy with permissions to create or use:

- OCI Data Science
- OCI Data Flow
- OCI Object Storage
- Oracle Autonomous Database
- Dynamic Groups / IAM Policies required by the lab

Basic knowledge of the following is helpful but not mandatory:

- Python
- SQL
- Apache Spark
- Data engineering concepts

---

# 🚀 Getting Started

Clone the repository:

```bash
git clone https://github.com/caiogusto2/workshop-adex-dataflow.git
```

Enter the project directory:

```bash
cd workshop-adex-dataflow
```

Start with the environment preparation instructions:

👉 **[`0-pt-config-lab/0-pt-config-lab.md`](./0-pt-config-lab/0-pt-config-lab.md)**

Then continue with the complete workshop:

👉 **[`lab/lab.md`](./lab/lab.md)**

---

# 🧪 Workshop Flow

```text
1️⃣ OCI Infrastructure
        │
        ▼
2️⃣ Data Science + Data Flow
        │
        ▼
3️⃣ 🥉 Bronze
   CSV → Spark → Parquet
        │
        ▼
   🔍 Data Exploration
        │
        ▼
4️⃣ 🥈 Silver
   Parquet → Spark JOIN
        │
        ▼
   Autonomous Database
        │
        ▼
5️⃣ 🥇 Gold
   Spark Aggregations
        │
        ▼
   Autonomous Database
```

---

# 🎓 What You Will Have Built

By the end of the workshop you will have implemented an end-to-end data pipeline using Oracle Cloud:

```text
Source Data
    │
    ▼
Apache Spark
    │
    ▼
OCI Object Storage
    │
    ▼
🥉 Bronze
    │
    ▼
Data Quality / Exploration
    │
    ▼
Spark Transformations
    │
    ▼
🥈 Silver
    │
    ▼
Oracle Autonomous Database
    │
    ▼
Spark Analytics
    │
    ▼
🥇 Gold
```

The result is a practical example of combining **serverless Spark, cloud object storage, notebooks, and Oracle Autonomous Database** as an integrated data platform.

---

## 📚 Workshop

Ready to begin?

### 👉 [Start the Hands-on Lab](./lab/lab.md)

---

## 👤 Author

**Caio Augusto**

GitHub: [@caiogusto2](https://github.com/caiogusto2)

---

## ⭐ Support

If this workshop was useful, consider giving the repository a ⭐ on GitHub.

Enjoy the workshop and happy coding! 🚀