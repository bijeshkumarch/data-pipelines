# Indian E-Commerce Data Pipeline (S3 → Snowflake → Airflow)

This project demonstrates an end-to-end **data engineering pipeline** built using:

* **Apache Airflow** (orchestration)
* **AWS S3** (data lake storage)
* **Snowflake** (data warehouse)
* **Docker** (local development environment)

The dataset used is an **Indian E-Commerce Sales Dataset** from Kaggle.

---

## 📦 Project Overview

The goal of this pipeline is to:

1. Extract raw CSV files stored in **S3**.
2. Load them into **Snowflake RAW schema**.
3. Transform them into **MODELED (DIM/FACT) tables**.
4. Automate the entire workflow using **Airflow DAGs**.

This project reflects real-world practices used in modern companies.

---

## 🏗️ Architecture

### 🖼️ Architecture Diagram

```
           +-----------------------+
           |        Kaggle         |
           |  (Indian E‑Commerce   |
           |       Dataset)        |
           +-----------+-----------+
                       |
                       |  CSV Files (Manual Upload)
                       v
        +------------------------------------+
        |              AWS S3                |
        |  indian-ecommerce-data/raw/sales/  |
        +-----------------+------------------+
                          |
                          | COPY INTO
                          v
        +------------------------------------+
        |            Snowflake RAW           |
        |       (Landing/Staging Layer)      |
        +-----------------+------------------+
                          |
                          | SQL Transforms
                          v
        +------------------------------------+
        |          Snowflake MODEL           |
        |     (DIM_CUSTOMER, FACT_ORDER…)    |
        +-----------------+------------------+
                          |
                          | Orchestration
                          v
     +------------------------------------------+
     |             Apache Airflow               |
     |   DAG1: S3 → RAW  |  DAG2: RAW → MODEL   |
     +------------------------------------------+
```

---

## 📁 Project Structure

```
├── airflow/
|   ├── config/
|   ├── dags/
|   │   ├── ecommerce_s3_to_snowflake_raw.py        # Loads RAW data into Snowflake
|   │   └── ecommerce_transform_raw_to_model.py     # Transforms RAW → MODEL
|   ├── logs/
|   ├── plugins/
|   ├── .env                                        # add this file explicitly
|   └── docker-compose.yml
├── snowflake/
|   ├── ddl/
|   |  ├── tables/
|   |  |  ├── RAW/                                 # DDL files for RAW schema
|   |  |  ├── MODEL/                               # DDL files for MODEL schema
|   |  |  └── UTIL/                                # DDL files for UTIL schema
|   |  └── views/
└── README.md
```

---

📂 S3 Folder Structure
indian-ecommerce-data/raw/sales/List of Orders[.]csv'
```
s3://<your-bucket-name>indian-ecommerce-data/raw/sales/
├── List of Orders.csv
├── Order Details.csv
└── Sales target.csv
```

---

## 🐳 Docker Setup

The entire environment is containerized using **Docker**.

To start Airflow locally:

```
docker-compose up -d
```

Access the Airflow UI at:

```
http://localhost:8080
```

Use the default Airflow credentials:

```
Username: airflow
Password: airflow
```

---

## 🚀 Airflow DAGs

### 1️⃣ `ecommerce_s3_to_snowflake_raw.py`
* Checks if there is a new file in S3, if yes then proceeds
* Loads CSVs from S3 into Snowflake using `COPY INTO`
* Implemented using **SnowflakeHook cursor execution**

### 2️⃣ `ecommerce_transform_raw_to_model.py`

* Checks if there is any recent load in RAW table, if yes then only proceeds
* Executes DML statements using SnowflakeHook and cursor
* Populates DIM and FACT tables in the MODEL layer

---


## ▶️ How to Run the Project

### 1️⃣ Clone the Repository

```
git clone https://github.com/bijeshkumarch/data-pipelines.git
cd data-pipelines/airflow
```

add a .env file in the directory airflow/ and put the content below or modify accordingly
```
AIRFLOW_UID=50000
AIRFLOW_GID=0
```


### 2️⃣ Start Docker

```
docker-compose up -d
```

This launches the full Airflow environment.

### 3️⃣ Configure Airflow Connections

In Airflow UI → Admin → Connections:

* **snowflake_default** → Add Snowflake account, user, password, warehouse, role

### 4️⃣ Trigger DAGs

In the Airflow UI:

1. Run ecommerce_s3_to_snowflake_raw ( Scheduled for everyday run).
2. Then ecommerce_transform_raw_to_models will be auto triggred on successfull run of 

---


## ❗ Notes

* DDL files are available inside the `snowflake/ddl/` directory and can be executed manually in Snowflake.
* The project follows a **clean separation** of RAW and MODEL layers.
* All transformations are executed using **cursor.execute()** from the Airflow task.
* You need to upload the csv files in S3 with exact same names.
* You need first to populate the utility table TABLE_S3_MAP_MD with correct raw table name and its corresponding S3 object name.
* Also need to populate utility table TRANSFORM_PROCESS_MD only once wich will hold last run of transform dag. We check if there is any load happened in raw tables which is greater than last run of transform dag, then we transform again.

---

## ❗ Assumptions

* Docker is already installed and running on the system.
* Snowflake Storage Integration (for S3) is already created and configured.
* AWS IAM role used in the storage integration has proper S3 access.

---

## 🔄 Pipeline Logic

* The pipeline uses **Snowflake External Storage Integration** to check S3 for file modifications and access S3 file's data.
* RAW loading DAG loads data only if new/updated files are detected in S3.
* The **transformation DAG** (RAW → MODEL) is triggered **only on successful completion** of the RAW loading DAG.
* The **transformation DAG** will perform its action only if there is a new load in raw tables.
* This is a full load logic.

---

## ❗ About The Data
List of Orders-This dataset contains purchase information. The information includes ID, Date of Purchase and customer details
Order Details- This dataset contains order ID, with the order price, quantity,profit, category and subcategory of product
Sales target-This dataset contains sales target amount and date for each product category


## 📊 Future Enhancements

* Add Data Quality checks (e.g., Great Expectations)
* Add BI dashboard using Power BI or Streamlit

---

## 📘 Credits

Dataset: Kaggle – Indian E-Commerce Sales Dataset

---

## 🙌 Author

Project built by **Bijesh**.
