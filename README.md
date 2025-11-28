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

This project reflects real-world practices used in product-based companies.

---

## 🏗️ Architecture

```
S3 (raw zone)
    ↓ COPY INTO (Snowflake)
Snowflake RAW Schema
    ↓ Transform using SQL executed via SnowflakeHook
Snowflake MODEL Schema (DIM + FACT tables)
    ↓
Airflow orchestrates the flow end-to-end
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
cd data-pipelines
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

---

## ❗ Assumptions

* Docker is already installed and running on the system.
* Snowflake Storage Integration (for S3) is already created and configured.
* AWS IAM role used in the storage integration has proper S3 access.

---


## 📊 Future Enhancements

* Add Data Quality checks (e.g., Great Expectations)
* Add BI dashboard using Power BI or Streamlit

---

## 📘 Credits

Dataset: Kaggle – Indian E-Commerce Sales Dataset

---

## 🙌 Author

Project built by **Bijesh**.
