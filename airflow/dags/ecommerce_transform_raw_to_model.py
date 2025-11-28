import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook



# --------------------------------------------------------------------
# Config (change these as per your environment)
# --------------------------------------------------------------------
SNOWFLAKE_CONN_ID = "snowflake_default"

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------
# Core transformer function: Snowflake RAW tables -> Snowflake MODEL tables
# --------------------------------------------------------------------
def transform_raw_to_models(curs):
    # fetch last run timestamp of the transformation process
    logger.info("fetching last run timestamp of the transformation process...")
    last_run_ts = curs.execute(f"""SELECT MAX(LAST_RUN_TS) FROM ECOMMERCE_DATA_DB.UTIL.TRANSFORM_PROCESS_MD;""").fetchone()[0]
    logger.info(f"Last Run Timestamp: {last_run_ts}")

    # fetch last load timestamp of the raw tables
    logger.info("fetching last load timestamp of the raw tables...")
    raw_load_ts = curs.execute(f"""SELECT MAX(LAST_MODIFIED_TS) FROM ECOMMERCE_DATA_DB.UTIL.TABLE_S3_MAP_MD;""").fetchone()[0]
    logger.info(f"Raw Tables Last Load Timestamp: {raw_load_ts}")

    # Check if new data is available in raw tables based on last load timestamp
    if raw_load_ts > last_run_ts:
        logger.info("New data detected in RAW tables. Starting transformation process...")

        # truncating the tables before loading new data
        logger.info(f"Truncating table ECOMMERCE_DATA_DB.MODEL.DIM_DATE")
        curs.execute(f"""TRUNCATE ECOMMERCE_DATA_DB.MODEL.DIM_DATE;""")
        logger.info(f"Truncated table ECOMMERCE_DATA_DB.MODEL.DIM_DATE.")

        logger.info(f"Truncating table ECOMMERCE_DATA_DB.MODEL.DIM_CUSTOMER")
        curs.execute(f"""TRUNCATE ECOMMERCE_DATA_DB.MODEL.DIM_CUSTOMER;""")
        logger.info(f"Truncated table ECOMMERCE_DATA_DB.MODEL.DIM_CUSTOMER.")

        logger.info(f"Truncating table ECOMMERCE_DATA_DB.MODEL.DIM_CATEGORY")
        curs.execute(f"""TRUNCATE ECOMMERCE_DATA_DB.MODEL.DIM_CATEGORY;""")
        logger.info(f"Truncated table ECOMMERCE_DATA_DB.MODEL.DIM_CATEGORY.")

        logger.info(f"Truncating table ECOMMERCE_DATA_DB.MODEL.FACT_ORDER")
        curs.execute(f"""TRUNCATE ECOMMERCE_DATA_DB.MODEL.FACT_ORDER;""")
        logger.info(f"Truncated table ECOMMERCE_DATA_DB.MODEL.FACT_ORDER.")

        logger.info(f"Truncating table ECOMMERCE_DATA_DB.MODEL.FACT_SALES_TARGET")
        curs.execute(f"""TRUNCATE ECOMMERCE_DATA_DB.MODEL.FACT_SALES_TARGET;""")
        logger.info(f"Truncated table ECOMMERCE_DATA_DB.MODEL.FACT_SALES_TARGET.")

        # insert into DIM_DATE
        logger.info(f"Inserting data into ECOMMERCE_DATA_DB.MODEL.DIM_DATE")
        curs.execute(f"""
            INSERT INTO ECOMMERCE_DATA_DB.MODEL.DIM_DATE
                (date, year, month, month_name, quarter, year_month )
                SELECT
                    order_date AS date,
                    YEAR(order_date) AS year,
                    MONTH(order_date) AS month,
                    TO_CHAR(order_date, 'Mon') AS month_name,
                    QUARTER(order_date) AS quarter,
                    TO_CHAR(order_date, 'YYYY-MM') AS year_month
                FROM (
                    SELECT DISTINCT order_date
                    FROM ECOMMERCE_DATA_DB.RAW.ORDERS
                )
                ORDER BY order_date;
                    """)
        logger.info(f"Inserted data into ECOMMERCE_DATA_DB.MODEL.DIM_DATE. Total rows inserted: {curs.rowcount}")

        # insert into DIM_CUSTOMER
        logger.info(f"Inserting data into ECOMMERCE_DATA_DB.MODEL.DIM_CUSTOMER")
        curs.execute(f"""
                INSERT INTO ECOMMERCE_DATA_DB.MODEL.DIM_CUSTOMER
                    (customer_id, customer_name, state, city)
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY customer_name, state, city) AS customer_id,
                        customer_name,
                        state,
                        city
                    FROM (
                        SELECT DISTINCT customer_name, state, city
                        FROM ECOMMERCE_DATA_DB.RAW.ORDERS
                    );
                    """)
        logger.info(f"Inserted data into ECOMMERCE_DATA_DB.MODEL.DIM_CUSTOMER. Total rows inserted: {curs.rowcount}")

        # insert into DIM_CATEGORY
        logger.info(f"Inserting data into ECOMMERCE_DATA_DB.MODEL.DIM_CATEGORY")
        curs.execute(f"""
                INSERT INTO ECOMMERCE_DATA_DB.MODEL.DIM_CATEGORY
                    (category_id, Category, Sub_Category)
                    SELECT
                    ROW_NUMBER() OVER (ORDER BY Category, Sub_Category) AS category_id,
                    Category,
                    Sub_Category
                    FROM (
                    SELECT DISTINCT Category, Sub_Category
                    FROM ECOMMERCE_DATA_DB.RAW.ORDER_DETAILS
                    );
                    """)
        logger.info(f"Inserted data into ECOMMERCE_DATA_DB.MODEL.DIM_CATEGORY. Total rows inserted: {curs.rowcount}")

        # insert into FACT_ORDER
        logger.info(f"Inserting data into ECOMMERCE_DATA_DB.MODEL.FACT_ORDER")
        curs.execute(f"""
                INSERT INTO ECOMMERCE_DATA_DB.MODEL.FACT_ORDER
                    (
                        order_id,
                        order_date,
                        customer_id,
                        category_id,
                        total_amount,
                        total_profit,
                        total_quantity
                    )
                    WITH ALL_ORDERS AS (
                        SELECT
                            o.order_id,
                            o.order_date,
                            o.customer_name,
                            o.state,
                            o.city,
                            od.Amount,
                            od.Profit,
                            od.Quantity,
                            od.Category,
                            od.Sub_Category
                        FROM ECOMMERCE_DATA_DB.RAW.ORDERS o
                        JOIN ECOMMERCE_DATA_DB.RAW.ORDER_DETAILS od
                        ON o.order_id = od.order_id
                    ),
                    FINAL_CTE AS (
                        SELECT
                            ao.order_id,
                            d.date AS order_date,
                            c.customer_id,
                            cat.category_id,
                            ao.Amount,
                            ao.Profit,
                            ao.Quantity
                        FROM ALL_ORDERS as ao
                        LEFT JOIN ECOMMERCE_DATA_DB.MODEL.DIM_DATE d
                            ON d.date = ao.order_date
                        LEFT JOIN ECOMMERCE_DATA_DB.MODEL.DIM_CUSTOMER c
                            ON c.customer_name = ao.customer_name
                            AND c.state = ao.state
                            AND c.city  = ao.city
                        LEFT JOIN ECOMMERCE_DATA_DB.MODEL.DIM_CATEGORY cat
                            ON cat.Category = ao.Category
                            AND cat.Sub_Category = ao.Sub_Category
                    )
                    SELECT
                        order_id,
                        order_date,
                        customer_id,
                        category_id,
                        SUM(Amount) AS total_amount,
                        SUM(Profit) AS total_profit,
                        SUM(Quantity) AS total_quantity
                    FROM FINAL_CTE
                    GROUP BY
                        order_id, order_date, customer_id, category_id;
                    """)
        logger.info(f"Inserted data into ECOMMERCE_DATA_DB.MODEL.FACT_ORDER. Total rows inserted: {curs.rowcount}")

        # insert into FACT_SALES_TARGET
        logger.info(f"Inserting data into ECOMMERCE_DATA_DB.MODEL.FACT_SALES_TARGET")
        curs.execute(f"""
                INSERT INTO ECOMMERCE_DATA_DB.MODEL.FACT_SALES_TARGET
                    (   year,
                        month,
                        year_month,
                        Category,
                        Target
                    )
                    WITH norm AS (
                        SELECT
                            Month_of_Order_Date,
                            Category,
                            Target,
                            TRY_TO_DATE(Month_of_Order_Date || '-01', 'YYYY-MON-DD') AS dt
                        FROM ECOMMERCE_DATA_DB.RAW.SALES_TARGET
                    ),
                    lkp AS (
                        SELECT
                            n.Month_of_Order_Date,
                            n.Category,
                            n.Target,
                            d.year,
                            d.month,
                            d.year_month
                        FROM norm n
                        LEFT JOIN ECOMMERCE_DATA_DB.MODEL.DIM_DATE d
                        ON d.year_month = TO_CHAR(n.dt, 'YYYY-MM')
                    )
                    SELECT
                        year,
                        month,
                        year_month,
                        Category,
                        Target
                    FROM lkp;
                    """)
        logger.info(f"Inserted data into ECOMMERCE_DATA_DB.MODEL.FACT_SALES_TARGET. Total rows inserted: {curs.rowcount}")
        
        logger.info("Data transformation from RAW to MODEL completed successfully.")

        # Update the last run timestamp in the transformation metadata table
        logger.info("Updating the last run timestamp in the transformation metadata table...")
        curs.execute(f"""UPDATE ECOMMERCE_DATA_DB.UTIL.TRANSFORM_PROCESS_MD
                            SET LAST_RUN_TS = CURRENT_TIMESTAMP();""")
        logger.info("Updated the last run timestamp in the transformation metadata table.")
        logger.info("Transformation process completed successfully.")
    else:
        logger.info("No new data to process in RAW tables. Transformation skipped.")


def execute_program(curs):
    try:
        curs.execute(f"BEGIN;")
        transform_raw_to_models(curs)
        curs.execute(f"COMMIT;")
    except Exception as e:
        logger.error(f"Error occurred while running the proc: {e}")
        curs.execute(f"ROLLBACK;")


def main():
    logging.basicConfig(level=logging.INFO)
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    cursor = conn.cursor()
    execute_program(cursor)
    cursor.close()
    conn.close()




# --------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------

# DAG Default Arguments
default_args = {
    "owner": "bijesh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG Definition
with DAG(
    dag_id="ecommerce_transform_raw_to_models",
    default_args=default_args,
    # start_date=datetime.now() - timedelta(days=1),
    # schedule="@daily",
    catchup=False,
    description="Load S3 data to Snowflake RAW using COPY INTO",
    tags=["snowflake", "transform", "ecommerce", "raw-to-model", "process"],
) as dag:


    transform_raws_task = PythonOperator(
        task_id="ecommerce_transform_raw_to_model",
        python_callable=main
    )
