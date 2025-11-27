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
# Core loader function: S3 (external stage) -> Snowflake RAW tables
# --------------------------------------------------------------------
def load_s3_to_sf(curs, table_name):
    # fetch the last modified timestamp of the s3 object
    logger.info(f"Checking for new files for table {table_name} in S3 stage...")
    s3_object_name, s3_last_processed_ts = curs.execute(f"""SELECT S3_OBJECT_NAME, LAST_MODIFIED_TS FROM ECOMMERCE_DATA_DB.UTIL.TABLE_S3_MAP_MD WHERE TABLE_NAME = '{table_name}';""").fetchone()
    logger.info(f"S3 Object Name: {s3_object_name}, Last Processed Timestamp: {s3_last_processed_ts}")

    # get metadata of all the files in the S3 stage
    curs.execute(f"""LIST '@ECOMMERCE_DATA_DB.RAW.ECOMMERCE_RAW_STAGE/{s3_object_name}';""")

    # get the query id to access the last modified timestamp of the s3 object
    query_id = curs.sfqid

    logger.info(f"Fetching metadata for S3 object {s3_object_name} from stage...")
    file_size, s3_last_modified_ts = curs.execute(f""" SELECT
                                       "size",
                                        CONVERT_TIMEZONE('UTC','Asia/Kolkata', TO_TIMESTAMP_NTZ("last_modified", 'DY, DD MON YYYY HH24:MI:SS "GMT"')) AS s3_last_modified_ts
                                    FROM TABLE(RESULT_SCAN('{query_id}'))
                                    WHERE contains("name", '{s3_object_name}');
                                       """).fetchone()
    logger.info(f"S3 Object: {s3_object_name}, Size (bytes): {file_size}, Last Modified Timestamp: {s3_last_modified_ts}")
    
    # Check if new file is available based on last modified timestamp and file size less than 50MB
    if file_size//1024 <= 50000 and s3_last_modified_ts > s3_last_processed_ts:
        logger.info(f"New file detected for table {table_name}. Starting load process.  S3 Last Modified Timestamp: {s3_last_modified_ts}, Last Processed Timestamp: {s3_last_processed_ts}")
        
        # truncate the table before loading new data
        curs.execute(f"""TRUNCATE ECOMMERCE_DATA_DB.RAW.{table_name};""")
        logger.info(f"Truncated table ECOMMERCE_DATA_DB.RAW.{table_name} before loading new data. Total rows after truncation: {curs.rowcount}")
        
        # load data from s3 to snowflake raw table
        curs.execute(f"""
            COPY INTO ECOMMERCE_DATA_DB.RAW.{table_name}
            FROM @ECOMMERCE_DATA_DB.RAW.ECOMMERCE_RAW_STAGE/
            PATTERN = '.*indian-ecommerce-data/raw/sales/{s3_object_name}[.]csv'
            FILE_FORMAT = ECOMMERCE_DATA_DB.RAW.ECOMMERCE_CSV_FORMAT;
            """)
        
        
        logger.info(f"Data load completed for table {table_name}. Total rows loaded: {curs.fetchone()[2]}")

        # Update the last processed timestamp in the metadata table
        curs.execute(f"""UPDATE ECOMMERCE_DATA_DB.UTIL.TABLE_S3_MAP_MD
                         SET LAST_MODIFIED_TS = '{s3_last_modified_ts}'
                         WHERE TABLE_NAME = '{table_name}';""")
    elif file_size//1024 > 50000:
        logger.warning(f"File size {file_size//1024} KB exceeds the limit of 50000 KB for table {table_name}. Load skipped.")
    else:
        logger.info(f"No new file to process for table {table_name}. Load skipped.")


def execute_program(curs):
    table_names = ["ORDERS", "ORDER_DETAILS", "SALES_TARGET"]
    for table_name in table_names:
        try:
            curs.execute(f"BEGIN;")
            load_s3_to_sf(curs, table_name)
            curs.execute(f"COMMIT;")
        except Exception as e:
            logger.error(f"Error occurred while loading data for table {table_name}: {e}")
            curs.execute(f"ROLLBACK;")


def main():
    logging.basicConfig(level=logging.INFO)
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    cursor = conn.cursor()
    execute_program(cursor)




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
    dag_id="ecommerce_s3_to_snowflake_raw",
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=1),
    schedule="@daily",
    catchup=False,
    description="Load S3 data to Snowflake RAW using COPY INTO",
    tags=["snowflake", "s3", "ecommerce", "raw-load"],
) as dag:

    load_orders_task = PythonOperator(
        task_id="load_raw_from_s3_to_snowflake",
        python_callable=main
    )