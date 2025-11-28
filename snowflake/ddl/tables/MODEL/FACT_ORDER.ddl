CREATE TABLE ECOMMERCE_DATA_DB.MODEL.FACT_ORDER (
    order_id STRING,
    order_date DATE,
    customer_id INT,
    category_id INT,
    total_amount NUMBER(12,2),
    total_profit NUMBER(12,2),
    total_quantity NUMBER(12,0)
);