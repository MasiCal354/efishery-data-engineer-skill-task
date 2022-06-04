CREATE TABLE IF NOT EXISTS "fact_order_accumulating" (
    "order_date_id" INT,
    "invoice_date_id" INT,
    "payment_date_id" INT,
    "customer_id" INT,
    "order_number" VARCHAR PRIMARY KEY,
    "invoice_number" VARCHAR,
    "payment_number" VARCHAR,
    "total_order_quantity" INT,
    "total_order_usd_amount" DECIMAL,
    "order_to_invoice_lag_days" INT,
    "invoice_to_payment_lag_days" INT
);

CREATE TABLE IF NOT EXISTS "dim_customer" (
    "id" INT PRIMARY KEY,
    "name" VARCHAR
);

CREATE TABLE IF NOT EXISTS "dim_date" (
    "id" INT PRIMARY KEY,
    "date" DATE,
    "month" INT,
    "quarter_of_year" INT,
    "year" INT,
    "is_weekend" BOOLEAN
);
