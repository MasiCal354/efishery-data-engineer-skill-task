INSERT INTO "dim_date"
SELECT EXTRACT(JULIAN FROM DATE_TRUNC('day', d)::DATE) AS id,
       DATE_TRUNC('day', d)::DATE AS date,
       EXTRACT('month' FROM DATE_TRUNC('day', d)::DATE) AS month,
       EXTRACT('quarter' FROM DATE_TRUNC('day', d)::DATE) AS quarter_of_year,
       EXTRACT('year' FROM DATE_TRUNC('day', d)::DATE) AS year,
       EXTRACT('isodow' FROM DATE_TRUNC('day', d)::DATE) IN (6, 7) AS is_weekend
FROM GENERATE_SERIES(
    '2000-01-01'::TIMESTAMP,
    '2099-12-31'::TIMESTAMP,
    '1 day'::INTERVAL
) AS d;

INSERT INTO "dim_customer"
SELECT id,
       name
FROM customers;

INSERT INTO "fact_order_accumulating"
SELECT EXTRACT(JULIAN FROM MAX(o.date)) AS order_date_id,
       EXTRACT(JULIAN FROM MAX(i.date)) AS invoice_date_id,
       EXTRACT(JULIAN FROM MAX(p.date)) AS payment_date_id,
       MAX(o.customer_id) AS customer_id,
       o.order_number,
       i.invoice_number,
       p.payment_number,
       SUM(ol.quantity) AS total_order_quantity,
       SUM(ol.usd_amount) AS total_order_usd_amount,
       EXTRACT(DAY FROM MAX(i.date::TIMESTAMP) - MAX(o.date::TIMESTAMP)) AS order_to_invoice_lag_days,
       EXTRACT(DAY FROM MAX(p.date::TIMESTAMP) - MAX(i.date::TIMESTAMP)) AS invoice_to_payment_lag_days
FROM orders AS o
LEFT JOIN order_lines AS ol
    ON ol.order_number = o.order_number
LEFT JOIN invoices AS i
    ON i.order_number = o.order_number
LEFT JOIN payments AS p
    ON p.invoice_number = i.invoice_number
GROUP BY o.order_number,
         i.invoice_number,
         p.payment_number;
