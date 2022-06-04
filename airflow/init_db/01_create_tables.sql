CREATE TABLE IF NOT EXISTS "products" (
  "id" int PRIMARY KEY,
  "name" varchar
);

CREATE TABLE IF NOT EXISTS "customers" (
  "id" int PRIMARY KEY,
  "name" varchar
);

CREATE TABLE IF NOT EXISTS "orders" (
  "order_number" varchar PRIMARY KEY,
  "customer_id" int,
  "date" date
);

CREATE TABLE IF NOT EXISTS "order_lines" (
  "order_line_number" varchar PRIMARY KEY,
  "order_number" varchar,
  "product_id" int,
  "quantity" int,
  "usd_amount" decimal
);

CREATE TABLE IF NOT EXISTS "invoices" (
  "invoice_number" varchar PRIMARY KEY,
  "order_number" varchar,
  "date" date
);

CREATE TABLE IF NOT EXISTS "payments" (
  "payment_number" varchar PRIMARY KEY,
  "invoice_number" varchar,
  "date" date
);
