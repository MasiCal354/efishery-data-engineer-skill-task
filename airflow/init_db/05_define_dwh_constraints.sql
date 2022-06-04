ALTER TABLE "fact_order_accumulating" ADD FOREIGN KEY ("order_date_id") REFERENCES "dim_date" ("id");
ALTER TABLE "fact_order_accumulating" ADD FOREIGN KEY ("invoice_date_id") REFERENCES "dim_date" ("id");
ALTER TABLE "fact_order_accumulating" ADD FOREIGN KEY ("payment_date_id") REFERENCES "dim_date" ("id");
ALTER TABLE "fact_order_accumulating" ADD FOREIGN KEY ("customer_id") REFERENCES "dim_customer" ("id");
