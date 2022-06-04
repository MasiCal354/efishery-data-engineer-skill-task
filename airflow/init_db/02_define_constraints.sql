ALTER TABLE "orders" ADD FOREIGN KEY ("customer_id") REFERENCES "customers" ("id");
ALTER TABLE "order_lines" ADD FOREIGN KEY ("order_number") REFERENCES "orders" ("order_number");
ALTER TABLE "order_lines" ADD FOREIGN KEY ("product_id") REFERENCES "products" ("id");
ALTER TABLE "invoices" ADD FOREIGN KEY ("order_number") REFERENCES "orders" ("order_number");
ALTER TABLE "payments" ADD FOREIGN KEY ("invoice_number") REFERENCES "invoices" ("invoice_number");
