start batch ddl;

drop index if exists fkey_history_2;
drop index if exists fkey_history_1;
drop index if exists fkey_order_line_2;
drop index if exists fkey_stock_2;
drop index if exists idx_orders;
drop index if exists idx_customer;

ALTER TABLE new_orders DROP CONSTRAINT fkey_new_orders_1_;
ALTER TABLE orders DROP CONSTRAINT fkey_orders_1_;
ALTER TABLE customer DROP CONSTRAINT fkey_customer_1_;
ALTER TABLE history DROP CONSTRAINT fkey_history_1_;
ALTER TABLE history DROP CONSTRAINT fkey_history_2_;
ALTER TABLE district DROP CONSTRAINT fkey_district_1_;
ALTER TABLE order_line DROP CONSTRAINT fkey_order_line_1_;
ALTER TABLE order_line DROP CONSTRAINT fkey_order_line_2_;
ALTER TABLE stock DROP CONSTRAINT fkey_stock_1_;
ALTER TABLE stock DROP CONSTRAINT fkey_stock_2_;

drop table if exists new_orders;
drop table if exists order_line;
drop table if exists history;
drop table if exists orders;
drop table if exists stock;
drop table if exists customer;
drop table if exists district;
drop table if exists warehouse;
drop table if exists item;

run batch;
