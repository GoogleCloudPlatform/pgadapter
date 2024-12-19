SET AUTOCOMMIT = FALSE;
START BATCH DDL;

CREATE TABLE IF NOT EXISTS warehouse (
    w_id INT64 not null,
    w_name STRING(10),
    w_street_1 STRING(20),
    w_street_2 STRING(20),
    w_city STRING(20),
    w_state STRING(2),
    w_zip STRING(9),
    w_tax NUMERIC,
    w_ytd NUMERIC,
) primary key (w_id);

create table IF NOT EXISTS district (
    d_id INT64 not null,
    w_id INT64 not null,
    d_name STRING(10),
    d_street_1 STRING(20),
    d_street_2 STRING(20),
    d_city STRING(20),
    d_state STRING(2),
    d_zip STRING(9),
    d_tax NUMERIC,
    d_ytd NUMERIC,
    d_next_o_id INT64,
) primary key (w_id, d_id);

-- CUSTOMER TABLE

create table IF NOT EXISTS customer (
    c_id INT64 not null,
    d_id INT64 not null,
    w_id INT64 not null,
    c_first STRING(16),
    c_middle STRING(2),
    c_last STRING(16),
    c_street_1 STRING(20),
    c_street_2 STRING(20),
    c_city STRING(20),
    c_state STRING(2),
    c_zip STRING(9),
    c_phone STRING(16),
    c_since TIMESTAMP,
    c_credit STRING(2),
    c_credit_lim INT64,
    c_discount NUMERIC,
    c_balance NUMERIC,
    c_ytd_payment NUMERIC,
    c_payment_cnt INT64,
    c_delivery_cnt INT64,
    c_data STRING(MAX),
) PRIMARY KEY(w_id, d_id, c_id);

-- HISTORY TABLE

create table IF NOT EXISTS history (
    c_id INT64,
    d_id INT64,
    w_id INT64,
    h_d_id INT64,
    h_w_id INT64,
    h_date TIMESTAMP,
    h_amount NUMERIC,
    h_data STRING(24),
) PRIMARY KEY(c_id, d_id, w_id, h_d_id, h_w_id, h_date);

create table IF NOT EXISTS orders (
    o_id INT64 not null,
    d_id INT64 not null,
    w_id INT64 not null,
    c_id INT64 not null,
    o_entry_d TIMESTAMP,
    o_carrier_id INT64,
    o_ol_cnt INT64,
    o_all_local INT64,
) PRIMARY KEY(w_id, d_id, c_id, o_id);

-- NEW_ORDER table

create table IF NOT EXISTS new_orders (
    o_id INT64 not null,
    c_id INT64 not null,
    d_id INT64 not null,
    w_id INT64 not null,
) PRIMARY KEY(w_id, d_id, c_id, o_id);

create table IF NOT EXISTS order_line (
    o_id INT64 not null,
    c_id INT64 not null,
    d_id INT64 not null,
    w_id INT64 not null,
    ol_number INT64 not null,
    ol_i_id INT64,
    ol_supply_w_id INT64,
    ol_delivery_d TIMESTAMP,
    ol_quantity INT64,
    ol_amount NUMERIC,
    ol_dist_info STRING(24),
) PRIMARY KEY(w_id, d_id, c_id, o_id, ol_number);

-- STOCK table

create table IF NOT EXISTS stock (
    s_i_id INT64 not null,
    w_id INT64 not null,
    s_quantity INT64,
    s_dist_01 STRING(24),
    s_dist_02 STRING(24),
    s_dist_03 STRING(24),
    s_dist_04 STRING(24),
    s_dist_05 STRING(24),
    s_dist_06 STRING(24),
    s_dist_07 STRING(24),
    s_dist_08 STRING(24),
    s_dist_09 STRING(24),
    s_dist_10 STRING(24),
    s_ytd NUMERIC,
    s_order_cnt INT64,
    s_remote_cnt INT64,
    s_data STRING(50),
) PRIMARY KEY(w_id, s_i_id);

create table IF NOT EXISTS item (
    i_id INT64 not null,
    i_im_id INT64,
    i_name STRING(24),
    i_price NUMERIC,
    i_data STRING(50),
) PRIMARY KEY(i_id);

CREATE INDEX idx_customer ON customer (w_id,d_id,c_last,c_first);
CREATE INDEX idx_orders ON orders (w_id,d_id,c_id,o_id);
CREATE INDEX fkey_stock_2 ON stock (s_i_id);
CREATE INDEX fkey_order_line_2 ON order_line (ol_supply_w_id,ol_i_id);
CREATE INDEX fkey_history_1 ON history (w_id,d_id,c_id);
CREATE INDEX fkey_history_2 ON history (h_w_id,h_d_id );

ALTER TABLE new_orders ADD CONSTRAINT fkey_new_orders_1_ FOREIGN KEY(w_id,d_id,c_id,o_id) REFERENCES orders(w_id,d_id,c_id,o_id);
ALTER TABLE orders ADD CONSTRAINT fkey_orders_1_ FOREIGN KEY(w_id,d_id,c_id) REFERENCES customer(w_id,d_id,c_id);
ALTER TABLE customer ADD CONSTRAINT fkey_customer_1_ FOREIGN KEY(w_id,d_id) REFERENCES district(w_id,d_id);
ALTER TABLE history ADD CONSTRAINT fkey_history_1_ FOREIGN KEY(w_id,d_id,c_id) REFERENCES customer(w_id,d_id,c_id);
ALTER TABLE history ADD CONSTRAINT fkey_history_2_ FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(w_id,d_id);
ALTER TABLE district ADD CONSTRAINT fkey_district_1_ FOREIGN KEY(w_id) REFERENCES warehouse(w_id);
ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_1_ FOREIGN KEY(w_id,d_id,c_id,o_id) REFERENCES orders(w_id,d_id,c_id,o_id);
ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_2_ FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock(w_id,s_i_id);
ALTER TABLE stock ADD CONSTRAINT fkey_stock_1_ FOREIGN KEY(w_id) REFERENCES warehouse(w_id);
ALTER TABLE stock ADD CONSTRAINT fkey_stock_2_ FOREIGN KEY(s_i_id) REFERENCES item(i_id);

RUN BATCH;
