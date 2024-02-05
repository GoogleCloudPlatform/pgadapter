START BATCH DDL;

CREATE TABLE IF NOT EXISTS warehouse (
    w_id int not null,
    w_name varchar(10),
    w_street_1 varchar(20),
    w_street_2 varchar(20),
    w_city varchar(20),
    w_state varchar(2),
    w_zip varchar(9),
    w_tax decimal,
    w_ytd decimal,
    primary key (w_id)
);

create table IF NOT EXISTS district (
    w_id int not null,
    d_id int not null,
    d_name varchar(10),
    d_street_1 varchar(20),
    d_street_2 varchar(20),
    d_city varchar(20),
    d_state varchar(2),
    d_zip varchar(9),
    d_tax decimal,
    d_ytd decimal,
    d_next_o_id int,
    primary key (w_id, d_id)
) interleave in parent warehouse;

-- CUSTOMER TABLE

create table IF NOT EXISTS customer (
    w_id int not null,
    d_id int not null,
    c_id int not null,
    c_first varchar(16),
    c_middle varchar(2),
    c_last varchar(16),
    c_street_1 varchar(20),
    c_street_2 varchar(20),
    c_city varchar(20),
    c_state varchar(2),
    c_zip varchar(9),
    c_phone varchar(16),
    c_since timestamptz,
    c_credit varchar(2),
    c_credit_lim bigint,
    c_discount decimal,
    c_balance decimal,
    c_ytd_payment decimal,
    c_payment_cnt int,
    c_delivery_cnt int,
    c_data text,
    PRIMARY KEY(w_id, d_id, c_id)
) interleave in parent district;

-- HISTORY TABLE

create table IF NOT EXISTS history (
    w_id int,
    d_id int,
    c_id int,
    h_d_id int,
    h_w_id int,
    h_date timestamptz,
    h_amount decimal,
    h_data varchar(24),
    PRIMARY KEY(w_id, d_id, c_id, h_d_id, h_w_id, h_date)
) interleave in parent customer;

create table IF NOT EXISTS orders (
    w_id int not null,
    d_id int not null,
    c_id int not null,
    o_id int not null,
    o_entry_d timestamptz,
    o_carrier_id int,
    o_ol_cnt int,
    o_all_local int,
    PRIMARY KEY(w_id, d_id, c_id, o_id)
) interleave in parent customer;

-- NEW_ORDER table

create table IF NOT EXISTS new_orders (
    w_id int not null,
    d_id int not null,
    c_id int not null,
    o_id int not null,
    PRIMARY KEY(w_id, d_id, c_id, o_id)
) interleave in parent orders;

create table IF NOT EXISTS order_line (
    w_id int not null,
    d_id int not null,
    c_id int not null,
    o_id int not null,
    ol_number int not null,
    ol_i_id int,
    ol_supply_w_id int,
    ol_delivery_d timestamptz,
    ol_quantity int,
    ol_amount decimal,
    ol_dist_info varchar(24),
    PRIMARY KEY(w_id, d_id, c_id, o_id, ol_number)
) interleave in parent orders;

-- STOCK table

create table IF NOT EXISTS stock (
    w_id int not null,
    s_i_id int not null,
    s_quantity int,
    s_dist_01 varchar(24),
    s_dist_02 varchar(24),
    s_dist_03 varchar(24),
    s_dist_04 varchar(24),
    s_dist_05 varchar(24),
    s_dist_06 varchar(24),
    s_dist_07 varchar(24),
    s_dist_08 varchar(24),
    s_dist_09 varchar(24),
    s_dist_10 varchar(24),
    s_ytd decimal,
    s_order_cnt int,
    s_remote_cnt int,
    s_data varchar(50),
    PRIMARY KEY(w_id, s_i_id)
) interleave in parent warehouse;

create table IF NOT EXISTS item (
    i_id int not null,
    i_im_id int,
    i_name varchar(24),
    i_price decimal,
    i_data varchar(50),
    PRIMARY KEY(i_id)
);

CREATE INDEX idx_customer ON customer (w_id,d_id,c_last,c_first);

ALTER TABLE history ADD CONSTRAINT fkey_history_2_ FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(w_id,d_id);
ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_2_ FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock(w_id,s_i_id);
ALTER TABLE stock ADD CONSTRAINT fkey_stock_2_ FOREIGN KEY(s_i_id) REFERENCES item(i_id);

RUN BATCH;
