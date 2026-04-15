create table if not exists dds.fct_product_sales
(
    id serial primary key,
    product_id int not null,
    order_id int not null,
    count int not null default 0 constraint fct_product_sales_count_check check (count >= 0),
    price numeric(14, 2) not null default 0 constraint fct_product_sales_price_check check (price >= 0),
    total_sum numeric(14, 2) not null default 0 constraint fct_product_sales_total_sum_check check (total_sum >= 0),
    bonus_payment numeric(14, 2) not null default 0 constraint fct_product_sales_bonus_payment_check check (bonus_payment >= 0),
    bonus_grant numeric(14, 2) not null default 0 constraint fct_product_sales_bonus_grant_check check (bonus_grant >= 0)
);