create table if not exists dds.dm_products
(
    id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
    product_id varchar not null,
    product_name varchar not null,
    product_price numeric(14, 2) not null default 0 constraint dm_products_count_check check (product_price >= 0),
    restaurant_id int not null,
    active_from timestamp not null,
    active_to timestamp not null    
);

alter table dds.dm_products
    add constraint dm_products_id_key unique (id);