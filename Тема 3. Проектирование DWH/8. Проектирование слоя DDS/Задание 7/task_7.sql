create table if not exists dds.dm_orders
(
    id serial primary key,
    user_id int not null,
    restaurant_id int not null,
    timestamp_id int not null,
    order_key varchar not null,
    order_status varchar not null
);