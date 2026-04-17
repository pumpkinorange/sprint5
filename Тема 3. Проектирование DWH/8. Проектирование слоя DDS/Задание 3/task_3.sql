create table if not exists dds.dm_restaurants
(
    id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
    restaurant_id varchar not null,
    restaurant_name varchar not null,
    active_from timestamp not null,
    active_to timestamp not null
);

alter table dds.dm_restaurants
    add constraint dm_restaurants_restaurant_id_key unique (id);