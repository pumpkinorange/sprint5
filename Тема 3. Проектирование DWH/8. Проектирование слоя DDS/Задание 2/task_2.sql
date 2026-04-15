create table if not exists dds.dm_users
(
    id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
    user_id varchar not null,
    user_login varchar not null,
    user_name varchar not null
);