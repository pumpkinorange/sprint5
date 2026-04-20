create table if not exists public.outbox (
    id serial primary key,
    object_id int not null,
    record_ts timestamp not null,
    type varchar(255) not null,
    payload text not null
);