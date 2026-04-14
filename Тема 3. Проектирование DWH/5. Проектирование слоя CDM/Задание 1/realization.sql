create table if not exists cdm.dm_settlement_report
(
    id serial not null, --идентификатор записи
    restaurant_id varchar(255) not null, -- идентификатор ресторана (строковый, из системы-источника)
    restaurant_name varchar(255) not null, -- название ресторана
    settlement_date date not null, -- дата отчёта
    orders_count integer not null, -- количество заказов
    orders_total_sum numeric(14, 2) not null, -- общая сумма заказов клиентов
    orders_bonus_payment_sum numeric(14, 2) not null, -- сумма оплат бонусами
    orders_bonus_granted_sum numeric(14, 2) not null, -- сумма накопленных бонусов
    order_processing_fee numeric(14, 2) not null, -- сумма, удержанная компанией за обработку заказов
    restaurant_reward_sum numeric(14, 2) not null -- сумма, которую необходимо перечислить ресторану
)