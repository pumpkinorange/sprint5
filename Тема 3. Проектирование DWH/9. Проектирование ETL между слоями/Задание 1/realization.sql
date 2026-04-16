INSERT INTO cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT
    dr.restaurant_id,
    dr.restaurant_name,
    dt.date AS settlement_date,
    COUNT(DISTINCT do2.id) AS orders_count,
    SUM(fps.total_sum) AS orders_total_sum,
    SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
    SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
    SUM(fps.total_sum) * 0.25 AS order_processing_fee,
    SUM(fps.total_sum) - SUM(fps.total_sum) * 0.25 - SUM(fps.bonus_payment) AS restaurant_reward_sum
FROM dds.fct_product_sales fps
JOIN dds.dm_orders do2 ON do2.id = fps.order_id
JOIN dds.dm_restaurants dr ON dr.id = do2.restaurant_id
JOIN dds.dm_timestamps dt ON dt.id = do2.timestamp_id
WHERE do2.order_status = 'CLOSED'
GROUP BY dr.restaurant_id, dr.restaurant_name, dt.date
ON CONFLICT (settlement_date, restaurant_id) DO UPDATE SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
