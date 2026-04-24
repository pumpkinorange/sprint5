WITH diff AS (
    SELECT a.id AS actual_id, e.id AS expected_id
    FROM public_test.dm_settlement_report_actual   a
    FULL JOIN public_test.dm_settlement_report_expected e
        ON  a.restaurant_id            = e.restaurant_id
        AND a.settlement_year          = e.settlement_year
        AND a.settlement_month         = e.settlement_month
        AND a.orders_count             = e.orders_count
        AND a.orders_total_sum         = e.orders_total_sum
        AND a.orders_bonus_payment_sum = e.orders_bonus_payment_sum
        AND a.orders_bonus_granted_sum = e.orders_bonus_granted_sum
        AND a.order_processing_fee     = e.order_processing_fee
        AND a.restaurant_reward_sum    = e.restaurant_reward_sum
    WHERE a.id IS NULL
       OR e.id IS NULL
),
ins AS (
    INSERT INTO public_test.testing_result (test_name, test_date_time, test_result)
    SELECT
        'test_01',
        current_timestamp,
        (SELECT COUNT(*) = 0 FROM diff)
    RETURNING test_name, test_date_time AT TIME ZONE 'UTC' AS test_date_time, test_result
)
SELECT * FROM ins;
