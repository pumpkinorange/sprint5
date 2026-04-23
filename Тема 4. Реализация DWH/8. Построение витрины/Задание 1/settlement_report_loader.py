from logging import Logger

from lib import PgConnect
from psycopg import Connection


class SettlementReportLoader:
    def __init__(self, pg_dwh: PgConnect, logger: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.log = logger

    def load_report(self) -> None:
        with self.pg_dwh.connection() as conn:
            self._upsert_settlement_report(conn)
            self.log.info("dm_settlement_report updated successfully.")

    def _upsert_settlement_report(self, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_settlement_report(
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
                    dt.date                                  AS settlement_date,
                    COUNT(DISTINCT dmo.id)                   AS orders_count,
                    SUM(fps.total_sum)                       AS orders_total_sum,
                    SUM(fps.bonus_payment)                   AS orders_bonus_payment_sum,
                    SUM(fps.bonus_grant)                     AS orders_bonus_granted_sum,
                    SUM(fps.total_sum) * 0.25                AS order_processing_fee,
                    SUM(fps.total_sum)
                        - SUM(fps.bonus_payment)
                        - SUM(fps.total_sum) * 0.25          AS restaurant_reward_sum
                FROM dds.fct_product_sales fps
                JOIN dds.dm_orders         dmo ON dmo.id           = fps.order_id
                JOIN dds.dm_restaurants    dr  ON dr.id            = dmo.restaurant_id
                JOIN dds.dm_timestamps     dt  ON dt.id            = dmo.timestamp_id
                WHERE dmo.order_status = 'CLOSED'
                GROUP BY
                    dr.restaurant_id,
                    dr.restaurant_name,
                    dt.date
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                SET
                    restaurant_name          = EXCLUDED.restaurant_name,
                    orders_count             = EXCLUDED.orders_count,
                    orders_total_sum         = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee     = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum    = EXCLUDED.restaurant_reward_sum;
                """
            )