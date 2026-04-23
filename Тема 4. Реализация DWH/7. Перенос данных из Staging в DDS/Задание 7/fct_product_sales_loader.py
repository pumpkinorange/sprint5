from datetime import datetime
from decimal import Decimal
from logging import Logger
from typing import List, Optional

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EventStgObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class FctProductSalesStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, threshold: int, limit: int) -> List[EventStgObj]:
        with self._db.client().cursor(row_factory=class_row(EventStgObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE id > %(threshold)s
                      AND event_type = 'bonus_transaction'
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit},
            )
            return cur.fetchall()


class FctProductSalesDdsRepository:
    def get_order_id(self, conn: Connection, order_key: str) -> Optional[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_orders
                    WHERE order_key = %(order_key)s;
                """,
                {"order_key": order_key},
            )
            row = cur.fetchone()
            return row[0] if row else None

    def get_product_id(self, conn: Connection, product_id: str) -> Optional[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s
                    ORDER BY active_from DESC
                    LIMIT 1;
                """,
                {"product_id": product_id},
            )
            row = cur.fetchone()
            return row[0] if row else None

    def insert_product_sale(self, conn: Connection, product_id: int, order_id: int,
                            count: int, price: Decimal, total_sum: Decimal,
                            bonus_payment: Decimal, bonus_grant: Decimal) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count,
                                                      price, total_sum, bonus_payment, bonus_grant)
                    SELECT %(product_id)s::int, %(order_id)s::int, %(count)s::int,
                           %(price)s::numeric, %(total_sum)s::numeric,
                           %(bonus_payment)s::numeric, %(bonus_grant)s::numeric
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dds.fct_product_sales
                        WHERE product_id = %(product_id)s::int
                          AND order_id = %(order_id)s::int
                    );
                """,
                {
                    "product_id": product_id,
                    "order_id": order_id,
                    "count": count,
                    "price": price,
                    "total_sum": total_sum,
                    "bonus_payment": bonus_payment,
                    "bonus_grant": bonus_grant,
                },
            )


class FctProductSalesLoader:
    WF_KEY = "example_fct_product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dwh: PgConnect, logger: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = FctProductSalesStgRepository(pg_dwh)
        self.dds = FctProductSalesDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = logger

    def load_product_sales(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f"starting to load from last checkpoint: id > {last_loaded_id}")

            load_queue = self.stg.list_events(last_loaded_id, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} bonus_transaction events to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for event in load_queue:
                doc = str2json(event.event_value)

                # Находим заказ в DDS по order_key
                order_key = doc.get("order_id")
                order_id = self.dds.get_order_id(conn, order_key=order_key)
                if order_id is None:
                    self.log.warning(f"Order not found for key={order_key}. Skipping event {event.id}.")
                    continue

                # Обрабатываем каждую позицию заказа
                product_payments = doc.get("product_payments", [])
                for item in product_payments:
                    product_obj_id = str(item.get("product_id", ""))
                    product_id = self.dds.get_product_id(conn, product_id=product_obj_id)
                    if product_id is None:
                        self.log.warning(
                            f"Product not found for id={product_obj_id}. Skipping item in event {event.id}."
                        )
                        continue

                    count = int(item.get("quantity", 0))
                    price = Decimal(str(item.get("price", 0)))
                    total_sum = Decimal(str(item.get("product_cost", 0)))
                    bonus_payment = Decimal(str(item.get("bonus_payment", 0)))
                    bonus_grant = Decimal(str(item.get("bonus_grant", 0)))

                    self.dds.insert_product_sale(
                        conn,
                        product_id=product_id,
                        order_id=order_id,
                        count=count,
                        price=price,
                        total_sum=total_sum,
                        bonus_payment=bonus_payment,
                        bonus_grant=bonus_grant,
                    )

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([e.id for e in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")
