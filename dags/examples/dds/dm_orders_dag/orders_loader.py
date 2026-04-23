from datetime import datetime, timezone
from logging import Logger
from typing import List, Optional

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderStgObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class OrdersStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, threshold: int, limit: int) -> List[OrderStgObj]:
        with self._db.client().cursor(row_factory=class_row(OrderStgObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit},
            )
            return cur.fetchall()


class OrdersDdsRepository:
    def get_restaurant_version_id(self, conn: Connection, restaurant_id: str) -> Optional[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s
                    ORDER BY active_from DESC
                    LIMIT 1;
                """,
                {"restaurant_id": restaurant_id},
            )
            row = cur.fetchone()
            return row[0] if row else None

    def get_user_id(self, conn: Connection, user_id: str) -> Optional[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_users
                    WHERE user_id = %(user_id)s;
                """,
                {"user_id": user_id},
            )
            row = cur.fetchone()
            return row[0] if row else None

    def get_timestamp_id(self, conn: Connection, ts: datetime) -> Optional[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_timestamps
                    WHERE ts = %(ts)s;
                """,
                {"ts": ts},
            )
            row = cur.fetchone()
            return row[0] if row else None

    def insert_order(self, conn: Connection, order_key: str, order_status: str,
                     restaurant_id: int, timestamp_id: int, user_id: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id,
                                             timestamp_id, user_id)
                    SELECT %(order_key)s::varchar, %(order_status)s::varchar,
                           %(restaurant_id)s::int, %(timestamp_id)s::int, %(user_id)s::int
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dds.dm_orders
                        WHERE order_key = %(order_key)s::varchar
                    );
                """,
                {
                    "order_key": order_key,
                    "order_status": order_status,
                    "restaurant_id": restaurant_id,
                    "timestamp_id": timestamp_id,
                    "user_id": user_id,
                },
            )


class OrdersLoader:
    WF_KEY = "example_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dwh: PgConnect, logger: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = OrdersStgRepository(pg_dwh)
        self.dds = OrdersDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = logger

    def load_orders(self):
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

            load_queue = self.stg.list_orders(last_loaded_id, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order in load_queue:
                doc = str2json(order.object_value)

                # Обрабатываем только заказы с финальным статусом
                final_status = doc.get("final_status")
                if final_status not in ("CLOSED", "CANCELLED"):
                    continue

                # Парсим дату заказа (UTC)
                date_str = doc.get("date")
                if not date_str:
                    continue
                ts = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)

                # Находим версию ресторана
                restaurant_obj_id = doc["restaurant"]["id"]
                restaurant_version_id = self.dds.get_restaurant_version_id(
                    conn,
                    restaurant_id=restaurant_obj_id,
                )
                if restaurant_version_id is None:
                    self.log.warning(
                        f"Restaurant version not found for id={restaurant_obj_id}. Skipping order {order.object_id}."
                    )
                    continue

                # Находим пользователя
                user_obj_id = doc["user"]["id"]
                user_id = self.dds.get_user_id(conn, user_id=user_obj_id)
                if user_id is None:
                    self.log.warning(
                        f"User not found for id={user_obj_id}. Skipping order {order.object_id}."
                    )
                    continue

                # Находим временную метку
                timestamp_id = self.dds.get_timestamp_id(conn, ts=ts)
                if timestamp_id is None:
                    self.log.warning(
                        f"Timestamp not found for ts={ts}. Skipping order {order.object_id}."
                    )
                    continue

                self.dds.insert_order(
                    conn,
                    order_key=order.object_id,
                    order_status=final_status,
                    restaurant_id=restaurant_version_id,
                    timestamp_id=timestamp_id,
                    user_id=user_id,
                )

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([o.id for o in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")
