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


class RestaurantStgObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class ProductsStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, threshold: int, limit: int) -> List[RestaurantStgObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantStgObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": threshold, "limit": limit},
            )
            return cur.fetchall()


class ProductsDdsRepository:
    def get_restaurant_version_id(self, conn: Connection, restaurant_id: str,
                                  update_ts: datetime) -> Optional[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s
                      AND active_from <= %(update_ts)s
                      AND active_to > %(update_ts)s;
                """,
                {"restaurant_id": restaurant_id, "update_ts": update_ts},
            )
            row = cur.fetchone()
            return row[0] if row else None

    def insert_product(self, conn: Connection, restaurant_id: int, product_id: str,
                       product_name: str, product_price: Decimal,
                       active_from: datetime, active_to: datetime) -> None:
        with conn.cursor() as cur:
            # Закрываем актуальную запись, если цена или название изменились
            cur.execute(
                """
                    UPDATE dds.dm_products
                    SET active_to = %(active_from)s::timestamp
                    WHERE product_id = %(product_id)s::varchar
                      AND active_to = '2099-12-31'::timestamp
                      AND (product_name <> %(product_name)s::text OR product_price <> %(product_price)s::numeric);
                """,
                {
                    "product_id": product_id,
                    "product_name": product_name,
                    "product_price": product_price,
                    "active_from": active_from,
                },
            )
            # Вставляем новую запись, если актуальной нет
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name,
                                               product_price, active_from, active_to)
                    SELECT %(restaurant_id)s::int, %(product_id)s::varchar, %(product_name)s::text,
                           %(product_price)s::numeric, %(active_from)s::timestamp, %(active_to)s::timestamp
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dds.dm_products
                        WHERE product_id = %(product_id)s::varchar
                          AND active_to = '2099-12-31'::timestamp
                    );
                """,
                {
                    "restaurant_id": restaurant_id,
                    "product_id": product_id,
                    "product_name": product_name,
                    "product_price": product_price,
                    "active_from": active_from,
                    "active_to": active_to,
                },
            )


class ProductsLoader:
    WF_KEY = "example_products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dwh: PgConnect, logger: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = ProductsStgRepository(pg_dwh)
        self.dds = ProductsDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = logger

    def load_products(self):
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

            load_queue = self.stg.list_restaurants(last_loaded_id, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load products from.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for restaurant in load_queue:
                doc = str2json(restaurant.object_value)

                # Находим версию ресторана в DDS по restaurant_id и update_ts
                restaurant_version_id = self.dds.get_restaurant_version_id(
                    conn,
                    restaurant_id=restaurant.object_id,
                    update_ts=restaurant.update_ts,
                )
                if restaurant_version_id is None:
                    self.log.warning(
                        f"Restaurant version not found for id={restaurant.object_id}, "
                        f"update_ts={restaurant.update_ts}. Skipping."
                    )
                    continue

                # Извлекаем меню ресторана и вставляем продукты
                menu = doc.get("menu", [])
                for item in menu:
                    self.dds.insert_product(
                        conn,
                        restaurant_id=restaurant_version_id,
                        product_id=str(item.get("_id", "")),
                        product_name=item.get("name", ""),
                        product_price=Decimal(str(item.get("price", 0))),
                        active_from=restaurant.update_ts,
                        active_to=datetime(2099, 12, 31),
                    )

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([r.id for r in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")
