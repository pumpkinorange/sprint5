from datetime import datetime
from logging import Logger
from typing import List

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


class RestaurantsStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurant_threshold: int, limit: int) -> List[RestaurantStgObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantStgObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": restaurant_threshold, "limit": limit},
            )
            return cur.fetchall()


class RestaurantsDdsRepository:
    def insert_restaurant(self, conn: Connection, restaurant_id: str, restaurant_name: str,
                          active_from: datetime, active_to: datetime) -> None:
        with conn.cursor() as cur:
            # Закрываем актуальную запись, если имя изменилось
            cur.execute(
                """
                    UPDATE dds.dm_restaurants
                    SET active_to = %(active_from)s
                    WHERE restaurant_id = %(restaurant_id)s::varchar
                      AND active_to = '2099-12-31'::timestamp
                      AND restaurant_name <> %(restaurant_name)s::varchar;
                """,
                {
                    "restaurant_id": restaurant_id,
                    "restaurant_name": restaurant_name,
                    "active_from": active_from,
                },
            )
            # Вставляем новую запись, если актуальной нет
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    SELECT %(restaurant_id)s::varchar, %(restaurant_name)s::varchar,
                           %(active_from)s::timestamp, %(active_to)s::timestamp
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dds.dm_restaurants
                        WHERE restaurant_id = %(restaurant_id)s::varchar
                          AND active_to = '2099-12-31'::timestamp
                    );
                """,
                {
                    "restaurant_id": restaurant_id,
                    "restaurant_name": restaurant_name,
                    "active_from": active_from,
                    "active_to": active_to,
                },
            )


class RestaurantsLoader:
    WF_KEY = "example_restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dwh: PgConnect, logger: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = RestaurantsStgRepository(pg_dwh)
        self.dds = RestaurantsDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = logger

    def load_restaurants(self):
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
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for restaurant in load_queue:
                doc = str2json(restaurant.object_value)
                self.dds.insert_restaurant(
                    conn,
                    restaurant_id=restaurant.object_id,
                    restaurant_name=doc.get("name", ""),
                    active_from=restaurant.update_ts,
                    active_to=datetime(2099, 12, 31),
                )

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([r.id for r in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")