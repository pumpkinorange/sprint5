from datetime import datetime, timezone
from logging import Logger
from typing import List

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


class TimestampsStgRepository:
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


class TimestampsDdsRepository:
    def insert_timestamp(self, conn: Connection, ts: datetime) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    SELECT %(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dds.dm_timestamps WHERE ts = %(ts)s
                    );
                """,
                {
                    "ts": ts,
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "date": ts.date(),
                    "time": ts.time(),
                },
            )


class TimestampsLoader:
    WF_KEY = "example_timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dwh: PgConnect, logger: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = TimestampsStgRepository(pg_dwh)
        self.dds = TimestampsDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = logger

    def load_timestamps(self):
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

                # Обрабатываем только заказы с финальным статусом CLOSED или CANCELLED
                final_status = doc.get("final_status")
                if final_status not in ("CLOSED", "CANCELLED"):
                    continue

                # Парсим дату из JSON и приводим к UTC-aware datetime
                date_str = doc.get("date")
                if not date_str:
                    continue

                ts = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)

                self.dds.insert_timestamp(conn, ts)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([o.id for o in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")