import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

log = logging.getLogger(__name__)


# -------- Ranks --------

class RankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float


class RanksOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ranks(self) -> List[RankObj]:
        with self._db.client().cursor(row_factory=class_row(RankObj)) as cur:
            cur.execute(
                """
                    SELECT id, name, bonus_percent, min_payment_threshold
                    FROM ranks
                    ORDER BY id ASC;
                """
            )
            return cur.fetchall()


class RankDestRepository:

    def insert_rank(self, conn: Connection, rank: RankObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                    VALUES (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        bonus_percent = EXCLUDED.bonus_percent,
                        min_payment_threshold = EXCLUDED.min_payment_threshold;
                """,
                {
                    "id": rank.id,
                    "name": rank.name,
                    "bonus_percent": rank.bonus_percent,
                    "min_payment_threshold": rank.min_payment_threshold
                },
            )


class RankLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log) -> None:
        self.pg_dest = pg_dest
        self.origin = RanksOriginRepository(pg_origin)
        self.stg = RankDestRepository()
        self.log = log

    def load_ranks(self):
        with self.pg_dest.connection() as conn:
            load_queue = self.origin.list_ranks()
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            for rank in load_queue:
                self.stg.insert_rank(conn, rank)
            self.log.info(f"Loaded {len(load_queue)} ranks.")


# -------- Users --------

class UserObj(BaseModel):
    id: int
    order_user_id: str


class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_user_id
                    FROM users
                    ORDER BY id ASC;
                """
            )
            return cur.fetchall()


class UserDestRepository:

    def insert_user(self, conn: Connection, user: UserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_users(order_user_id)
                    VALUES (%(order_user_id)s)
                    ON CONFLICT DO NOTHING;
                """,
                {"order_user_id": user.order_user_id},
            )

    def truncate(self, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE stg.bonussystem_users;")


class UserLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log) -> None:
        self.pg_dest = pg_dest
        self.origin = UsersOriginRepository(pg_origin)
        self.stg = UserDestRepository()
        self.log = log

    def load_users(self):
        with self.pg_dest.connection() as conn:
            load_queue = self.origin.list_users()
            self.log.info(f"Found {len(load_queue)} users to load.")
            self.stg.truncate(conn)
            for user in load_queue:
                self.stg.insert_user(conn, user)
            self.log.info(f"Loaded {len(load_queue)} users.")


# -------- Events (outbox) --------

class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class EventsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, last_loaded_id: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE id > %(threshold)s
                    ORDER BY id ASC;
                """,
                {"threshold": last_loaded_id}
            )
            return cur.fetchall()


class EventDestRepository:

    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    OVERRIDING SYSTEM VALUE
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                },
            )


class WfSettingsRepository:

    def get_setting(self, conn: Connection, workflow_key: str) -> Optional[Dict]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT workflow_settings
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = %(workflow_key)s;
                """,
                {"workflow_key": workflow_key},
            )
            row = cur.fetchone()
        if row:
            return row[0] if isinstance(row[0], dict) else json.loads(row[0])
        return None

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: Dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(workflow_key)s, %(workflow_settings)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "workflow_key": workflow_key,
                    "workflow_settings": json.dumps(workflow_settings)
                },
            )


class EventLoader:
    WF_KEY = "bonus_system_events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log) -> None:
        self.pg_dest = pg_dest
        self.origin = EventsOriginRepository(pg_origin)
        self.stg = EventDestRepository()
        self.settings = WfSettingsRepository()
        self.log = log

    def load_events(self):
        with self.pg_dest.connection() as conn:
            # Шаг 1: читаем курсор из предыдущего запуска
            wf_settings = self.settings.get_setting(conn, self.WF_KEY)
            last_loaded_id = wf_settings[self.LAST_LOADED_ID_KEY] if wf_settings else -1
            self.log.info(f"Starting from id > {last_loaded_id}.")

            # Шаг 2: читаем все новые события из outbox
            load_queue = self.origin.list_events(last_loaded_id)
            self.log.info(f"Found {len(load_queue)} events to load.")

            if not load_queue:
                self.log.info("No new events. Quitting.")
                return

            # Шаги 3 и 4: сохраняем данные и курсор в одной транзакции
            for event in load_queue:
                self.stg.insert_event(conn, event)

            new_last_id = max(e.id for e in load_queue)
            self.settings.save_setting(conn, self.WF_KEY, {self.LAST_LOADED_ID_KEY: new_last_id})

            self.log.info(f"Loaded {len(load_queue)} events. Last loaded id: {new_last_id}.")


# -------- DAG --------

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def sprint5_stg_bonus_system_events_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        RankLoader(origin_pg_connect, dwh_pg_connect, log).load_ranks()

    @task(task_id="users_load")
    def load_users():
        UserLoader(origin_pg_connect, dwh_pg_connect, log).load_users()

    @task(task_id="events_load")
    def load_events():
        EventLoader(origin_pg_connect, dwh_pg_connect, log).load_events()

    ranks_task = load_ranks()
    users_task = load_users()
    events_task = load_events()

    # ranks и users независимы, events идёт после обоих
    [ranks_task, users_task] >> events_task  # type: ignore


stg_bonus_system_events_dag = sprint5_stg_bonus_system_events_dag()