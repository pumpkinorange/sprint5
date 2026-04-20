import logging
from typing import List

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


# -------- DAG --------

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def sprint5_stg_bonus_system_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        RankLoader(origin_pg_connect, dwh_pg_connect, log).load_ranks()

    @task(task_id="users_load")
    def load_users():
        UserLoader(origin_pg_connect, dwh_pg_connect, log).load_users()

    ranks_task = load_ranks()
    users_task = load_users()

    ranks_task >> users_task  # type: ignore


stg_bonus_system_dag = sprint5_stg_bonus_system_dag()