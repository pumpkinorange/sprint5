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
            objs = cur.fetchall()
        return objs


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


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def sprint5_stg_bonus_system_ranks_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        rank_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rank_loader.load_ranks()

    load_ranks()


stg_bonus_system_ranks_dag = sprint5_stg_bonus_system_ranks_dag()
