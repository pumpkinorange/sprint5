import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_users_dag.users_loader import UsersLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'dds'],
    is_paused_upon_creation=True
)
def sprint5_example_dds_dm_users():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_users_load")
    def load_users():
        loader = UsersLoader(dwh_pg_connect, log)
        loader.load_users()

    load_users()


dds_dm_users_dag = sprint5_example_dds_dm_users()  # noqa