import logging

import pendulum
from airflow.decorators import dag, task
from examples.cdm.dm_settlement_report_dag.settlement_report_loader import SettlementReportLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'cdm'],
    is_paused_upon_creation=True
)
def sprint5_example_cdm_dm_settlement_report():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_settlement_report_load")
    def load_settlement_report():
        loader = SettlementReportLoader(dwh_pg_connect, log)
        loader.load_report()

    load_settlement_report()


cdm_dm_settlement_report_dag = sprint5_example_cdm_dm_settlement_report()  # noqa
