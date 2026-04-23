import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_users_dag.users_loader import UsersLoader
from examples.dds.dm_restaurants_dag.restaurants_loader import RestaurantsLoader
from examples.dds.dm_timestamps_dag.timestamps_loader import TimestampsLoader
from examples.dds.dm_products_dag.products_loader import ProductsLoader
from examples.dds.dm_orders_dag.orders_loader import OrdersLoader
from examples.dds.fct_product_sales_dag.fct_product_sales_loader import FctProductSalesLoader
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

    @task(task_id="dm_restaurants_load")
    def load_restaurants():
        loader = RestaurantsLoader(dwh_pg_connect, log)
        loader.load_restaurants()

    @task(task_id="dm_timestamps_load")
    def load_timestamps():
        loader = TimestampsLoader(dwh_pg_connect, log)
        loader.load_timestamps()

    @task(task_id="dm_products_load")
    def load_products():
        loader = ProductsLoader(dwh_pg_connect, log)
        loader.load_products()

    @task(task_id="dm_orders_load")
    def load_orders():
        loader = OrdersLoader(dwh_pg_connect, log)
        loader.load_orders()

    @task(task_id="fct_product_sales_load")
    def load_product_sales():
        loader = FctProductSalesLoader(dwh_pg_connect, log)
        loader.load_product_sales()

    load_users()
    load_restaurants()
    load_timestamps()
    load_products()
    load_orders()
    load_product_sales()


dds_dm_users_dag = sprint5_example_dds_dm_users()  # noqa
