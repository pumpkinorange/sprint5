Разберём шаблон объявление DAG. Код вы можете найти в файле order_system_restaurants_dag.py.

@dag(
        # Задаём расписание выполнения DAG - каждые 15 минут.
    schedule_interval='0/15 * * * *',
    # Указываем дату начала выполнения DAG. Поставьте текущую дату.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    # Указываем, нужно ли запускать даг за предыдущие периоды (со start_date до сегодня). 
    catchup=False,  #  DAG не нужно запускать за предыдущие периоды.
    # Задаём теги, которые используются для фильтрации в интерфейсе Airflow. 
    tags=['sprint5', 'example', 'stg', 'origin'], 
        # Прописываем, остановлен DAG или запущен при появлении.
    is_paused_upon_creation=False  # DAG cразу запущен.
)
def sprint5_example_stg_order_system_restaurants():
    # Создаём подключение к базе DWH.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, который реализует чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    restaurant_loader = load_restaurants()

    # Задаём порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurant_loader  # type: ignore

order_stg_dag = sprint5_example_stg_order_system_restaurants()  # noqaairflow

Рассмотрим на примере restaurants. Таск состоит из следующих шагов: 
Вычитать данные из таблицы. 
Проставить лимит обрабатываемых за один раз объектов, например, 1000. 
Ресторанов меньше, но для других объектов пригодится.
Преобразовать данные в формат JSON.
Сохранить данные в таблицу stg.ordersystem_restaurants.
Записать update_ts последнего сохранённого объекта в таблицу stg.srv_wf_settings.

Затем вам надо будет самостоятельно повторить для коллекций users и orders.

Используем тот же подход, который был при загрузке данных из PostgreSQL подсистемы бонусов. Разница только в том, что источником является MongoDB. Код подключения к MongoDB представлен в начале текущего урока.

Этот DAG вы можете использовать как основу для выполнения следующих заданий. Остаётся загрузить данные о пользователях и заказах.

Задание 4. Реализуйте таск, который переносит данные о пользователях
Реализуйте таск, который будет загружать данные о пользователях в ваш PostgreSQL. Этот таск должен перекладывать в таблицу stg.ordersystem_users те данные, у которых update_ts больше сохранённой отметки. При первом запуске этой отметки нет, поэтому загрузку стоит начать сначала.