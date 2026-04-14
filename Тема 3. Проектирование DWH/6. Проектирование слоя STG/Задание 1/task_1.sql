-- Вставка таблиц для загрузки данных в слой STG для проекта DWH по бонусной системе.
-- Пример таблиц берется из de-public yandex-cloud
-- Конфигурация подключения к базе данных:
-- "host": "localhost",
-- "user": "jovyan",
-- "password": "jovyan"
-- "port": 15432,
-- "ssl": false,
-- "database": "de"

CREATE TABLE stg.bonussystem_users (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) DEFAULT 0 NOT NULL,
	min_payment_threshold numeric(19, 5) DEFAULT 0 NOT NULL,
	CONSTRAINT ranks_bonus_percent_check CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_bonus_percent_check1 CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.bonussystem_events (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);