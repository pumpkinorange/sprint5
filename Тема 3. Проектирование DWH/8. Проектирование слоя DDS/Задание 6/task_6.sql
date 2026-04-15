DROP TABLE dds.dm_timestamps;

CREATE TABLE dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamptz NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"date" date NOT NULL,
	"time" time NOT NULL,
	CONSTRAINT dm_timestamps_consistency_check CHECK (((year = (date_part('year'::text, ts))::smallint) AND (month = (date_part('month'::text, ts))::smallint) AND (day = (date_part('day'::text, ts))::smallint) AND (date = (timezone('UTC'::text, ts))::date) AND ("time" = (ts)::time without time zone))),
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_ts_key UNIQUE (ts)	
);