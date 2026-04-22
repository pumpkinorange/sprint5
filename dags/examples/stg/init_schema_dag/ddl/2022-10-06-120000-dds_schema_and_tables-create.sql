CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
    id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
    workflow_key varchar NOT NULL,
    workflow_settings json NOT NULL,
    CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
    CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

CREATE TABLE IF NOT EXISTS dds.dm_users (
    id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
    user_id varchar NOT NULL,
    user_login varchar NOT NULL,
    user_name varchar NOT NULL,
    CONSTRAINT dm_users_pkey PRIMARY KEY (id),
    CONSTRAINT dm_users_user_id_uindex UNIQUE (user_id)
);