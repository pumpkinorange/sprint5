import json
from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UserStgObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class UsersStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, user_threshold: int, limit: int) -> List[UserStgObj]:
        with self._db.client().cursor(row_factory=class_row(UserStgObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value
                    FROM stg.ordersystem_users
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {"threshold": user_threshold, "limit": limit},
            )
            return cur.fetchall()


class UsersDdsRepository:
    def insert_user(self, conn: Connection, user_id: str, user_login: str, user_name: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_login, user_name)
                    SELECT %(user_id)s::varchar, %(user_login)s::varchar, %(user_name)s::varchar
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dds.dm_users WHERE user_id = %(user_id)s::varchar
                    );
                """,
                {
                    "user_id": user_id,
                    "user_login": user_login,
                    "user_name": user_name,
                },
            )


class UsersLoader:
    WF_KEY = "example_users_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dwh: PgConnect, logger: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.stg = UsersStgRepository(pg_dwh)
        self.dds = UsersDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = logger

    def load_users(self):
        with self.pg_dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f"starting to load from last checkpoint: id > {last_loaded_id}")

            load_queue = self.stg.list_users(last_loaded_id, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                doc = json.loads(user.object_value)
                self.dds.insert_user(
                    conn,
                    user_id=user.object_id,
                    user_login=doc.get("login", ""),
                    user_name=doc.get("name", ""),
                )

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([u.id for u in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")