import re
import os
import time
import json
import functools
import mysql.connector
from mysql.connector import errorcode
from pipeline_configs import PipelineConfig
from utils.logger import log
from dotenv import load_dotenv

load_dotenv()


class BigQueryNamespaceValidator:
    @staticmethod
    def validate_dataset_name(raw_name: str) -> str:
        """
        Cleans and validates a MySQL database name for BigQuery compatibility.
        """
        if not raw_name:
            raise ValueError("Database name is empty or None")

        clean_name = raw_name.lower()
        clean_name = re.sub(r"[-\s]+", "_", clean_name)
        clean_name = re.sub(r"[^a-z0-9_]", "", clean_name)

        if clean_name[0].isdigit():
            clean_name = f"db_{clean_name}"

        reserved_words = {"all", "default", "information_schema", "public"}
        if clean_name in reserved_words:
            clean_name = f"data_{clean_name}"

        return clean_name[:1024]


def retry_mysql_connection(retries=3, delay=2, backoff=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            m_retries, m_delay = retries, delay
            while m_retries > 1:
                try:
                    return func(*args, **kwargs)
                except (
                    mysql.connector.OperationalError,
                    mysql.connector.InterfaceError,
                ) as e:
                    log.warning(f"Connection failed: {e}. Retrying in {m_delay}s...")
                    time.sleep(m_delay)
                    m_retries -= 1
                    m_delay *= backoff
            return func(*args, **kwargs)

        return wrapper

    return decorator


class RelationalDBConfig(PipelineConfig):
    def __init__(self, data: dict) -> None:
        super().__init__(data=data)
        # Pre-compile for performance (place these outside the class)
        # self.SYNC_REGEX = re.compile(
        #     r"(updat|modif|chang|sync|last|creat|insert|event|ts|timestamp)"
        # )
        self.BLACKLIST_REGEX = re.compile(
            r"(birth|dob|delet|expir|valid_from|valid_to)"
        )
        self.live_schema_info = {}
        log.debug("RelationalDBConfig class initialized")

    def _save_schema_local(self, filepath: str, schema: dict) -> None:
        with open(filepath, "w") as f:
            log.info("Saving schema state to file...")
            json.dump(schema, f, indent=4)

    @retry_mysql_connection(retries=3, delay=2, backoff=2)
    def _mysql_connector(self, database=None):
        log.info(f"{25*'='} Initiating MySQL Connection {25*'='}")
        try:
            conn_args = {
                "host": self.RDMS_HOST,
                "user": self.RDMS_USERNAME,
                "password": self.RDMS_PASSWORD,
                "port": self.RDMS_PORT,
                "database": database,
                "connect_timeout": 10,
            }
            conn_args["ssl_disabled"] = not getattr(self, "USE_SSL", False)
            return mysql.connector.connect(**conn_args)
        except mysql.connector.Error as err:
            log.error(f"Connection Error: {err}")
            return None

    def _get_sync_date_column(self, mysql_types: dict) -> str:
        if not mysql_types:
            return None

        date_types = {"date", "timestamp", "datetime"}

        # Define priority: lower index means higher importance
        priority_keywords = ["updat", "modif", "chang", "sync", "last"]
        fallback_keywords = ["creat", "insert", "event", "ts", "timestamp"]

        best_match = None
        best_priority_index = float("inf")
        found_generic_date = None

        for col, dtype in mysql_types.items():
            col_lower = col.lower()
            dtype_lower = dtype.lower()

            if self.BLACKLIST_REGEX.search(col_lower):
                continue

            is_date_type = any(d in dtype_lower for d in date_types)
            if not is_date_type:
                continue

            # 1. Track generic date for final fallback
            if not found_generic_date:
                found_generic_date = col

            # 2. Check High Priority (Updates)
            for i, kw in enumerate(priority_keywords):
                if kw in col_lower and i < best_priority_index:
                    best_match = col
                    best_priority_index = i
                    break  # Found a match for this column, move to next column

            # 3. Check Low Priority (Creation) ONLY if no High Priority found yet
            if best_match is None:
                for i, kw in enumerate(fallback_keywords):
                    # We add length of priority list to keep indices unique and higher than updates
                    actual_prio = i + len(priority_keywords)
                    if kw in col_lower and actual_prio < best_priority_index:
                        best_match = col
                        best_priority_index = actual_prio
                        break

        return best_match if best_match else found_generic_date

    def _map_mysql_to_bigquery_schema(self, mysql_schema: dict) -> dict:
        bq_schema = {}
        for column, dtype in mysql_schema.items():
            # Extract base type (e.g., 'int(11)' -> 'int' or 'int unsigned' -> 'int')
            base_type = str(dtype).lower().split("(")[0].split(" ")[0]

            if any(
                kw in base_type for kw in ["varchar", "text", "char", "enum", "set"]
            ):
                bq_schema[column] = "STRING"
            else:
                bq_schema[column] = self.RDMS_TYPE_MAPPING.get(base_type, "STRING")
        return bq_schema

    def _get_all_table_schema_with_dtype(self) -> dict:
        log.info(f"{25*'='} Fetching MySQL Schemas {25*'='}")
        conn = self._mysql_connector()
        if not conn:
            return {}

        try:
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            system_dbs = {"information_schema", "mysql", "performance_schema", "sys"}
            databases = [db[0] for db in cursor.fetchall() if db[0] not in system_dbs]
            cursor.close()
            conn.close()

            for db_name in databases:
                db_conn = self._mysql_connector(database=db_name)
                if not db_conn:
                    continue

                current_db_tables = {}
                try:
                    db_cursor = db_conn.cursor()
                    db_cursor.execute("SHOW TABLES")
                    tables = [t[0] for t in db_cursor.fetchall()]

                    for table in tables:
                        db_cursor.execute(f"DESCRIBE `{table}`")
                        rows = db_cursor.fetchall()

                        # col[0] = Name, col[1] = Type, col[3] = Key (PRI = Primary Key)
                        mysql_types = {col[0]: col[1] for col in rows}
                        primary_key = next(
                            (col[0] for col in rows if col[3] == "PRI"), None
                        )

                        current_db_tables[table] = {
                            "columns": self._map_mysql_to_bigquery_schema(mysql_types),
                            "primary_key": primary_key,
                            "sync_date_column": self._get_sync_date_column(mysql_types),
                        }

                    bq_dataset = BigQueryNamespaceValidator.validate_dataset_name(
                        db_name
                    )
                    self.live_schema_info[bq_dataset] = current_db_tables
                    db_cursor.close()
                finally:
                    db_conn.close()

        except Exception as e:
            log.error(f"Global Error: {e}")
        # self._save_schema_local("live_schema_info.json", self.live_schema_info)
        return self.live_schema_info


# # Example Usage:
# if __name__ == "__main__":
#     data = {"START_DATE": "", "END_DATE": "", "ETL_SOURCE": "MySQL", "WINDOW_DAYS": 1}
#     config = RelationalDBConfig(data=data)
#     live_schema = config._get_all_table_schema_with_dtype()
#     print(live_schema)
