import os
import json
import psycopg2
from utils.logger import log
from typing import Dict, Any, Optional

# Load environment variables
from dotenv import load_dotenv

load_dotenv()


# GCP Cloud SQL PostgreSQL Database Handler
class GCPPostgresSQL:
    def __init__(self) -> None:
        try:
            # Cloud SQL connection parameters
            self.database_host = os.getenv("DATABASE_HOST")
            self.database_user = os.getenv("DATABASE_USER", "postgres")
            self.database_password = os.getenv("DATABASE_PASSWORD")
            self.database_port = os.getenv("DATABASE_PORT", "5432")
            self.database_name = os.getenv("DATABASE_NAME", "postgres")
            self.table_name = os.getenv("TABLE_NAME", "etl_logs")

            # Safety check for required host
            if not self.database_host:
                log.error("DATABASE_HOST environment variable is not set.")
                raise ValueError("DATABASE_HOST environment variable is not set.")

            log.debug("Database connection parameters initialized")
        except Exception as e:
            log.error(f"Error during database initialization: {e}")
            raise e

    # Representation methods (Simplified as instance_connection_name was undefined)
    def __repr__(self) -> str:
        return (
            f"GCPPostgresSQL(host={self.database_host}, database={self.database_name})"
        )

    # String conversion method
    def __str__(self) -> str:
        return "GCPPostgresSQL Database Connection Handler"

    # Create a connection to Cloud SQL instance
    def _getconn(self) -> Optional[psycopg2.extensions.connection]:
        """Creates a connection to Cloud SQL instance."""
        try:
            conn = psycopg2.connect(
                host=self.database_host,
                port=self.database_port,
                database=self.database_name,
                user=self.database_user,
                password=self.database_password,
            )
            log.success("Database connection established.")
            return conn
        except Exception as e:
            log.error(f"Error creating connection: {e}")
            # Do not re-raise here; return None so the calling function can handle it.
            return None

    # Function to insert ETL logs into the database
    def _insert_etl_logs(self, etl_logs: Dict[str, Any]) -> None:
        """
        Inserts ETL logs into the database.

        It is assumed the 'etl_logs' dictionary matches the column names:
        source, subaccount, connecting, sync_on, last_sync, status, logs.
        """
        log.info(f"{25*'#'} Inserting ETL logs {25*'#'}")
        log.debug("Attempting to insert ETL logs.")
        database_connection = None
        postgres_cursor = None

        try:
            database_connection = self._getconn()
            if database_connection is None:
                log.error("Database connection could not be established.")
                raise Exception("Database connection could not be established.")

            postgres_cursor = database_connection.cursor()
            log.info("Database connection established.")

            # 1. Extract values from the dictionary
            source = etl_logs.get("source")
            client_number = etl_logs.get("client_number")
            connecting = etl_logs.get("connecting")  # assumed to be a timestamp string
            sync_on = etl_logs.get("sync_on")  # assumed to be a string like "1 hour"
            last_sync = etl_logs.get("last_sync")  # assumed to be a timestamp string
            status = etl_logs.get("status")
            logs = etl_logs.get("logs")

            # 2. Prepare data for insertion
            # The 'logs' field (list of dictionaries) must be serialized to JSON text
            logs_json_data = json.dumps(logs)

            # 3. Execute the insert statement using the table_name from self.
            insert_query = f"""
                INSERT INTO {self.table_name}
                (source, client_number, connecting, sync_on, last_sync, status, logs)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            postgres_cursor.execute(
                insert_query,
                (
                    source,
                    client_number,
                    connecting,
                    sync_on,
                    last_sync,
                    status,
                    logs_json_data,
                ),
            )

            # 4. Commit the transaction to persist the changes
            database_connection.commit()
            log.success("ETL logs inserted successfully and transaction committed.")

        except Exception as e:
            log.error(f"Error inserting ETL logs: {e}")
            # Rollback the transaction on error
            if database_connection:
                database_connection.rollback()
            raise e

        finally:
            # 5. Safely close cursor and connection
            if postgres_cursor:
                postgres_cursor.close()
            if database_connection:
                database_connection.close()
            log.info(f"{25*'#'} End of ETL logs insertion {25*'#'}")
