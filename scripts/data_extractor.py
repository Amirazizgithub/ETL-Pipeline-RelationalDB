import os
import json
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
from utils.logger import log
from scripts.relationaldb_config import RelationalDBConfig


class DataExtractor(RelationalDBConfig):
    def __init__(self, data: dict) -> None:
        try:
            super().__init__(data=data)
            self._setup_authentication()
            self.bq_client = bigquery.Client(project=self.CLIENT_PROJECT_ID)
            log.info("DataExtractor initialized...")
        except Exception as e:
            log.error(f"Initialization Failed: {str(e)}")
            raise e

    def _setup_authentication(self) -> None:
        if getattr(self, "SERVICE_ACCOUNT_KEY", None) and os.path.exists(
            self.SERVICE_ACCOUNT_KEY
        ):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.SERVICE_ACCOUNT_KEY
        else:
            log.info("Auth: Using default ADC.")

    def _get_table_max_date(
        self, database_name: str, table_name: str, date_column: str
    ) -> datetime | None:
        """Fetches the latest timestamp from the BigQuery table."""
        try:
            production_table_id = (
                f"{self.CLIENT_PROJECT_ID}.{database_name}.{table_name}"
            )
            query = f"""
            SELECT MAX({date_column}) as max_date
            FROM `{production_table_id}`
            WHERE {date_column} IS NOT NULL
            """
            job = self.bq_client.query(query)
            results = job.result()
            for row in results:
                if row.max_date:
                    log.info(f"Last Loaded Date For {table_name}: {row.max_date}")
                    return pd.to_datetime(row.max_date)
                else:
                    log.info(f"No Data Found For {table_name} (First Load)")
                    return None
        except Exception as e:
            log.error(f"Error fetching MAX date for {table_name}: {e}")
            return None

    def _calculate_date_range(
        self, database_name: str, table_name: str, date_column: str = None
    ) -> tuple:
        """Calculates the start and end strings for the SQL BETWEEN clause."""
        if self.START_DATE and self.END_DATE and date_column:
            log.debug(f"Using provided date range: {self.START_DATE} - {self.END_DATE}")
            return (self.START_DATE, self.END_DATE)

        now = datetime.now(self.IST).replace(tzinfo=None)
        log.debug(f"Current Time: {now}")

        if not date_column:
            log.debug("No date column provided, using full table scan.")
            return None, None

        max_date = self._get_table_max_date(
            database_name=database_name, table_name=table_name, date_column=date_column
        )

        if max_date:
            log.debug(f"Max Date: {max_date}")
            # reference_date ensures we don't start the lookback from a future date
            # This solves the 'start_date > end_date' bug for schedules/events
            reference_date = min(max_date, now)
            start_date = reference_date - timedelta(days=self.SAFETY_BUFFER_DAYS)
            log.debug(
                f"Incremental Sync Logic: Reference({reference_date.date()}) - Window({self.SAFETY_BUFFER_DAYS})"
            )
        else:
            # First time extraction fallback
            start_date = now - timedelta(days=self.WINDOW_DAYS)
            log.info(
                f"No max date found for {table_name}. Using default {self.WINDOW_DAYS} days lookback."
            )

        # We look into the future to capture scheduled events/exams
        end_date = now + timedelta(days=1)
        log.debug(f"End Date: {end_date}")

        return (
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            end_date.strftime("%Y-%m-%d %H:%M:%S"),
        )

    def _initiate_extract_table_data(
        self, database_name: str, table_name: str, date_column: str = None
    ) -> pd.DataFrame:
        """Executes the extraction with incremental filtering and full-load fallback."""
        log.info(
            f"{25*'='} Initiating DataExtraction for {database_name}-{table_name} {25*'='}"
        )
        db_conn = None
        try:
            start_date, end_date = self._calculate_date_range(
                database_name=database_name,
                table_name=table_name,
                date_column=date_column,
            )
            log.debug(f"[{table_name}] Date Range: {start_date} - {end_date}")
            db_conn = self._mysql_connector(database=database_name)

            if date_column and start_date and end_date:
                query = f"SELECT * FROM `{table_name}` WHERE `{date_column}` BETWEEN '{start_date}' AND '{end_date}'"
                log.info(
                    f"[{table_name}] Filter: {date_column} BETWEEN '{start_date}' AND '{end_date}'"
                )
            else:
                query = f"SELECT * FROM `{table_name}`"
                log.info(f"[{table_name}] Triggering FULL TABLE scan (no date column).")

            df = pd.read_sql(query, con=db_conn)

            if not df.empty:
                log.success(f"✓ [{table_name}] Successfully extracted {len(df)} rows.")
            else:
                log.warning(
                    f"[{table_name}] No rows found in the specified date range."
                )
                df = pd.DataFrame()
            return df

        except Exception as e:
            log.error(
                f"CRITICAL ERROR: Extraction failed for {database_name}.{table_name} | Error: {e}"
            )
            raise e
        finally:
            if db_conn and db_conn.is_connected():
                db_conn.close()
                log.debug(f"Connection closed for {database_name}")
