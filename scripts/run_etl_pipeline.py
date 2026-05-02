import pytz
import json
import pandas as pd
from datetime import datetime
from utils.logger import log
from scripts.data_loader import BigQueryLoader
from utils.gcpcloud_postgres_conn import GCPPostgresSQL
from scripts.gcp_manager import GCPManager, SchemaStateManager
from scripts.data_extractor import DataExtractor


class ETLPipeline(GCPManager, DataExtractor, BigQueryLoader):
    def __init__(self, data: dict) -> None:
        try:
            self.data = data
            GCPManager.__init__(self, data=self.data)
            DataExtractor.__init__(self, data=self.data)
            BigQueryLoader.__init__(self, data=self.data)
            self.state_manager = SchemaStateManager(data=self.data)
            self.etl_logs = {
                "source": self.ETL_SOURCE,
                "client_number": self.CLIENT_NUMBER,
                "connecting": datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S"),
                "sync_on": "1 hour",
                "last_sync": "",
                "status": "Failed",
                "logs": [],
            }
            self.gcp_postgres_sql = GCPPostgresSQL()
        except Exception as e:
            log.error(f"Error in ETL Pipeline: {e}")
            raise e

    def __repr__(self) -> str:
        return f"ETLPipeline(data='{self.data}')"

    def __str__(self) -> str:
        return "ETLPipeline class is responsible for running the ETL pipeline."

    def _save_schema(self, filepath: str, schema: dict) -> None:
        with open(filepath, "w") as f:
            log.info("Saving schema state to file...")
            json.dump(schema, f, indent=4)

    def _initiate_etl_pipeline(self) -> None:
        log.info(f"{25*'='} Initiating ETLPipeline {25*'='}")
        log.debug(
            f"Initiating ETLPipeline at {datetime.now(self.IST).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        try:
            # Initiate GCP Manager
            self._initiate_gcp_manager()

            # Load stored schema
            stored_schema = self.state_manager._load_stored_schema()
            log.info("Loaded stored schema successfully")

            # Create a directory for outputs
            table_logs = {}
            for db_name, tables in stored_schema.items():
                for table_name in tables.keys():
                    log.info(
                        f"{25*'#'} ETLPipeline for {db_name}-{table_name} {25*'#'}"
                    )
                    iterate_logs = {
                        "last_updated_date": "",
                        "status": "",
                        "message": "",
                    }
                    try:
                        table_schema_info = stored_schema[db_name][table_name]
                        date_column = table_schema_info.get("sync_date_column")
                        primary_key = table_schema_info.get("primary_key")
                        df = self._initiate_extract_table_data(
                            database_name=db_name,
                            table_name=table_name,
                            date_column=date_column,
                        )

                        if not df.empty:
                            # Load data into BigQuery tables one by one
                            self._initiate_data_loading(
                                dataframe=df,
                                table_name=table_name,
                                dataset_id=db_name,
                                date_column=date_column,
                                primary_key=primary_key,
                                table_schema_info=table_schema_info.get("columns"),
                            )
                            iterate_logs["last_updated_date"] = datetime.now(
                                self.IST
                            ).strftime("%Y-%m-%d %H:%M:%S")
                            iterate_logs["status"] = "Success"
                            iterate_logs["message"] = "Data loaded successfully"
                            table_logs[db_name + "-" + table_name] = iterate_logs
                            log.info(
                                f"Data loaded successfully for {db_name}-{table_name}"
                            )
                        elif df.empty:
                            iterate_logs["last_updated_date"] = datetime.now(
                                self.IST
                            ).strftime("%Y-%m-%d %H:%M:%S")
                            iterate_logs["status"] = "Success"
                            iterate_logs["message"] = f"No data found for sync"
                            table_logs[db_name + "-" + table_name] = iterate_logs
                            log.warning(f"No data found for {db_name}-{table_name}")
                    except Exception as e:
                        log.error(f"Error in ETLPipeline: {str(e)}")
                        iterate_logs["last_updated_date"] = datetime.now(
                            self.IST
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        iterate_logs["status"] = "Failed"
                        iterate_logs["message"] = "Error: " + str(e)
                        table_logs[db_name + "-" + table_name] = iterate_logs
                        continue

            if all(item.get("status") == "Success" for item in table_logs.values()):
                self.etl_logs["status"] = "Success"
            elif all(item.get("status") == "Failed" for item in table_logs.values()):
                self.etl_logs["status"] = "Failed"
            else:
                self.etl_logs["status"] = "Warning"
            etl_end_time = datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")
            self.etl_logs["last_sync"] = etl_end_time
            self.etl_logs["logs"] = [table_logs]
            log.success(f"ETL pipeline completed successfully at {etl_end_time}")

            # Insert logs into Postgres
            self.gcp_postgres_sql._insert_etl_logs(self.etl_logs)
            log.success("ETL logs inserted into Postgres")
            log.success("Postgres connection closed")
            # self._save_schema(filepath="etl_logs.json", schema=self.etl_logs)
            log.info(f"{25*'#'} ETLPipeline completed {25*'#'}")
        except Exception as e:
            log.error(f"Error in ETLPipeline: {str(e)}")
            etl_end_time = datetime.now(self.IST).strftime("%Y-%m-%d %H:%M:%S")
            self.etl_logs["status"] = "Failed"
            self.etl_logs["last_sync"] = etl_end_time
            self.etl_logs["logs"].append(
                {
                    self.ETL_SOURCE: {
                        "last_updated_date": etl_end_time,
                        "status": "Failed",
                        "message": "Error: " + str(e),
                    }
                }
            )
            log.error(f"ETL pipeline failed at {etl_end_time}")

            # Insert logs into Postgres
            self.gcp_postgres_sql._insert_etl_logs(self.etl_logs)
            log.success("ETL logs inserted into Postgres")
            log.success("Postgres connection closed")
            # self._save_schema(filepath="etl_logs.json", schema=self.etl_logs)
            raise e
