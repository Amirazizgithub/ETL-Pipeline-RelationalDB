import os
import pandas as pd
from google.cloud import bigquery
from utils.logger import log
from pipeline_configs import PipelineConfig
from scripts.data_transformer import DataTransformer


class BigQueryLoader(PipelineConfig, DataTransformer):
    def __init__(self, data: dict) -> None:
        try:
            # Initialize both parent classes correctly
            PipelineConfig.__init__(self, data=data)
            DataTransformer.__init__(self)

            self._setup_authentication()
            self.client = bigquery.Client(project=self.CLIENT_PROJECT_ID)
            log.info("BigQueryLoader successfully initialized.")
        except Exception as e:
            log.error(f"Error in BigQueryLoader Init: {e}")
            raise e

    def _setup_authentication(self) -> None:
        if getattr(self, "SERVICE_ACCOUNT_KEY", None) and os.path.exists(
            self.SERVICE_ACCOUNT_KEY
        ):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.SERVICE_ACCOUNT_KEY
            log.info(f"Auth: Service account loaded.")
        else:
            log.info("Auth: Using default ADC.")

    def _build_bq_schema(self, table_schema_info: dict) -> list:
        """Converts dict to BigQuery SchemaField objects."""
        log.info("Converting dict to BigQuery SchemaField objects...")
        return [
            bigquery.SchemaField(name, dtype, mode="NULLABLE")
            for name, dtype in table_schema_info.items()
        ]

    def _get_full_table_id(self, dataset_id: str, table_id: str) -> str:
        """Returns a clean project.dataset.table string."""
        clean_table = table_id.split(".")[-1]
        return f"{self.CLIENT_PROJECT_ID}.{dataset_id}.{clean_table}"

    def _cleanup_staging_table(self, staging_table_id: str) -> None:
        """Deletes the temporary staging table from BigQuery."""
        try:
            self.client.delete_table(staging_table_id, not_found_ok=True)
            log.debug(f"Cleanup: Staging table {staging_table_id} deleted.")
        except Exception as e:
            log.warning(f"Cleanup Failed for {staging_table_id}: {e}")

    def _load_to_bigquery(
        self,
        dataframe: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        table_schema_info: dict,
        date_column: str = None,
        primary_key: str = None,
    ) -> bool:
        staging_table_id = None
        try:
            full_table_id = self._get_full_table_id(dataset_id, table_id)
            bq_schema = self._build_bq_schema(table_schema_info=table_schema_info)

            if primary_key and date_column and date_column in dataframe.columns:
                staging_table_id = f"{full_table_id}_staging"
                log.info(f"[{table_id}] Incremental Load via {staging_table_id}")

                job_config = bigquery.LoadJobConfig(
                    schema=bq_schema, write_disposition="WRITE_TRUNCATE"
                )
                self.client.load_table_from_dataframe(
                    dataframe, staging_table_id, job_config=job_config
                ).result()

                upsert_query = f"""
                DELETE FROM `{full_table_id}` WHERE `{primary_key}` IN (SELECT DISTINCT `{primary_key}` FROM `{staging_table_id}`);
                INSERT INTO `{full_table_id}` SELECT * FROM `{staging_table_id}`;
                """
                self.client.query(upsert_query).result()
                log.debug(f"Primary Key: {primary_key} Date Column: {date_column}")
                log.success(
                    f"✓ [{table_id}] Incremental upsert complete with Primary Key: {primary_key} Date Column: {date_column}."
                )

            elif date_column and date_column in dataframe.columns:
                staging_table_id = f"{full_table_id}_staging"
                log.info(f"[{table_id}] Incremental Load via {staging_table_id}")

                job_config = bigquery.LoadJobConfig(
                    schema=bq_schema, write_disposition="WRITE_TRUNCATE"
                )
                self.client.load_table_from_dataframe(
                    dataframe, staging_table_id, job_config=job_config
                ).result()

                upsert_query = f"""
                DELETE FROM `{full_table_id}` WHERE `{date_column}` IN (SELECT DISTINCT `{date_column}` FROM `{staging_table_id}`);
                INSERT INTO `{full_table_id}` SELECT * FROM `{staging_table_id}`;
                """
                self.client.query(upsert_query).result()
                log.debug(f"Date Column: {date_column}")
                log.success(
                    f"✓ [{table_id}] Incremental upsert complete with Date Column: {date_column}."
                )

            else:
                log.info(f"[{table_id}] No date column. Performing Full Overwrite.")
                job_config = bigquery.LoadJobConfig(
                    schema=bq_schema, write_disposition="WRITE_TRUNCATE"
                )
                self.client.load_table_from_dataframe(
                    dataframe, full_table_id, job_config=job_config
                ).result()
                log.success(f"✓ [{table_id}] Full load complete.")

            return True

        except Exception as e:
            log.error(f"Failed to load {table_id} to BigQuery: {e}")
            raise e

        finally:
            if staging_table_id:
                self._cleanup_staging_table(staging_table_id)

    def _initiate_data_loading(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        dataset_id: str,
        date_column: str = None,
        primary_key: str = None,
        table_schema_info: dict = None,
    ) -> bool:
        """Main flow: Transformation -> Loading."""
        log.info(
            f"{25*'='} Initiating BigQueryLoader for {dataset_id}-{table_name} {25*'='}"
        )
        try:
            if dataframe.empty:
                log.warning(f"Skipping {table_name}: Dataframe is empty.")
                return False

            log.info(f"--- Starting Pipeline for: {table_name} ---")
            transformed_df = self._initiate_transformations(
                dataframe=dataframe, table_schema_info=table_schema_info
            )

            return self._load_to_bigquery(
                dataframe=transformed_df,
                dataset_id=dataset_id,
                table_id=table_name,
                table_schema_info=table_schema_info,
                date_column=date_column,
                primary_key=primary_key,
            )
        except Exception as e:
            log.error(f"Pipeline flow failed for {table_name}: {e}")
            raise e
