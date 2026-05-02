import os
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict
from pipeline_configs import PipelineConfig
from scripts.relationaldb_config import RelationalDBConfig
from utils.logger import log
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()


class SchemaStateManager(PipelineConfig):
    def __init__(self, data: dict) -> None:
        PipelineConfig.__init__(self, data=data)
        self._setup_authentication()
        self.storage_client = storage.Client(project=self.CLIENT_PROJECT_ID)

    def _setup_authentication(self) -> None:
        """Setup GCP authentication from service account JSON."""
        if getattr(self, "SERVICE_ACCOUNT_KEY", None) and os.path.exists(
            self.SERVICE_ACCOUNT_KEY
        ):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.SERVICE_ACCOUNT_KEY
            log.info(f"Using service account: {self.SERVICE_ACCOUNT_KEY}")
        else:
            log.info("Using default ADC authentication.")

    def _load_stored_schema(self) -> dict:
        """Reads the schema JSON directly from a GCS bucket."""
        try:
            bucket = self.storage_client.bucket(self.BUCKET_NAME)
            blob = bucket.blob(self.SCHEMA_FILEPATH)

            if not blob.exists():
                log.info(
                    f"No stored schema found in GCS at {self.SCHEMA_FILEPATH}. Starting fresh."
                )
                return {}

            log.info(f"Downloading stored schema from GCS: {self.SCHEMA_FILEPATH}")
            data = blob.download_as_text()
            return json.loads(data)

        except Exception as e:
            log.error(f"Failed to load schema from GCS: {str(e)}")
            raise e

    def _save_schema(self, schema: dict) -> None:
        """Uploads (and overwrites) the schema JSON to the specified GCS bucket."""
        try:
            bucket = self.storage_client.bucket(self.BUCKET_NAME)
            blob = bucket.blob(self.SCHEMA_FILEPATH)

            log.info(
                f"Uploading/Overwriting schema state in GCS: {self.SCHEMA_FILEPATH}"
            )
            # upload_from_string handles the overwrite automatically
            blob.upload_from_string(
                data=json.dumps(schema, indent=4), content_type="application/json"
            )
            log.success(
                f"✓ Schema state successfully saved to gs://{self.BUCKET_NAME}/{self.SCHEMA_FILEPATH}"
            )

        except Exception as e:
            log.error(f"Failed to save schema to GCS: {str(e)}")
            raise e


class SchemaDifferenceEngine:
    @classmethod
    def _identify_delta(cls, stored_schema: dict, live_schema: dict) -> dict:
        delta = {}
        log.info("Starting Schema Delta Analysis...")

        for ds_id, tables in live_schema.items():
            # Scenario A: Entire Dataset is missing
            if ds_id not in stored_schema:
                delta[ds_id] = {
                    t_id: {
                        "columns": t_content["columns"],
                        "primary_key": t_content.get("primary_key"),
                        "sync_date_column": t_content.get("sync_date_column"),
                        "action": "CREATE",
                    }
                    for t_id, t_content in tables.items()
                }
                log.info(f"New Dataset detected: {ds_id}")
                continue

            for t_id, live_content in tables.items():
                live_cols = live_content.get("columns", {})
                # Assuming live data gives us a single PK string or we take the first from a list
                live_pk = live_content.get("primary_key")
                live_sync_date_column = live_content.get("sync_date_column")

                # Scenario B: Entire Table is missing
                if t_id not in stored_schema[ds_id]:
                    if ds_id not in delta:
                        delta[ds_id] = {}

                    delta[ds_id][t_id] = {
                        "columns": live_cols,
                        "primary_key": live_pk,
                        "sync_date_column": live_sync_date_column,
                        "action": "CREATE",
                    }
                    log.info(f"New Table detected: {ds_id}.{t_id}")
                    continue

                # Scenario C & D: Check for updates in existing tables
                stored_table_data = stored_schema[ds_id][t_id]
                stored_cols = stored_table_data.get("columns", {})
                stored_pk = stored_table_data.get("primary_key")
                stored_sync_date_column = stored_table_data.get("sync_date_column")

                columns_to_update = {}
                meta_changes = {}
                needs_alter = False

                # 1. Column/Type Changes
                for col_name, live_type in live_cols.items():
                    if col_name not in stored_cols:
                        columns_to_update[col_name] = live_type
                        needs_alter = True
                    elif str(live_type).upper() != str(stored_cols[col_name]).upper():
                        columns_to_update[col_name] = live_type
                        needs_alter = True

                # 2. Primary Key changes
                if live_pk != stored_pk:
                    meta_changes["primary_key"] = live_pk
                    needs_alter = True

                if live_sync_date_column != stored_sync_date_column:
                    meta_changes["sync_date_column"] = live_sync_date_column
                    needs_alter = True

                # 3. Assemble Delta
                if needs_alter:
                    if ds_id not in delta:
                        delta[ds_id] = {}

                    # Recalculate sync column in case the new columns offer a better choice
                    new_sync_col = live_sync_date_column

                    delta[ds_id][t_id] = {
                        "columns": columns_to_update,
                        "primary_key": live_pk,
                        "sync_date_column": new_sync_col,
                        "action": "ALTER",
                    }

        log.info("Schema Delta Analysis complete.")
        return delta


class GCPManager(RelationalDBConfig):
    def __init__(self, data: dict) -> None:
        self.data = data
        super().__init__(data=self.data)
        self._setup_authentication()
        self.client = bigquery.Client(project=self.CLIENT_PROJECT_ID)
        log.debug("BigQueryManager class initialized")

    def _setup_authentication(self) -> None:
        if self.SERVICE_ACCOUNT_KEY and os.path.exists(self.SERVICE_ACCOUNT_KEY):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.SERVICE_ACCOUNT_KEY
            log.info(f"Using service account: {self.SERVICE_ACCOUNT_KEY}")
        else:
            log.info("Using default ADC authentication.")

    def _get_schema_fields(self, columns: dict) -> list:
        """Converts dict to BigQuery SchemaField objects."""
        return [
            bigquery.SchemaField(name, dtype, mode="NULLABLE")
            for name, dtype in columns.items()
        ]

    def _ensure_dataset(self, ds_id: str) -> None:
        log.debug(f"Checking for Dataset: {ds_id}")
        ds_ref = self.client.dataset(ds_id)
        try:
            self.client.get_dataset(ds_ref)
            log.debug(f"Dataset {ds_id} exists.")
        except NotFound:
            log.info(f"Creating Dataset: {ds_id}")
            dataset = bigquery.Dataset(ds_ref)
            dataset.location = self.CLIENT_REGION
            self.client.create_dataset(dataset)
            log.info(f"✓ Created Dataset: {ds_id}")

    def _create_table_with_deep_sync(
        self, ds_id: str, t_id: str, table_info: dict
    ) -> None:
        """Handles table creation including PK and Sync metadata."""
        log.debug(f"Checking for Table: {ds_id}.{t_id}")
        table_ref = self.client.dataset(ds_id).table(t_id)
        columns = table_info.get("columns", {})
        pk = table_info.get("primary_key")
        sync_col = table_info.get("sync_date_column")

        schema = self._get_schema_fields(columns=columns)
        table = bigquery.Table(table_ref, schema=schema)

        # Metadata Management
        description_parts = []
        if pk:
            description_parts.append(f"PK: {pk}")
        if sync_col:
            description_parts.append(f"Sync: {sync_col}")

        table.description = " | ".join(description_parts)

        # Labels are useful for filtering tables in GCP Console
        table.labels = {
            "sync_column": str(sync_col).lower() if sync_col else "none",
            "managed_by": "etl_engine",
        }

        try:
            self.client.create_table(table)
            log.info(f"✓ Created Table: {ds_id}.{t_id}")
        except Conflict:
            log.warning(f"⚠️ Conflict: {t_id} exists. Updating instead...")
            self._alter_table(ds_id=ds_id, t_id=t_id, update_data=table_info)

    def _alter_table(self, ds_id: str, t_id: str, update_data: dict) -> None:
        """Processes column additions and metadata updates."""
        log.debug(f"Checking for Table: {ds_id}.{t_id}")
        table_ref = self.client.dataset(ds_id).table(t_id)
        new_cols = update_data.get("columns", {})
        live_pk = update_data.get("primary_key")
        live_sync = update_data.get("sync_date_column")

        try:
            table = self.client.get_table(table_ref)
            updated_schema = list(table.schema)
            existing_field_names = {
                f.name.lower(): i for i, f in enumerate(table.schema)
            }
            schema_changed = False

            # 1. Handle Column Additions (BigQuery doesn't support easy type changes)
            for name, dtype in new_cols.items():
                if name.lower() not in existing_field_names:
                    updated_schema.append(
                        bigquery.SchemaField(name, dtype, mode="NULLABLE")
                    )
                    schema_changed = True
                else:
                    log.debug(
                        f"Column {name} exists. Skipping (BQ Type changes require recreation)."
                    )

            if schema_changed:
                table.schema = updated_schema
                self.client.update_table(table, ["schema"])
                log.info(f"✓ Schema updated for {t_id}")

            # 2. Update Metadata
            description_parts = []
            if live_pk:
                description_parts.append(f"PK: {live_pk}")
            if live_sync:
                description_parts.append(f"Sync: {live_sync}")

            table.description = " | ".join(description_parts)
            self.client.update_table(table, ["description"])

        except Exception as e:
            log.error(f"Failed to alter {t_id}: {str(e)}")

    def _sync_delta(self, delta: dict) -> None:
        """Applies schema changes to both main and staging tables."""
        log.debug("Syncing Delta to BigQuery...")
        for ds_id, tables in delta.items():
            self._ensure_dataset(ds_id=ds_id)

            for t_id, info in tables.items():
                # Logic: If sync_date_column exists, we need a staging table for incremental loads
                needs_staging = info.get("sync_date_column") is not None

                if info["action"] == "CREATE":
                    self._create_table_with_deep_sync(
                        ds_id=ds_id, t_id=t_id, table_info=info
                    )
                    if needs_staging:
                        self._create_table_with_deep_sync(
                            ds_id=ds_id, t_id=f"staging_{t_id}", table_info=info
                        )

                elif info["action"] == "ALTER":
                    self._alter_table(ds_id=ds_id, t_id=t_id, update_data=info)
                    if needs_staging:
                        # Ensure staging table exists even if it wasn't there before
                        self._create_table_with_deep_sync(
                            ds_id=ds_id, t_id=f"staging_{t_id}", table_info=info
                        )

    def _initiate_gcp_manager(self) -> None:
        log.info(f"{25*'='} Initiating GCPManager {25*'='}")
        try:
            # 1. Fetch live schema from Source (e.g., MySQL)
            live_schema = self._get_all_table_schema_with_dtype()
            # log.debug(f"Live Schema: {live_schema}")

            # 2. Compare with previously stored state
            state_manager = SchemaStateManager(data=self.data)
            stored_schema = state_manager._load_stored_schema()
            # log.debug(f"Stored Schema: {stored_schema}")

            # 3. Use the Engine to find what's changed
            sd_engine = SchemaDifferenceEngine()
            missing_delta = sd_engine._identify_delta(
                stored_schema=stored_schema, live_schema=live_schema
            )
            # log.debug(f"Missing Delta: {missing_delta}")

            if missing_delta:
                log.info(f"Changes detected: {list(missing_delta.keys())}. Syncing...")
                self._sync_delta(delta=missing_delta)

                # 4. Save the new schema as the 'current' state
                state_manager._save_schema(schema=live_schema)
                log.info("✓ BigQuery and state file synchronized.")
            else:
                log.info("No schema changes detected.")

        except Exception as e:
            log.error(f"Critical failure in GCPManager: {str(e)}")
            raise e


# if __name__ == "__main__":
#     data = {"START_DATE": "", "END_DATE": "", "ETL_SOURCE": "MYSQL", "WINDOW_DAYS": 30}
#     gcp_manager = GCPManager(data=data)
#     gcp_manager._initiate_gcp_manager()
