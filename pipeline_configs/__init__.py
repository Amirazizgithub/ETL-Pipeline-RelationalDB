# ==============================================================================
# PIPELINE CONFIGURATION SETTINGS
# ==============================================================================
import os
import re
import pytz
from utils.logger import log
from dotenv import load_dotenv

load_dotenv()

"""
Configuration settings for Relational Databases ETL Pipeline.

This module contains all the configuration variables and settings
used throughout the pipeline components and scripts.
"""


class PipelineConfig:
    def __init__(self, data: dict) -> None:
        log.info("LOADING PIPELINE CONFIG...")
        try:
            # --- Google Cloud Configuration ---
            self.CLIENT_NUMBER: str = os.getenv("CLIENT_NUMBER")
            self.CLIENT_REGION: str = os.getenv("CLIENT_REGION")
            self.CLIENT_TIMEZONE: str = os.getenv("CLIENT_TIMEZONE")
            self.CLIENT_PROJECT_ID: str = os.getenv("CLIENT_PROJECT_ID")
            self.IST: pytz.timezone = pytz.timezone(self.CLIENT_TIMEZONE)
            self.ETL_SOURCE: str = data.get("ETL_SOURCE", "")
            self.WINDOW_DAYS: int = data.get("WINDOW_DAYS", 1)
            self.START_DATE: str = data.get("START_DATE", "")
            self.END_DATE: str = data.get("END_DATE", "")
            self.SAFETY_BUFFER_DAYS: int = 1
            self.SERVICE_ACCOUNT_KEY = os.getenv("SERVICE_ACCOUNT_KEY", None)
            self.BUCKET_NAME: str = f"etl-pipeline-statefiles-{self.CLIENT_NUMBER}"
            self.CLEAN_ETL_SOURCE: str = re.sub(r"[^0-9A-Za-z\s]+", "", self.ETL_SOURCE)
            self.FILENAME_NAME: str = re.sub(r"\s+", "_", self.CLEAN_ETL_SOURCE).strip(
                "_"
            )
            self.PIPELINE_NAME: str = "RelationalDB"
            self.SCHEMA_FILEPATH: str = (
                f"{self.PIPELINE_NAME}/{self.FILENAME_NAME}.json"
            )
            # Mapping as a class constant for better performance
            self.RDMS_TYPE_MAPPING = {
                # Integers
                "int": "INT64",
                "tinyint": "INT64",
                "smallint": "INT64",
                "mediumint": "INT64",
                "bigint": "INT64",
                "bit": "BOOL",
                # Decimals & Floats
                "decimal": "NUMERIC",  # Use NUMERIC for exact precision (money)
                "numeric": "NUMERIC",
                "float": "FLOAT64",
                "double": "FLOAT64",
                # Date & Time
                "datetime": "DATETIME",
                "date": "DATE",
                "timestamp": "TIMESTAMP",
                "time": "TIME",
                "year": "INT64",
                # Strings & Objects
                "char": "STRING",
                "varchar": "STRING",
                "text": "STRING",
                "mediumtext": "STRING",
                "longtext": "STRING",
                "tinytext": "STRING",
                "enum": "STRING",
                "set": "STRING",
                "json": "JSON",
                # Binary
                "blob": "BYTES",
                "longblob": "BYTES",
                "binary": "BYTES",
                "varbinary": "BYTES",
                # Defaults
                "boolean": "BOOL",
            }

            # --- Relational Database Configuration ---
            self.RDMS_USERNAME = os.getenv("RDMS_USERNAME")
            self.RDMS_PASSWORD = os.getenv("RDMS_PASSWORD")
            self.RDMS_HOST = os.getenv("RDMS_HOST")
            self.RDMS_PORT = os.getenv("RDMS_PORT")
            self.USE_SSL: bool = os.getenv("USE_SSL", False)
            log.success("Pipeline Config loaded successfully")
            log.info(f"PIPELINE CONFIG LOADED")

        except Exception as e:
            log.error(f"Error in Pipeline Config: {str(e)}")
            raise e
