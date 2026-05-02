import re
import pytz
import pandas as pd
from utils.logger import log
from decimal import Decimal


class DataTransformer:
    def __init__(self) -> None:
        pass

    def __repr__(self) -> str:
        return f"TransformedData class is responsible for transforming data before loading into BigQuery."

    def __str__(self) -> str:
        return f"TransformedData class is responsible for transforming data before loading into BigQuery."

    # def _sanitize_bigquery_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Clean DataFrame column names to be BigQuery-safe.
    #     - Replace spaces and special chars with underscores
    #     - Strip leading/trailing underscores
    #     - Ensure names start with a letter or underscore
    #     """
    #     log.info("Sanitizing column names for BigQuery compatibility...")
    #     log.debug(f"Input DataFrame shape: {dataframe.shape}")
    #     try:
    #         clean_cols = []
    #         for col in dataframe.columns:
    #             new_col = re.sub(r"[^0-9a-zA-Z_]", "_", col)  # Replace invalid chars
    #             new_col = re.sub(r"_+", "_", new_col)  # Collapse multiple underscores
    #             new_col = new_col.strip("_")  # Remove leading/trailing underscores
    #             if not re.match(r"^[A-Za-z_]", new_col):
    #                 new_col = f"_{new_col}"  # Ensure valid start
    #             clean_cols.append(new_col)
    #         dataframe.columns = clean_cols
    #         log.debug(f"Output DataFrame shape: {dataframe.shape}")
    #         return dataframe
    #     except Exception as e:
    #         log.error(f"Error in Sanitize BQ Columns: {e}")
    #         raise e

    # def _convert_datetime_columns_to_ist(self, dataframe: pd.DataFrame) -> pd.DataFrame:
    #     log.info("Converting datetime columns to IST...")
    #     for col in dataframe.columns:
    #         if any(x in col.lower() for x in ["date", "time", "timestamp"]):
    #             try:
    #                 # Use simpler to_datetime first
    #                 dt = pd.to_datetime(dataframe[col], errors="coerce")

    #                 # FIX: Explicitly localize naive times to UTC before converting
    #                 if dt.dt.tz is None:
    #                     dt = dt.dt.tz_localize(pytz.utc)

    #                 # Now that it's aware, convert to IST
    #                 dt = dt.dt.tz_convert(self.IST)

    #                 # Drop the timezone, making it a naive timestamp at the IST clock time
    #                 dt = dt.dt.tz_localize(None)

    #                 # ↓ Downcast to microsecond precision safely
    #                 dataframe[col] = dt.astype("datetime64[us]")
    #             except Exception as e:
    #                 log.error(f"Error in Convert DateTime Columns: {e}")
    #                 raise e
    #     return dataframe

    # Remove duplicate rows
    def _remove_duplicate_rows(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        log.info("Removing duplicate rows...")
        return dataframe.drop_duplicates()

    def _fix_numeric_precision(
        self, dataframe: pd.DataFrame, table_schema_info: dict
    ) -> pd.DataFrame:
        """
        Converts NUMERIC columns to Python Decimal objects.
        This allows PyArrow to correctly map them to BigQuery NUMERIC types
        without precision loss or byte-length errors.
        """
        if table_schema_info is None:
            log.warning(f"Table Schema Info is None. Skipping precision fix.")
            return dataframe

        log.info(
            "Converting NUMERIC columns to Decimal objects for PyArrow compatibility..."
        )

        for col, bq_type in table_schema_info.items():
            if bq_type == "NUMERIC" and col in dataframe.columns:
                try:
                    # 1. Convert to string first (prevents float precision issues)
                    # 2. Convert string to Decimal
                    # 3. Handle NULLs/NaNs
                    dataframe[col] = dataframe[col].apply(
                        lambda x: (
                            Decimal(str(x))
                            if pd.notnull(x) and str(x).strip() != ""
                            else None
                        )
                    )
                    log.debug(f"Successfully converted {col} to Decimal.")
                except Exception as e:
                    log.error(f"Failed to convert {col} to Decimal: {e}")

        return dataframe

    def _initiate_transformations(
        self, dataframe: pd.DataFrame, table_schema_info: dict = None
    ) -> pd.DataFrame:
        try:
            log.info(f"{25*'='} Initiating DataTransformer {25*'='}")
            # sanitized_dataframe = self._sanitize_bigquery_columns(dataframe=dataframe)
            # transformed_dataframe = self._convert_datetime_columns_to_ist(
            #     dataframe=sanitized_dataframe
            # )
            clean_dataframe = self._remove_duplicate_rows(dataframe=dataframe)
            transformed_dataframe = self._fix_numeric_precision(
                dataframe=clean_dataframe, table_schema_info=table_schema_info
            )
            log.success("✓ DataTransformer completed successfully.")
            return transformed_dataframe
        except Exception as e:
            log.error(f"Error in Initiate Transformations: {str(e)}")
            raise e
