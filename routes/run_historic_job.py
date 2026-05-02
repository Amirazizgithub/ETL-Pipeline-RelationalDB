import os
import sys
import json
from utils.logger import log
from scripts.run_etl_pipeline import ETLPipeline


def run_historic_job():
    # Configure logging
    log.info(f"{'#'*25} HISTORIC JOB STARTED {'#'*25}")

    # 1. Retrieve the payload from the environment variable
    payload_str = os.environ.get("HISTORIC_TASK_PAYLOAD")

    if not payload_str:
        log.error("Error: 'HISTORIC_TASK_PAYLOAD' environment variable is missing.")
        sys.exit(1)

    try:
        # 2. Parse the JSON payload
        log.debug("Parsing HISTORIC_TASK_PAYLOAD...")
        data = json.loads(payload_str)
        log.debug(f"Parsed HISTORIC_TASK_PAYLOAD: {data}")

        # 3. Initialize and run the pipeline
        log.debug("Initializing ETLPipeline for Historic Data...")
        pipeline = ETLPipeline(data=data)
        pipeline._initiate_etl_pipeline()
        log.success("Historic Data ETLPipeline completed successfully.")
        log.info(f"{'#'*25} HISTORIC JOB ENDED {'#'*25}")
        sys.exit(0)
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse HISTORIC_TASK_PAYLOAD JSON: {str(e)}")
        sys.exit(1)
    except Exception as e:
        log.error(f"ETLPipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    run_historic_job()
