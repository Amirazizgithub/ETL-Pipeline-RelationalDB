import sys
import os
import json
from utils.logger import log
from scripts.run_etl_pipeline import ETLPipeline


def run_cron_job():
    log.info(f"{'#'*25} CRONJOB STARTED {'#'*25}")

    # 1. Retrieve payload from cron_task_payload.json
    payload_str = os.environ.get("CRONJOB_TASK_PAYLOAD")
    if payload_str is None:
        log.error("CRONJOB_TASK_PAYLOAD environment variable is not set")
        sys.exit(1)

    try:
        # 2. Parse the JSON payload
        log.debug(f"Parsing CRONJOB_TASK_PAYLOAD...")
        data = json.loads(payload_str)
        log.debug(f"Parsed CRONJOB_TASK_PAYLOAD: {data}")

        # 3. Initialize ETLPipeline
        log.debug("Initializing ETLPipeline via CronJob...")
        pipeline = ETLPipeline(data=data)
        pipeline._initiate_etl_pipeline()
        log.success("CronJob completed successfully")
        log.info(f"{'#'*25} CRONJOB ENDED {'#'*25}")
        sys.exit(0)
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse CRONJOB_TASK_PAYLOAD JSON: {str(e)}")
        sys.exit(1)
    except Exception as e:
        log.error(f"CronJob failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_cron_job()
