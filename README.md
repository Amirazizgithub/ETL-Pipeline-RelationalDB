# ETL Microservice for Relational Databases

A robust, containerized ETL (Extract, Transform, Load) microservice designed to replicate data from Relational Databases (MySQL) to Google BigQuery. This service features dynamic schema mapping, incremental synchronization, and state management via Google Cloud Storage. The project exposes these ETL processes via a GitLab API Trigger and is containerized for deployment on Kubernetes.

## � Features

-   **Dynamic Schema Mapping**: Automatically converts MySQL data types to their BigQuery equivalents (e.g., `VARCHAR` -> `STRING`, `DECIMAL` -> `NUMERIC`).
-   **Incremental Sync**: Intelligently identifies modification timestamps (e.g., `updated_at`, `timestamp`) to pull only changed records.
-   **State Management**: Tracks the synchronization state (last synced timestamp) in a JSON file stored in a GCS Bucket (`etl-pipeline-statefiles-{CLIENT_NUMBER}`).
-   **Namespace Validation**: Sanitizes database and table names to ensure compatibility with BigQuery datasets.
-   **Automated Logging**: centralized logging to standard output and optional persistence to a metadata database (PostgreSQL).
-   **Containerized**: Fully Dockerized for easy deployment on Kubernetes.

## 📂 Project Structure

```text
etl-pipeline-relationaldb/
├── 🐳 Dockerfile                    # Docker container build instructions
├── 📝 .dockerignore                 # Files to exclude from Docker context
├── 🙈 .gitignore                    # Files to exclude from Git version control
├── 📦 requirements.txt              # Python project dependencies
├── 📁 gitlab-pipelines/             # Environment-specific CI/CD steps
│   ├── ⚙️ .gitlab-ci-dev.yml        # Development pipeline
│   ├── ⚙️ .gitlab-ci-stag.yml       # Staging pipeline
│   ├── ⚙️ .gitlab-ci-update.yml     # Update pipeline
│   └── ⚙️ .gitlab-ci-prod.yml       # Production pipeline
├── 📁 k8s-pipelines/                # Kubernetes manifests
│   ├── ⚙️ historic-data-deploy-service.yml
│   └── ⚙️ cronjob-data-sync-deploy-service.yml
├── 📁 pipeline_configs/             # ETL pipeline settings and constants
│   └── ⚙️ __init__.py
├── 📁 routes/                       # API endpoint definitions
│   ├── 🛣️ run_cron_job.py
│   └── 🛣️ run_historic_job.py
├── scripts/                          # Core business logic
│   ├── run_etl_pipeline.py           # Main orchestrator class
│   ├── relationaldb_config.py        # Database connection & schema logic
│   ├── data_extractor.py             # Pulls data from MySQL
│   ├── data_loader.py                # Loads data into BigQuery
│   ├── data_transformer.py           # Data cleaning & formatting
│   └── gcp_manager.py                # GCS & BigQuery client management
├── 📁 utils/                         # Shared utility functions
│   ├── 🪣 auth.py                    # Authentication logic
│   ├── 🪣 gcpcloud_postgres_conn.py.py  # Logic for insert etl logs
│   └── 🪵 logger.py                  # Centralized logging configuration
└── ♾️ .gitlab-ci.yml                 # Main GitLab CI/CD configuration
```
---

## 🛠️ Prerequisites

-   **Python 3.12+**
-   **Docker**
-   **Google Cloud Platform** account with:
    -   BigQuery enabled
    -   Cloud Storage enabled
    -   Service Account with appropriate permissions (`roles/bigquery.dataEditor`, `roles/storage.objectAdmin`, etc.)
-   **MySQL Database** access

## ⚙️ Configuration

The application is configured using Environment Variables. These must be set in your `.env` file for local development or in your Kubernetes deployment manifests.

### Required Environment Variables

| Variable | Description | Example |
| :--- | :--- | :--- |
| `CLIENT_NUMBER` | Unique identifier for the client | `12345` |
| `CLIENT_REGION` | GCP Region for resources | `us-central1` |
| `CLIENT_TIMEZONE` | Timezone for scheduling & logging | `Asia/Kolkata` |
| `CLIENT_PROJECT_ID` | GCP Project ID | `my-gcp-project` |
| `RDMS_HOST` | Hostname/IP of the Source MySQL DB | `10.0.0.5` |
| `RDMS_PORT` | Port of the Source MySQL DB | `3306` |
| `RDMS_USERNAME` | Username for MySQL | `etl_user` |
| `RDMS_PASSWORD` | Password for MySQL | `secret_password` |
| `USE_SSL` | Enable SSL for MySQL connection | `True` or `False` |
| `CRONJOB_TASK_PAYLOAD` | JSON string definition for the job | `{"ETL_SOURCE": "MySQL", "WINDOW_DAYS": 1}` |

### Optional / Sensitive

-   `SERVICE_ACCOUNT_KEY`: Path or content of the GCP Service Account JSON key.
-   `USE_SSL`: Set to `True` if the DB connection requires SSL.

## 🏃 Usage

### Local Development

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Amirazizgithub/ETL-Pipeline-RelationalDB.git
    cd etl-pipeline-relationaldb
    ```

2.  **Install dependencies:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

3.  **Set up Environment Variables:**
    Create a `.env` file in the root directory and add the variables listed above.

4.  **Run the CronJob trigger:**
    You need to mock the `CRONJOB_TASK_PAYLOAD` environment variable.
    ```bash
    # Linux/Mac
    export CRONJOB_TASK_PAYLOAD='{"START_DATE": "", "END_DATE": "", "ETL_SOURCE": "MySQL", "WINDOW_DAYS": 30}'
    python -m routes.run_cron_job

    # Windows PowerShell
    $env:CRONJOB_TASK_PAYLOAD='{"START_DATE": "", "END_DATE": "", "ETL_SOURCE": "MySQL", "WINDOW_DAYS": 30}'
    python -m routes.run_cron_job
    ```

---

### Docker Deployment

1.  **Build the image:**
    ```bash
    docker build -t etl-pipeline-relationaldb .
    ```

2.  **Run the container:**
    ```bash
    docker run --env-file .env \
      -e CRONJOB_TASK_PAYLOAD='{"START_DATE": "", "END_DATE": "", "ETL_SOURCE": "MySQL", "WINDOW_DAYS": 30}' \
      etl-pipeline-relationaldb python -m routes.run_cron_job
    ```

## ☸️ Kubernetes Deployment

Manifests are located in the `k8s-pipelines/` directory.

1.  **Update ConfigMaps/Secrets** with your environment variables.
2.  **Apply manifests:**
    ```bash
    kubectl apply -f k8s-pipelines/cronjob-data-sync-deploy-service.yml
    ```

## 🧩 Schema Mapping Logic

The pipeline uses `RelationalDBConfig` to map specific MySQL types to BigQuery:

-   `int`, `tinyint`, `bigint` ⮕ `INT64`
-   `decimal`, `numeric` ⮕ `NUMERIC`
-   `float`, `double` ⮕ `FLOAT64`
-   `datetime` ⮕ `DATETIME`
-   `timestamp` ⮕ `TIMESTAMP`
-   `varchar`, `text`, `enum` ⮕ `STRING`
-   `json` ⮕ `JSON`

It also automatically detects `primary_key` and `sync_date_column` (looking for columns like `updated_at`, `modified_on`, etc.) to drive the incremental sync logic.

## 🚀 Deployment

The project handles containerization using Docker and runs scheduled operations mapped through GitLab CI/CD pipelines deploying to a **Kubernetes cluster**.

- **Cron Jobs**: Run routinely to fetch incremental patches from Microsoft SQL to keep BigQuery updated (managed via `cronjob-data-sync-deploy-service.yml`).
- **Historic Data Syncs**: Exposed via FastAPI on Kubernetes to ingest historical database structures entirely (managed via `historic-data-deploy-service.yml`).

## 🛠️ CI/CD Pipelines

The project includes GitLab CI/CD pipelines for:

- **Development**: Code formatting and linting
- **Staging**: Automated testing and deployment
- **Production**: Full deployment pipeline

Pipelines are triggered based on branch:
- development branch: Development pipeline
- staging branch: Staging deployment
- production branch: Production deployment

### Logging

The application uses structured logging configured in app/utils/logger.py. Logs include:
- API request/response details
- Error tracking
- Performance metrics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: git checkout -b feature/your-feature
3. Make your changes and add tests
4. Run tests: pytest
5. Format code: black .
6. Commit your changes: git commit -am 'Add your feature'
7. Push to the branch: git push origin feature/your-feature
8. Submit a pull request

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## ⚙️ Support

For support or questions:
- Create an issue in the repository
- Contact the development team
