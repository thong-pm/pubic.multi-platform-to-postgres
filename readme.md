# Multi-Pipeline Orchestration Pipeline

This project orchestrates multiple Singer data pipelines ([pipelinewise-singer-python](https://github.com/transferwise/pipelinewise-singer-python)) by TransferWise (now Wise). Extract data from various sources and load it into a PostgreSQL database using a central scheduler.  
The views are created and maintain by dbt.

Current available pipelines:
- Hubspot
- Xero
- Wrike

## I. How the Pipeline Works (Conceptual Overview)

When execute:
```bash
    $ ./run.sh
```  

This script will:

- ✅ Check for Poetry.
- ✅ Ensure dependencies are installed (`poetry lock` and `poetry install` if needed).
- ✅ Activate the Poetry virtual environment.
- ✅ Start the `scheduler.py` script.

---

### Scheduler Behavior

The `scheduler.py` script will:

🔍 Discover the pipeline directories `(pipeline_*_to_postgres)`.  

▶️ Run each discovered pipeline sequentially, waiting for each to complete before moving on.  

📝 Print logs prefixed with `[pipeline_name]` and `[system]` (e.g., `[pipeline_xero_to_postgres]`, `[xero]`).  

📊 After all pipelines finish, it will run the associated `dbt` transformations defined in the dbt project.  

🔁 After completing a full sequence run of all pipelines and dbt, it will wait for the duration specified by `PIPELINE_INTERVAL_SECONDS` (from the .env file) before starting the next sequence.

---

### How Each Individual Works 

This project utilizes the Singer specification for data extraction and loading. The general flow for each pipeline tap is:

1️⃣  **Tap starts** → Logs startup message.
2️⃣  **Tap discovers catalog** → Lists available data streams (tables/endpoints).
3️⃣  **Schema is sent** → Defines the structure (columns, types) for selected streams.
4️⃣  **Catalog/Metadata is applied** → The tap uses a catalog file (based on discovery) to know which streams/fields are selected for replication.
5️⃣  **State is checked** → Reads the last saved state (bookmark) to determine where to resume syncing for incremental updates.
6️⃣  **Data is extracted** → Fetches data for selected streams, emitting `RECORD` messages conforming to the schema.
7️⃣  **State is updated** → Emits `STATE` messages periodically and at the end, saving progress for the next run.

The `target-postgres` component reads the `SCHEMA`, `RECORD`, and `STATE` messages and handles writing the data to the appropriate PostgreSQL tables.

## II. Deploy & Dev Instructions

### ***Prerequisites***

*   **Python:** Version 3.9 or higher.
*   **Poetry:** MUST BE verison 2.1.2 - Python dependency management tool.
*   **DBT:** Currently use verison 1.5.7 for both dbt-core and dbt-postgres
*   **PostgreSQL:** PostgreSQL Version 16. Hosted on render.com

See the `pyproject.toml` for dependency details. 

### 1. Set up Environment

> #### RENDER:
#### ***NOTE: the repo is built to be deployed on `Render Background Worker with Persistent Disk`***

*   **Build Command:** 
    ```bash
    $ poetry install
    ```
*   **Start Command:** 
    ```bash
    $ ./run.sh
    ```
*  **Persistent Disk Path:**
    ```bash
    /var/data
    ```
*  **Environment:**
    ```bash
    HUBSPOT_TOKEN=""
    WRIKE_TOKEN=""
    XERO_CLIENT_ID=""
    XERO_CLIENT_SECRET=""
    XERO_REFRESH_TOKEN=""
    POETRY_VERSION = "2.1.2" # need to specify if deploy on Render
    PIPELINE_INTERVAL_SECONDS="300"
    DISK_STORAGE_PATH="/var/data" # persistent disk path
    DB_HOST=""
    DB_DATABASE=""
    DB_PORT="5432"
    DB_USERNAME=""
    DB_PASSWORD=""
    ```

> #### LOCAL:
#### ***NOTE: The pipeline has only been tested on WSL***
1.  **Clone Repository:**
    ```bash
    git clone https://github.com/thong-pm/pipeline-multi-platform.git
    cd pipeline-multi-platform
    ```

2.  **Configure Environment:**
    *   Copy the sample environment file:
        ```bash
        cp .env.sample .env
        ```
    *   Edit the `.env` file in the **project root directory**. Fill in necessary credentials:
        *   `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD` for the target PostgreSQL database.
        *   API credentials required by the taps (e.g., `HUBSPOT_API_KEY`, `XERO_CLIENT_ID`, `XERO_CLIENT_SECRET`, `XERO_REFRESH_TOKEN`, `WRIKE_PERMANENT_TOKEN`). These are referenced by the individual `config.yml` files.
        *   Optionally, `PIPELINE_INTERVAL_SECONDS` to control the wait time between full sequence runs (defaults to 300 seconds / 5 minutes).

### 2. Configure Individual Pipelines
   
### i. Check listted
1. In each runner/__init__.py update the following line::  system = "{platform}"  e.g. system = "hubspot"
2. Rename tap_{platform}  e.g.ta p_hubspot 
3. In tap_{platform}/schemas -> update the json files, each files is a schema/table
4. In tap_{platform}/vendored -> Only consider it when mismatch version bugs
5. Most of the time you don't need to touch tap_{platform}/internal.py which defines:
    🔄 Functions for API pagination handling
    🕒 Helpers for rate limiting or retries
    🧹 Data cleaning or transformation utilities
    🔍 Logic for constructing API URLs or query params
    🧩 Shared logic used across fetch.py or __init__.py
6. Review and update the `config.yml` file inside each `pipelines/pipeline_*/` directory. These files define:
    *   Which environment variables map to tap configuration settings (like API keys).
    *   Which data streams (tables) should be selected for replication using the `select:` key  
    For example:
    ```bash
    select:
        - deals
        - deals_companies
    ```
### ii. Select the the pipeline to run in `scheduler.py`:
```bash
PIPELINE_NAMES =  [
    "pipeline_hubspot_to_postgres",
    "pipeline_wrike_to_postgres",
    # "pipeline_xero_to_postgres", #comment out if not intend for running
]
```
### iii. Run the Pipeine:

*   Run this single command line:
```bash
    ./run.sh
```
### runner.sh script handles:

- Verifies Poetry is installed and on the correct version.
- Detects changes to pyproject.toml and installs all dependencies for the scheduler and *all* pipelines, defined in the root `pyproject.toml`. It will also create the necessary `tap-*` command-line executables.
- Installs or updates the poetry.lock file if necessary.
- Initiates the full ETL + modeling sequence via running `sheduler.py`

### 4. DEBUG
    
### Ensure Python Package Structure:
This project relies on specific directory naming and `__init__.py` files for Python's import system and Poetry's script generation. Verify or create the following:
*   Directories **must** use underscores: `pipelines/pipeline_hubspot_to_postgres`, `pipelines/pipeline_wrike_to_postgres`, `pipelines/pipeline_xero_to_postgres`. (Rename using `mv old-name new_name` if needed).
*   Ensure these (empty) files exist:
    ```bash
    touch pipelines/__init__.py
    touch pipelines/pipeline_hubspot_to_postgres/__init__.py
    touch pipelines/pipeline_wrike_to_postgres/__init__.py
    touch pipelines/pipeline_xero_to_postgres/__init__.py
    # (Should already have __init__.py inside each tap_* (for authorization) and runner directory (main entry))
    ```
### Ensure poetry version 
MUST BE verison `2.1.2` or higher.
    Check for poetry version
```bash
poetry --version
```
If Poetry is not installed or the version is too low.  
Install or upgrade to the latest Poetry version using the official method:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```
Try a clean install
```bash
rm -rf $(poetry env info --path) && rm -f poetry.lock && poetry install
```

### 5. Directory Structure Overview

#### Root Directory (`pipeline-multi-platform/`):

- `pyproject.toml`: Defines project dependencies (shared libs, taps via scripts), metadata, and scripts for the entire project.
- `poetry.lock`: Locks dependency versions for reproducibility.
- `.env`: Stores secrets and configuration (DB credentials, API keys). **Do not commit this file.**
- `scheduler.py`: The main script that orchestrates sequential pipeline runs.
- `run.sh`: The entry point script for setup and execution.
- `pipelines/`: Contains the code for individual pipelines.
- `dbt/`: Contains the code for dbt project.

#### Pipeline Directories (`pipelines/pipeline_[system]_to_postgres/`):

- `config.yml`: Configures the specific tap (env var mapping, stream selection).
- `runner/`: Contains `runner/__init__.py`, Contains the `main()` entry point. Also generates configs and executes the `tap | target` command for this specific pipeline.
- `tap_name/internal.py`: Contains the core Singer tap logic:
- `fetch.py`, `utility.py`: Handle API syncing logic.
- `schemas/`: Define schema will be loaded into Postgres for each data endpoints.
- `run.sh`: A execution scirpt inside each pipelines. A simple script executed by the scheduler.

#### DBT Project Structure (`dbt/`):

- `dbt_project.yml`  
  Defines the dbt project configuration:
  - Model paths
  - Materialization settings (e.g., view/table)
  - Model namespaces (`novellidbt.wrike`, `novellidbt.hubspot`, etc.)

- `profiles.yml`: Contains connection settings for dbt to connect to the target database (e.g., Postgres).  
    The `profile` value in `dbt_project.yml` must match a profile defined here.

- `models/`  
  Contains SQL transformation logic, organized by platform:
  - `wrike/`: SQL models for Wrike (e.g., `task_duration.sql`)
  - `hubspot/`: SQL models for HubSpot
  - `xero/`: SQL models for Xero

- `macros/`: Currently used for print custom schema name
