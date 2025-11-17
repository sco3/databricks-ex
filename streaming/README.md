# Databricks Streaming Exercise

This project demonstrates how to set up a Databricks streaming pipeline that reads Parquet files from a Databricks Volume and ingests them into a streaming table using Delta Live Tables (DLT).

## Overview

The process is as follows:
1.  A local CSV file (`data.csv`) is converted to a Parquet file.
2.  The Parquet file is uploaded to a Databricks Volume.
3.  A Databricks workspace is prepared by creating a directory and importing a SQL script.
4.  A Delta Live Tables pipeline is created and started. The pipeline reads the Parquet files from the volume in a streaming fashion and loads the data into a target table.
5.  Verification scripts in Python and Go are provided to query the final table and verify the data has been ingested.

## Prerequisites

Before you begin, ensure you have the following installed:
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)
- [DuckDB CLI](https://duckdb.org/docs/installation/index.html)
- [Go](https://go.dev/doc/install)
- [Python](https://www.python.org/downloads/)
- [uv](https://github.com/astral-sh/uv) (for the Python verification script)

You also need a Databricks workspace and the appropriate permissions to create volumes, clusters, and pipelines.

## Environment Variables

The verification scripts require environment variables to connect to your Databricks workspace. Create a `.env` file in the root of the project with the following content:

```bash
DATABRICKS_HOST=...
DATABRICKS_TOKEN=...
WAREHOUSE_ID=... # Required for the Python script
DATABRICKS_HTTP_PATH=... # Required for the Go script
```

## File Descriptions

| File                            | Description                                                                                                                               |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `data.csv`                      | Sample raw data in CSV format.                                                                                                            |
| `data.parquet`                  | The sample data converted to Parquet format.                                                                                              |
| `00-create-volume.sh`           | Creates a `MANAGED` volume named `dz-vol-streaming` in the `dz.dz` schema.                                                                  |
| `01.01-convert.sh`              | Uses `duckdb` to convert `data.csv` to `data.parquet`.                                                                                    |
| `02-copy-local-data-to-volume.sh` | Copies the local `data.parquet` file to the created Databricks Volume. It generates a random name for the uploaded file.                  |
| `03-create-workspace.sh`        | Creates a directory in your Databricks workspace and imports the `streaming.sql` file.                                                    |
| `04-create-pipeline.sh`         | Creates a new Delta Live Tables pipeline using the configuration from `streaming.json`.                                                   |
| `streaming.json`                | Configuration file for the DLT pipeline. Defines the pipeline name, target catalog/schema, and path to the notebook.                      |
| `streaming.sql`                 | The SQL script that defines the streaming logic. It creates a streaming table that reads from the files in the target volume.             |
| `05-verify.py`                  | A Python script to query the `dz.dz.data_streaming` table and print its contents to verify the pipeline worked.                            |
| `main.go`                       | A Go program that does the same as the Python verification script.                                                                        |


## Setup and Execution

Run the scripts in the following order:

1.  **Convert CSV to Parquet:**
    ```bash
    ./01.01-convert.sh
    ```

2.  **Create Volume and Upload Data:**
    This script creates the volume (if it doesn't exist) and uploads the `data.parquet` file.
    ```bash
    ./02-copy-local-data-to-volume.sh
    ```

3.  **Setup Workspace:**
    This script will create a folder in your user directory and upload the `streaming.sql` notebook.
    **Note:** You might need to edit the workspace path inside `03-create-workspace.sh` and `streaming.json` to match your Databricks username.
    ```bash
    ./03-create-workspace.sh
    ```

4.  **Create and Run the Pipeline:**
    This will create and start the DLT pipeline.
    ```bash
    ./04-create-pipeline.sh
    ```
    You can monitor the pipeline's progress in the Databricks UI under "Delta Live Tables".

## Verification

Once the pipeline has successfully run, you can verify the data in the target table using either the Python or Go scripts.

**Python Verification:**

First, install the dependencies:
```bash
uv sync 
```

Then run the script:
```bash
uv run ./05-verify.py
```

**Go Verification:**

First, install the dependencies:
```bash
go mod tidy
```

Then run the script:
```bash
go run main.go
```

Both scripts should print the contents of the `dz.dz.data_streaming` table, confirming that the streaming pipeline has ingested the data correctly.
