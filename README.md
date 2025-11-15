# Databricks Ingestion Example

This project demonstrates a simple data ingestion workflow for Databricks.

## Setup

1.  **Environment Variables:**

    This project uses a `.env` file to manage Databricks connection details. Create a `.env` file in the root of the project by copying the template:

    ```bash
    cp .databricks.env.template .env
    ```

    Then, edit the `.env` file and provide your Databricks host, token, and target warehouse name.

    *   `.databricks.env.template`: This is a template file for the environment variables. **Do not store credentials in this file.**
    *   `.env`: This file is used to store your Databricks credentials and is ignored by git.

2.  **Run the Ingestion Scripts:**

    The `ingestion` directory contains the scripts to create a volume and upload data.

    *   `ingestion/00-create-volume.sh`: Creates a Databricks volume named `dz-vol`.
    *   `ingestion/01-copy-local-data-to-volume.sh`: Copies the `ingestion/data.csv` file to the `dz-vol` volume.

    To run the scripts:

    ```bash
    cd ingestion
    ./00-create-volume.sh
    ./01-copy-local-data-to-volume.sh
    ```

## Data

The `ingestion/data.csv` file contains sample data that is uploaded to the Databricks volume.
