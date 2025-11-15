# Databricks Ingestion Example

This project demonstrates a simple data ingestion workflow for Databricks.

## Getting Started

1.  **Configure Environment:**
    Copy the environment template and fill in your Databricks connection details:
    ```bash
    cp .databricks.env.template .env
    ```

2.  **Run Ingestion:**
    Execute the scripts in the `ingestion` directory in numerical order:
    ```bash
    cd ingestion
    ./00-create-volume.sh
    ./01-copy-local-data-to-volume.sh
    ./02-insert.sh
    ```

## Project Structure

-   `.databricks.env.template`: Template for environment variables.
-   `.gitignore`: Files and directories ignored by git.
-   `README.md`: This file.
-   `ingestion/`: Contains scripts and data for ingesting data into Databricks. The process involves creating a volume, copying local data to that volume, and then inserting the data into a table.