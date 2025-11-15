# Databricks Data Ingestion

This directory contains scripts and data for ingesting data into Databricks. The process involves creating a volume, copying local data to that volume, and then inserting the data into a table.

## Files:

- `00-create-volume.sh`: Script to create a Databricks volume.
- `01-copy-local-data-to-volume.sh`: Script to copy `data.csv` from the local machine to the created Databricks volume.
- `02-insert.sh`: Script to execute the SQL command in `02-insert.sql` to insert data from the volume into a Databricks table.
- `02-insert.json`: Configuration file for the insert operation (e.g., specifying table name, volume path).
- `02-insert.sql`: SQL script containing the `INSERT` statement to load data from the volume into a table.
- `data.csv`: The sample data file to be ingested.
- `README.md`: This file.

## Usage:

Follow the scripts in numerical order to perform the data ingestion:

1.  **Create Volume**:
    ```bash
    ./00-create-volume.sh
    ```
2.  **Copy Data to Volume**:
    ```bash
    ./01-copy-local-data-to-volume.sh
    ```
3.  **Insert Data into Table**:
    ```bash
    ./02-insert.sh
    ```

Ensure you have the necessary Databricks CLI and authentication configured before running these scripts.
