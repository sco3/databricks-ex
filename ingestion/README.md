# Databricks Data Ingestion

This directory contains scripts and data for ingesting data into Databricks. The process involves creating a volume, copying local data to that volume, and then inserting the data into a table.

## Files:

- `00-create-volume.sh`: Script to create a Databricks volume.
- `01-copy-local-data-to-volume.sh`: Script to copy `data.csv` from the local machine to the created Databricks volume.
- `01.01-convert.sh`: Script to convert `data.csv` to `data.parquet`.
- `02.01-create-table-as.sh`: Script to create a table in Databricks from the data in the volume.
- `02.02-copy-into.sh`: Script to copy data from the volume into a Databricks table.
- `03-verify.sh`: Script to verify the data in the table.
- `ingest_with_python.py`: Python script for ingesting data.
- `main.py`: Main Python script.
- `data.csv`: The sample data file to be ingested.
- `data.parquet`: The sample data file in Parquet format.
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
3.  **Convert data to parquet**:
    ```bash
    ./01.01-convert.sh
    ```
4.  **Create Table and Insert Data**:
    ```bash
    ./02.01-create-table-as.sh
    ```
5.  **Copy Into existing table**:
    ```bash
    ./02.02-copy-into.sh
    ```
6.  **Verify**:
    ```bash
    ./03-verify.sh
    ```

Ensure you have the necessary Databricks CLI and authentication configured before running these scripts.


** Final Result: 3 (from CTAS)+3 (from COPY INTO)=6 Records **