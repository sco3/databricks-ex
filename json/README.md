# Databricks JSON Processing Example

This project demonstrates how to process JSON data in Databricks, from ingestion to structured analysis. It includes shell scripts for managing Databricks resources, a Databricks notebook for the processing logic, and sample data.

## Files

### Shell Scripts

- `00-create-volume.sh`: Creates a Databricks volume named `dz-vol-json` to store the data.
- `02-copy-local-data-to-volume.sh`: Copies the local `data.json` file to the `dz-vol-json` volume in Databricks.
- `06.01-export-ws-py.sh`: Exports a Databricks workspace notebook to the local file `json.py`.
- `99-clean.sh`: Deletes the `dz-vol-json` volume from Databricks.

### Python Script

- `json.py`: A Databricks notebook (exported as a Python file) that performs the following steps:
    1.  **Reads JSON data:** Ingests raw JSON data from the Databricks volume.
    2.  **Creates a bronze table:** Saves the raw data into a Delta table named `dz.dz.data_json_bronze`.
    3.  **Decodes base64:** Decodes the `key` and `value` columns, which are base64 encoded.
    4.  **Creates a decoded table:** Saves the decoded data into a new Delta table named `dz.dz.data_json_bronze_decoded`.
    5.  **Parses JSON fields:** Extracts specific fields from the JSON string in the `decoded_value` column.
    6.  **Infers schema:** Uses `schema_of_json` to infer the schema of the JSON data.
    7.  **Creates a structured table:** Parses the JSON string into a structured format and saves it in the `dz.dz.data_json_bronze_decoded_struct` table.
    8.  **Queries structured data:** Demonstrates how to query the structured data, including nested fields and arrays.
    9.  **Variant type:** Shows how to use the `parse_json` function to work with semi-structured data, creating the `dz.dz.data_json_variant` table.

### Data Files

- `data.json`: The main data file, containing one JSON object per line. Each object represents a message with metadata and a base64 encoded `value` which is a JSON object containing order details.
- `data.json.array`: A JSON array of objects.
- `data.json.perline`: JSON objects, one per line.
- `data.json.single`: A single JSON object.
- `data.original.json`: The original, unmodified JSON data.
- `data.decoded.json`: The decoded JSON data.
- `data.decoded.schema`: The schema of the decoded JSON data.

## Usage

1.  **Create the volume:**
    ```bash
    ./00-create-volume.sh
    ```
2.  **Copy data to the volume:**
    ```bash
    ./02-copy-local-data-to-volume.sh
    ```
3.  **Run the notebook:**
    Import and run the `json.py` notebook in your Databricks workspace.
4.  **Clean up:**
    ```bash
    ./99-clean.sh
    ```
