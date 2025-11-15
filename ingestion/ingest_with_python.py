
import os
import time
from dotenv import load_dotenv # Added
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.sql import StatementState
from databricks.sdk.errors import platform as platform_errors

load_dotenv() 

warehouse_id = os.getenv("WAREHOUSE_ID")
if not warehouse_id:
    raise ValueError("WAREHOUSE_ID environment variable must be set.")

CATALOG_NAME = "dz"
SCHEMA_NAME = "dz"
VOLUME_NAME = "dz-vol"
LOCAL_DATA_FILE = "data.csv"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"
DBFS_DEST_PATH = f"{VOLUME_PATH}/{LOCAL_DATA_FILE}"
TABLE_NAME = "data"
SQL_FILE = "02-insert.sql"


def main():
    """
    Main function to orchestrate the data ingestion process.
    """
    print("Starting Databricks data ingestion with Python...")
    w = WorkspaceClient()

    create_volume(w)

    upload_data_to_volume(w)

    insert_data_into_table(w)

    print("\nData ingestion process completed successfully!")
    print(f"Check the table '{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}' in your Databricks workspace.")


def create_volume(w: WorkspaceClient):
    """
    Creates a new managed volume in Databricks if it doesn't already exist.
    """
    full_volume_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}"
    print(f"Attempting to create volume: {full_volume_name}...")
    try:
        w.volumes.create(
            catalog_name=CATALOG_NAME,
            schema_name=SCHEMA_NAME,
            name=VOLUME_NAME,
            volume_type=VolumeType.MANAGED,
        )
        print(f"Volume '{full_volume_name}' created successfully.")
    except platform_errors.ResourceAlreadyExists:
        print(f"Volume '{full_volume_name}' already exists. Skipping creation.")
    except Exception as e:
        print(f"An error occurred while creating volume: {e}")
        raise


def upload_data_to_volume(w: WorkspaceClient):
    """
    Uploads the local data.csv file to the specified volume in DBFS.
    """
    print(f"Uploading '{LOCAL_DATA_FILE}' to '{DBFS_DEST_PATH}'...")
    try:
        with open(LOCAL_DATA_FILE, "rb") as f:
            w.dbfs.upload(path=DBFS_DEST_PATH, src=f, overwrite=True)
        print("File uploaded successfully.")

        # Verify upload by listing the file
        print("Verifying file upload...")
        for item in w.dbfs.list(VOLUME_PATH):
            if item.path == DBFS_DEST_PATH:
                print(f"Verified: Found '{item.path}' ({item.file_size} bytes)")
                return
        raise FileNotFoundError(f"File not found in volume after upload: {DBFS_DEST_PATH}")
    except Exception as e:
        print(f"An error occurred during file upload: {e}")
        raise


def insert_data_into_table(w: WorkspaceClient):
    """
    Executes a SQL statement to create a table from the data in the volume.
    """
    print(f"Executing SQL from '{SQL_FILE}' to create table '{TABLE_NAME}'...")
    try:
        with open(SQL_FILE, "r") as f:
            sql_statement = f.read()

        print("Submitting SQL statement...")
        resp = w.statement_execution.execute_statement(
            statement=sql_statement,
            warehouse_id=warehouse_id,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME,
        )
        statement_id = resp.statement_id
        print(f"SQL statement submitted with ID: {statement_id}")

        # Poll for statement completion
        while True:
            status = w.statement_execution.get_statement(statement_id).status
            current_state = status.state
            print(f"Statement status: {current_state}")

            if current_state in (
                StatementState.SUCCEEDED,
                StatementState.CANCELED,
                StatementState.CLOSED,
                StatementState.FAILED,
            ):
                break
            time.sleep(2)

        if status.state == StatementState.SUCCEEDED:
            print("SQL statement executed successfully.")
        else:
            error_msg = status.error.message if status.error else "Unknown error"
            raise Exception(f"SQL statement failed with status '{status.state}': {error_msg}")

    except Exception as e:
        print(f"An error occurred during SQL execution: {e}")
        raise


if __name__ == "__main__":
    main()
