#!/usr/bin/env -S bash

set -xueo pipefail

catalog="dz"
schema="dz"
volshort="dz-vol-merge"
vol="/Volumes/$catalog/$schema/$volshort"

databricks sql -e "CREATE OR REPLACE TABLE $catalog.$schema.source_table USING CSV LOCATION '$vol/source_table.csv' OPTIONS (header = 'true', inferSchema = 'true');"
databricks sql -e "CREATE OR REPLACE TABLE $catalog.$schema.target_table USING CSV LOCATION '$vol/target_table.csv' OPTIONS (header = 'true', inferSchema = 'true');"

echo "Tables created successfully."
