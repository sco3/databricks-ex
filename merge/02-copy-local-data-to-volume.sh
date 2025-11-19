#!/usr/bin/env -S bash

set -xueo pipefail

volshort="dz-vol-merge"
vol="/Volumes/dz/dz/$volshort"

databricks volumes create dz dz $volshort MANAGED || echo "Volume $vol already exists"

databricks fs cp source_table.csv dbfs:$vol/source_table.csv --overwrite
databricks fs cp target_table.csv dbfs:$vol/target_table.csv --overwrite

databricks fs ls dbfs:$vol

echo finished
