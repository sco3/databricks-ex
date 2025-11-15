#!/usr/bin/env -S bash

set -xueo pipefail
databricks fs cp data.csv dbfs:/Volumes/dz/dz/dz-vol/data.csv
databricks fs ls dbfs:/Volumes/dz/dz/dz-vol

echo finished
