#!/usr/bin/env -S bash

set -xueo pipefail

volshort="dz-vol-streaming"
vol="/Volumes/dz/dz/$volshort"

name=$(dd  if=/dev/random of=/dev/stdout count=4 bs=1 status=none | xxd -p)

databricks volumes create dz dz $volshort MANAGED || echo "Volume $vol already exists"

databricks fs cp data.parquet dbfs:$vol/$name.parquet

databricks fs ls dbfs:$vol

echo finished
