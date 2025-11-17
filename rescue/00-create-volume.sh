#!/usr/bin/env -S bash

set -xueo pipefail
databricks volumes create dz dz dz-vol-rescue MANAGED || echo "Volume already exists"

echo finished
