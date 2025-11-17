#!/usr/bin/env -S bash

set -xueo pipefail
databricks volumes create dz dz dz-vol-streaming MANAGED || echo "Volume already exists"

echo finished
