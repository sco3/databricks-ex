#!/usr/bin/env -S bash

set -xueo pipefail

# Delete volume
echo "Deleting volume dz.dz.dz-vol-json..."
databricks volumes delete dz.dz.dz-vol-json || echo "Volume dz.dz.dz-vol-streaming did not exist."


echo "Cleanup complete."
