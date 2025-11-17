#!/usr/bin/env -S bash

set -xueo pipefail

# Delete volume
echo "Deleting volume dz.dz.dz-vol-streaming..."
databricks volumes delete dz.dz.dz-vol-streaming || echo "Volume dz.dz.dz-vol-streaming did not exist."

# Delete pipeline
echo "Deleting pipeline 'streaming'..."
pipeline_id=$(databricks pipelines list-pipelines --output JSON | jq -r 'map(select(.name == "streaming")) | .[0].pipeline_id')
if [ -n "$pipeline_id" ]; then
  databricks pipelines delete "$pipeline_id" || echo "Failed to delete pipeline '$pipeline_id'."
else
  echo "Pipeline 'streaming' not found."
fi

# Delete workspace directory
user_name=$(databricks current-user me | jq -r .userName)
echo "Deleting workspace directory /Users/${user_name}/streaming..."
databricks workspace delete --recursive /Users/${user_name}/streaming || echo "Workspace directory /Users/${user_name}/streaming did not exist or could not be deleted."

echo "Cleanup complete."
