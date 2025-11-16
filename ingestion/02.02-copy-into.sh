#!/usr/bin/env -S bash

source .env

set -xueo pipefail

yq -i ".warehouse_id=\"${WAREHOUSE_ID}\"" 02.02-copy-into.json
yq -i '.statement=load("02.02-copy-into.sql")' 02.02-copy-into.json

databricks api post /api/2.0/sql/statements \
--profile DEFAULT \
--json @02.02-copy-into.json > /tmp/02-response.json

stmt=$(yq -r '.statement_id' /tmp/02-response.json)

while true; do
   state=$(databricks api get /api/2.0/sql/statements/$stmt | yq '.status.state')
   if [[ "$state" == "PENDING" || "$state" == "RUNNING" ]]; then
      sleep 1
   else
      echo $state
      cat /tmp/02-response.json
      exit
   fi
done
