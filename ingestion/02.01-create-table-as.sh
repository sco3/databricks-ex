#!/usr/bin/env -S bash

source .env

set -xueo pipefail

yq -i ".warehouse_id=\"${WAREHOUSE_ID}\"" 02.01-create-table-as.json
yq -i '.statement=load("02.01-create-table-as.sql")' 02.01-create-table-as.json

databricks api post /api/2.0/sql/statements \
--profile DEFAULT \
--json @02.01-create-table-as.json > /tmp/02-response.json

stmt=$(yq -r '.statement_id' /tmp/02-response.json)

while true; do
   state=$(databricks api get /api/2.0/sql/statements/$stmt | yq '.status.state')
   if [ "$state" == "PENDING" ]; then
      sleep 1
   else
      echo $state
      exit
   fi
done
