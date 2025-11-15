#!/usr/bin/env -S bash

source .env

set -xueo pipefail

yq -i ".warehouse_id=\"${WAREHOUSE_ID}\"" 02-insert.json
yq -i '.statement=load("02-insert.sql")' 02-insert.json

databricks api post /api/2.0/sql/statements \
--profile DEFAULT \
--json @02-insert.json > /tmp/02-response.json

stmt=$(yq '.statement_id' /tmp/02-response.json)

while true; do
   state=$(databricks api get /api/2.0/sql/statements/01f0c221-6285-1a82-b449-c1d1e4f51bb5 | yq '.status.state')
   if [ "$state" == "PENDING" ]; then
      sleep 1
   else
      echo $state
      exit
   fi
done
