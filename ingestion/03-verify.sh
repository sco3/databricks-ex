#!/usr/bin/env -S bash

source .env

set -xueo pipefail

yq -i ".warehouse_id=\"${WAREHOUSE_ID}\"" 03-verify.json

databricks api post /api/2.0/sql/statements \
--profile DEFAULT \
--json @03-verify.json > /tmp/03-response.json

stmt=$(yq -r '.statement_id' /tmp/03-response.json)

while true; do
   databricks api get /api/2.0/sql/statements/$stmt > /tmp/03-response.json
   state=$(  yq '.status.state' /tmp/03-response.json )
   if [ "$state" == "RUNNING" ]; then
      sleep 1
   else
      echo $state
      yq -P -o yaml '.result' /tmp/03-response.json
      exit
   fi
done
