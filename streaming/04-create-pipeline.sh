#!/usr/bin/env -S bash

set -xueo pipefail


databricks pipelines create --json @streaming.json