#!/usr/bin/env -S bash

set -xueo pipefail

databricks workspace mkdirs /Users/texter.texel@gmail.com/streaming

databricks workspace import \
  /Users/texter.texel@gmail.com/streaming/streaming.sql \
  --file streaming.sql \
  --format SOURCE \
  --language SQL