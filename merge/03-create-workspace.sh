#!/usr/bin/env -S bash

set -xueo pipefail

databricks workspace mkdirs /Users/texter.texel@gmail.com/merge

databricks workspace import \
  /Users/texter.texel@gmail.com/merge/merge.py \
  --file merge_notebook.py \
  --format SOURCE \
  --language PYTHON