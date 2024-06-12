#!/bin/bash

# shellcheck disable=SC1090
OUTPUT=$(source ../"$1".sh)
if [ "$OUTPUT" != "$2" ]; then
  export RESULT=1
  export MESSAGE="
  Received unexpected output:
$OUTPUT

  Expected:
$2"
fi
