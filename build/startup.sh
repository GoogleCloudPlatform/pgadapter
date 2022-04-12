#!/bin/bash

ARGUMENTS=""
JAVA_ARGUMENTS=""

for var in "$@"
do
  if [[ $var == "-D"* ]]; then
    JAVA_ARGUMENTS="${JAVA_ARGUMENTS} $var"
  else
    ARGUMENTS="${ARGUMENTS} $var"
  fi
done

cd /home/pgadapter
COMMAND="java ${JAVA_ARGUMENTS} -cp \"pgadapter.jar:lib/*\" com.google.cloud.spanner.pgadapter.Server ${ARGUMENTS}"
echo $COMMAND
eval " $COMMAND"
