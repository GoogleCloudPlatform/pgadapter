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
COMMAND="java -jar ${JAVA_ARGUMENTS} /home/pgadapter/pgadapter.jar ${ARGUMENTS}"
echo $COMMAND
eval " $COMMAND"
