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
COMMAND="java -XX:+IgnoreUnrecognizedVMOptions -XX:UseAVX=2 ${JAVA_ARGUMENTS} -jar pgadapter.jar ${ARGUMENTS}"
echo $COMMAND
exec $COMMAND
