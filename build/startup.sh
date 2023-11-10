#!/bin/bash

ARGUMENTS="-p emulator-project -i test-instance -r autoConfigEmulator=true -c \"\" -x"
JAVA_ARGUMENTS=""

for var in "$@"
do
  if [[ $var == "-D"* ]]; then
    JAVA_ARGUMENTS="${JAVA_ARGUMENTS} $var"
  else
    ARGUMENTS="${ARGUMENTS} $var"
  fi
done

nohup sh -c "/home/emulator_main --host_port localhost:9010 &"

cd /home/pgadapter
COMMAND="java ${JAVA_ARGUMENTS} -jar pgadapter.jar ${ARGUMENTS}"
echo $COMMAND
exec $COMMAND
