#!/bin/bash

ARGUMENTS="-p emulator-project -i test-instance -r autoConfigEmulator=true -c \"\" -x"
JAVA_ARGUMENTS=""

# The message that the emulator prints once it is ready to accept requests. The emulator only
# prints this after it has verified that its gRPC server is up, which is why this is used instead
# of checking whether the port is open. The port is opened before the emulator is ready to serve.
EMULATOR_READY_MESSAGE="Cloud Spanner emulator running."
# The maximum time to wait for the emulator to start. PGAdapter is also started if the emulator
# does not start within this time, so a slow or failing emulator can never prevent this container
# from starting.
EMULATOR_STARTUP_TIMEOUT_SECONDS=30
# The emulator log is copied to this file, so it can be scanned for the message above. The emulator
# only logs a handful of lines after startup, so this file remains small.
EMULATOR_LOG_FILE="$(mktemp)"

for var in "$@"
do
  if [[ $var == "-D"* ]]; then
    JAVA_ARGUMENTS="${JAVA_ARGUMENTS} $var"
  else
    ARGUMENTS="${ARGUMENTS} $var"
  fi
done

# Returns successfully once the emulator has logged that it is running. Returns 1 if the emulator
# stops or if it does not start within EMULATOR_STARTUP_TIMEOUT_SECONDS.
wait_for_emulator() {
  local deadline=$((SECONDS + EMULATOR_STARTUP_TIMEOUT_SECONDS))
  until grep -q "${EMULATOR_READY_MESSAGE}" "${EMULATOR_LOG_FILE}"; do
    if ! kill -0 "${EMULATOR_PID}" 2>/dev/null; then
      echo "The Spanner emulator stopped before it had started." >&2
      return 1
    fi
    if ((SECONDS >= deadline)); then
      echo "The Spanner emulator did not start within ${EMULATOR_STARTUP_TIMEOUT_SECONDS} seconds." >&2
      return 1
    fi
    sleep 0.1
  done
}

# The emulator logs to stderr. Copy everything it logs to both the container log and the file that
# is scanned above.
nohup /emulator/gateway_main --hostname "0.0.0.0" > >(tee "${EMULATOR_LOG_FILE}") 2>&1 &
EMULATOR_PID=$!

# Wait for the emulator before starting PGAdapter. Clients consider this container to be ready as
# soon as PGAdapter is listening on port 5432. Starting PGAdapter first therefore means that a
# client can connect before the emulator is up, and that connection then fails with 'no running
# emulator or other server could be found at localhost:9010'.
echo "Waiting for the Spanner emulator to start"
if wait_for_emulator; then
  echo "The Spanner emulator is running"
else
  echo "Starting PGAdapter without a running Spanner emulator." >&2
fi

cd /home/pgadapter
COMMAND="java ${JAVA_ARGUMENTS} -jar pgadapter.jar ${ARGUMENTS}"
echo $COMMAND
exec $COMMAND
