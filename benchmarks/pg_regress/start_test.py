"""Run the pg_regress test suite with a selected list for spanner.
"""

import argparse
import os
import sys
import subprocess
import time
import uuid


OSS_PG_VERSION=16


current_script_cwd = os.getcwd()
print(f"Current working directory of Python script: {current_script_cwd}")

def execute_cmd(command, subprocess_dir="./"):
    print(f"Executing command: {command}")
    try:
        # Execute the command and capture output
        # capture_output=True captures stdout and stderr
        # text=True decodes output as text using default encoding
        proc = subprocess.Popen(command,
                                cwd=subprocess_dir,
                                shell=True,
                                text=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        print("Command Output:")
        stdout = []
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            stdout.append(line)
            print(line.rstrip())

        # Access any error output
        _, stderr = proc.communicate()
        if stderr:
            print("\nError Output:")
            print(stderr)

        # Access the exit code
        print(f"\nExit Code: {proc.returncode}")
        return "".join(stdout)

    except subprocess.CalledProcessError as e:
        # Handle errors if the command returns a non-zero exit code
        print(f"Error executing command: {e}")
        print(f"Stderr: {e.stderr}")
    except FileNotFoundError:
        print(f"Error: Command '{command}' not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    """Main function to run the pg_regress test suite."""
    parser = argparse.ArgumentParser(description="Run the enhanced pg_regress test suite.")
    parser.add_argument("target", choices=["spanner_emulator", "spanner_prod", "cockroachdb", "oss_pg"], help="The target database provider.")
    parser.add_argument("--project", help="The GCP project.")
    parser.add_argument("--instance", help="The spanner instance name.")
    parser.add_argument("--database", help="The database name.")
    parser.add_argument("--testcases", default="", help="Only run specified test cases. Separated by comma, e.g., 'int8,float8'")
    parser.add_argument("--skip-container", action='store_true', help="Skip to start containers and only run pg_regress test")
    args = parser.parse_args()

    database_name = "test-database"
    username = "root"
    if args.target == "spanner_prod":
      if args.database:
          database_name = args.database
      else:
        random_suffix = str(uuid.uuid4())[:6]
        database_name = f"test-database-{random_suffix}"
    if args.target == "cockroachdb":
      database_name = "defaultdb"
    if args.target == "oss_pg":
      database_name = "postgres"
      username = "postgres"

    if args.target == "spanner_prod":
        if not (args.project and args.instance):
            print("Usage: project and instance must be provided for spanner_prod.")
            sys.exit(1)
        print(f"Spanner database path: projects/{args.project}/instances/{args.instance}/databases/{database_name}")

    # 1. configure (at project root directory)
    execute_cmd("./configure --without-icu --without-readline --without-zlib", "../../..")
    # 2. make pg_regress binary
    execute_cmd("make")
    # 3. start pgadapter + emulator container
    if args.target == "spanner_prod":
        execute_cmd(f"gcloud spanner databases create {database_name} --instance={args.instance} --database-dialect=POSTGRESQL")

    container_id = ""
    if not args.skip_container:
        if args.target == "spanner_emulator":
            execute_cmd("docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator")
            container_id = execute_cmd("docker run -d -p 5432:5432 gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator")
            print(f"Emulator container ID: {container_id}")
        elif args.target == "spanner_prod":
            execute_cmd("docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter")
            container_id = execute_cmd(f"docker run -d -p 5432:5432 -v $HOME/.config/gcloud/application_default_credentials.json:/credentials.json:ro gcr.io/cloud-spanner-pg-adapter/pgadapter -p {args.project} -i {args.instance} -d {database_name} -c /credentials.json -x")
            print(f"Pgadapter container ID: {container_id}")
        elif args.target == "cockroachdb":
            execute_cmd("docker pull cockroachdb/cockroach:latest")
            container_id = execute_cmd(f"docker run -d -p 5432:26257 -p 8080:8080 cockroachdb/cockroach:latest start-single-node --insecure")
            print(f"CockroachDB container ID: {container_id}")
        elif args.target == "oss_pg":
            execute_cmd(f"docker pull postgres:{OSS_PG_VERSION}")
            container_id = execute_cmd(f"docker run -d -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:{OSS_PG_VERSION}")
            print(f"OSS PG container ID: {container_id}")

        if not container_id:
            print("Error: could not start the container.")
            sys.exit(1)

    print("Program execution paused for 5 seconds...")
    time.sleep(5)

    # 4. test_setup.sql
    execute_cmd(f"./pg_regress --bindir=/usr/bin --host=127.0.0.1 --port=5432 --user={username} --dbname={database_name} --use-existing test_setup")
    # 5. load test data
    prepend_stmts = ""
    if args.target.startswith("spanner_"):
      prepend_stmts = "SET SPANNER.AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC';"
    execute_cmd(f"cat data/onek.data | psql 'postgresql://{username}@127.0.0.1:5432/{database_name}?sslmode=disable' -c \"COPY onek (unique1, unique2, two, four, ten, twenty, hundred, thousand, twothousand, fivethous, tenthous, odd, even, stringu1, stringu2, string4) FROM STDIN\"")
    execute_cmd(f"cat data/tenk.data | psql 'postgresql://{username}@127.0.0.1:5432/{database_name}?sslmode=disable' -c \"{prepend_stmts} COPY tenk1 (unique1, unique2, two, four, ten, twenty, hundred, thousand, twothousand, fivethous, tenthous, odd, even, stringu1, stringu2, string4) FROM STDIN\"")
    # 6. run pg_regress test
    if args.testcases:
        testcases = [testcase.strip() for testcase in args.testcases.split(",")]
    else:
        testcases = [os.path.splitext(filename)[0].removeprefix("sql/") for filename in execute_cmd("ls sql/*.sql").split("\n") if filename != ""]
    # We already ran this before.
    if "test_setup" in testcases:
        testcases.remove("test_setup")
    print(f"Running sql files: {testcases}")
    testcases_string = " ".join(testcases)
    execute_cmd(f"./pg_regress --bindir=/usr/bin --host=127.0.0.1 --port=5432 --user={username} --dbname={database_name} --use-existing {testcases_string}")
    # 7. compare results (json format) to get a score
    execute_cmd(f"python compare_results.py expected/ results/")
    # 8. stop pg_adapter & emulator container
    if container_id:
        execute_cmd(f"docker stop {container_id}")
    if args.target == "spanner_prod":
        execute_cmd(f"gcloud spanner databases delete {database_name} --instance={args.instance} --quiet")

if __name__ == "__main__":
    main()

