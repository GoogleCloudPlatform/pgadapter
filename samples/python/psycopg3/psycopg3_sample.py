# Copyright 2023 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Sample for connecting to PGAdapter with psycopg3

This sample application shows how to connect to PGAdapter and Cloud Spanner
using the PostgreSQL psycopg3 driver. The sample starts PGAdapter in an embedded
Docker container and then connects through this container to the Cloud Spanner
emulator or real Cloud Spanner.

The sample uses the Cloud Spanner emulator by default.

This sample requires Docker to be installed on the local environment.

Usage (emulator):
  python psycopg3_sample.py

Usage (Cloud Spanner):
  python psycopg3_sample.py -p my-project -i my-instance -d my-database
"""

import argparse
import psycopg
from pgadapter import start_pgadapter

parser = argparse.ArgumentParser(
  prog='PGAdapter psycopg3 sample',
  description='Sample application for using psycopg3 with PGAdapter')
parser.add_argument('-e', '--emulator', required=False, default=None,
                    help="Set this option to 'True' to force PGAdapter to connect to the emulator.")
parser.add_argument('-p', '--project', default="my-project",
                    help="The Google Cloud project containing the Cloud Spanner instance that PGAdapter should connect to.")
parser.add_argument('-i', '--instance', default="my-instance",
                    help="The Cloud Spanner instance that PGAdapter should connect to.")
parser.add_argument('-d', '--database', default="my-database",
                    help="The Cloud Spanner database that psycopg3 should connect to.")
parser.add_argument('-c', '--credentials', required=False,
                    help="The credentials file that PGAdapter should use to connect to Cloud Spanner. If None, then the sample application will try to use the default credentials in the environment.")
args = parser.parse_args()

if (args.project == "my-project"
    and args.instance == "my-instance"
    and args.database == "my-database"
    and args.emulator is None):
  use_emulator = True
else:
  if args.emulator is None:
    use_emulator = False
  else:
    use_emulator = args.emulator == "True"

# Start PGAdapter in an embedded container.
container, port = start_pgadapter(args.project,
                                  args.instance,
                                  use_emulator,
                                  args.credentials)
try:
  print("PGAdapter running on port ", port, "\n")

  # Connect to Cloud Spanner using psycopg3 by connecting to PGAdapter that is
  # running in the embedded container.
  with psycopg.connect("host=localhost port={port} "
                       "dbname={database} "
                       "sslmode=disable"
                           .format(port=port, database=args.database)) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
      cur.execute("select 'Hello world!' as hello")
      print("Greeting from Cloud Spanner PostgreSQL:", cur.fetchone()[0], "\n")
finally:
  if container is not None:
    container.stop()
