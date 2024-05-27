# Copyright 2024 Google LLC All rights reserved.
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

# [START spanner_create_connection]
import string
import psycopg


def create_connection(host: string, port: int, database: string):
    # Connect to Cloud Spanner using psycopg3 through PGAdapter.
    # 'sslmode=disable' is optional, but adding it reduces the connection time,
    # as psycopg3 will then skip first trying to create an SSL connection.
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("select 'Hello world!' as hello")
            print("Greeting from Cloud Spanner PostgreSQL:", cur.fetchone()[0])
# [END spanner_create_connection]

import sample_runner

sample_runner.parse_arguments()
create_connection(sample_runner.host, sample_runner.port, sample_runner.database)
