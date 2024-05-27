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

# [START spanner_partitioned_dml]
import string
import psycopg


def execute_partitioned_dml(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Change the DML mode that is used by this connection to Partitioned
            # DML. Partitioned DML is designed for bulk updates and deletes.
            # See https://cloud.google.com/spanner/docs/dml-partitioned for more
            # information.
            cur.execute(
                "set spanner.autocommit_dml_mode='partitioned_non_atomic'")

            # The following statement will use Partitioned DML.
            cur.execute("update albums "
                        "set marketing_budget=0 "
                        "where marketing_budget is null")
            print("Updated at least %d albums" % cur.rowcount)

# [END spanner_partitioned_dml]

import sample_runner

sample_runner.parse_arguments()
execute_partitioned_dml(sample_runner.host, sample_runner.port, sample_runner.database)
