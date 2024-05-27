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

# [START spanner_query_with_parameter]
import string
import psycopg


def query_data_with_parameter(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT singer_id, first_name, last_name "
                        "FROM singers "
                        "WHERE last_name = %s", ("Garcia",))
            for singer in cur:
                print(singer)
# [END spanner_query_with_parameter]

import sample_runner

sample_runner.parse_arguments()
query_data_with_parameter(sample_runner.host, sample_runner.port, sample_runner.database)
