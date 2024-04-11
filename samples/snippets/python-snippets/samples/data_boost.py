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

# [START spanner_data_boost]
import string
import psycopg


def data_boost(host: string, port: int, database: string):
    with (psycopg.connect("host={host} port={port} dbname={database} "
                          "sslmode=disable".format(host=host,
                                                   port=port,
                                                   database=database)) as conn):
        # Set autocommit=True so each query uses a separate transaction.
        conn.autocommit = True

        with conn.cursor() as cur:
            # This enables Data Boost for all partitioned queries on this
            # connection.
            cur.execute("set spanner.data_boost_enabled=true")

            # Run a partitioned query. This query will use Data Boost.
            cur.execute("run partitioned query "
                        "select singer_id, first_name, last_name "
                        "from singers")
            for singer in cur:
                print(singer)
# [END spanner_data_boost]
