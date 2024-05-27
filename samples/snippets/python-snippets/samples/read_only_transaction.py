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

# [START spanner_read_only_transaction]
import string
import psycopg


def read_only_transaction(host: string, port: int, database: string):
    with (psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn):
        # Set autocommit=False to enable transactions.
        conn.autocommit = False

        with conn.cursor() as cur:
            # Change the current transaction to a read-only transaction.
            # This statement can only be executed at the start of a transaction.
            cur.execute("set transaction read only")

            # The following two queries use the same read-only transaction.
            cur.execute("select singer_id, album_id, album_title "
                        "from albums "
                        "order by singer_id, album_id")
            for album in cur:
                print(album)

            cur.execute("select singer_id, album_id, album_title "
                        "from albums "
                        "order by album_title")
            for album in cur:
                print(album)

        # Read-only transactions must also be committed or rolled back to mark
        # the end of the transaction. There is no semantic difference between
        # rolling back or committing a read-only transaction.
        conn.commit()
# [END spanner_read_only_transaction]

import sample_runner

sample_runner.parse_arguments()
read_only_transaction(sample_runner.host, sample_runner.port, sample_runner.database)
