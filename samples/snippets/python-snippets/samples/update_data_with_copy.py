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
import os
# [START spanner_update_data]
import string
import psycopg


def update_data_with_copy(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Instruct PGAdapter to use insert-or-update for COPY statements.
            # This enables us to use COPY to update data.
            cur.execute("set spanner.copy_upsert=true")

            # COPY uses mutations to insert or update data in Spanner.
            with cur.copy("COPY albums (singer_id, album_id, marketing_budget) "
                          "FROM STDIN") as copy:
                copy.write_row((1, 1, 100000))
                copy.write_row((2, 2, 500000))
            print("Updated %d albums" % cur.rowcount)
# [END spanner_update_data]
