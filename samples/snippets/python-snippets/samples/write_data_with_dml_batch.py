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

# [START spanner_dml_batch]
import string
import psycopg


def write_data_with_dml_batch(host: string, port: int, database: string):
    with psycopg.connect("host={host} port={port} dbname={database} "
                         "sslmode=disable".format(host=host,
                                                  port=port,
                                                  database=database)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO singers (singer_id, first_name, last_name) "
                            "VALUES (%s, %s, %s)",
                            [(16, "Sarah", "Wilson",),
                             (17, "Ethan", "Miller",),
                             (18, "Maya", "Patel",),])
            print("%d records inserted" % cur.rowcount)
# [END spanner_dml_batch]
