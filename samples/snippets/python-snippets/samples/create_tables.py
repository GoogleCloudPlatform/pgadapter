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

# [START spanner_create_database]
import string
import psycopg


def create_tables(host: string, port: int, database: string):
    # Connect to Cloud Spanner using psycopg3 through PGAdapter.
    with psycopg.connect("host={host} port={port} "
                         "dbname={database} "
                         "sslmode=disable".format(host=host, port=port,
                                                  database=database)) as conn:
        # Enable autocommit to execute DDL statements, as psycopg otherwise
        # tries to use a read/write transaction.
        conn.autocommit = True

        # Use a pipeline to execute multiple DDL statements in one batch.
        with conn.pipeline():
            conn.execute("create table singers ("
                         + "  singer_id   bigint primary key not null,"
                         + "  first_name  character varying(1024),"
                         + "  last_name   character varying(1024),"
                         + "  singer_info bytea,"
                         + "  full_name   character varying(2048) generated "
                         + "  always as (first_name || ' ' || last_name) stored"
                         + ")")
            conn.execute("create table albums ("
                         + "  singer_id     bigint not null,"
                         + "  album_id      bigint not null,"
                         + "  album_title   character varying(1024),"
                         + "  primary key (singer_id, album_id)"
                         + ") interleave in parent singers on delete cascade")
        print("Created Singers & Albums tables in database: [{database}]"
              .format(database=database))
# [END spanner_create_database]

import sample_runner

sample_runner.parse_arguments()
create_tables(sample_runner.host, sample_runner.port, sample_runner.database)
