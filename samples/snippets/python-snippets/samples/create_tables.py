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
            conn.execute("CREATE TABLE Singers ("
                         + "  SingerId   bigint NOT NULL,"
                         + "  FirstName  character varying(1024),"
                         + "  LastName   character varying(1024),"
                         + "  SingerInfo bytea,"
                         + "  FullName character varying(2048) GENERATED "
                         + "  ALWAYS AS (FirstName || ' ' || LastName) STORED,"
                         + "  PRIMARY KEY (SingerId)"
                         + ")")
            conn.execute("CREATE TABLE Albums ("
                         + "  SingerId     bigint NOT NULL,"
                         + "  AlbumId      bigint NOT NULL,"
                         + "  AlbumTitle   character varying(1024),"
                         + "  PRIMARY KEY (SingerId, AlbumId)"
                         + ") INTERLEAVE IN PARENT Singers ON DELETE CASCADE")
        print("Created Singers & Albums tables in database: [{database}]"
              .format(database=database))
