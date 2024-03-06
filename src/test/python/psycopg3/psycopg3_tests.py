""" Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

import argparse
from datetime import datetime, date
from decimal import Decimal

import pytz
import psycopg
from psycopg import Copy, Rollback
from psycopg.errors import InFailedSqlTransaction
from psycopg.types.json import Jsonb


def select1(conn_string: str):
  with psycopg.connect(conn_string, autocommit=True) as conn:
    with conn.cursor() as cur:
      cur.execute("SELECT 1")
      print(cur.fetchone())


def show_server_version(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
      print(cur.execute("show server_version").fetchone()[0])


def show_application_name(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    print(conn.execute("show application_name").fetchone()[0])


def query_with_parameter(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    print(conn.execute(
      "SELECT * FROM FOO WHERE BAR=%s",
      ("baz",)).fetchone())


def query_with_parameter_twice(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    for value in ["baz", "foo"]:
      print(conn.execute(
        "SELECT * FROM FOO WHERE BAR=%s",
        (value,)).fetchone())


def query_all_data_types(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    row = conn.execute("SELECT * FROM all_types WHERE col_bigint=1").fetchone()
    print_all_types(row)


def query_all_data_types_with_parameter(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    row = conn.execute(
      "SELECT * FROM all_types WHERE col_bigint=%s", (1,)).fetchone()
    print_all_types(row)


def query_all_data_types_text(conn_string: str):
  query_all_data_types_with_fixed_format(conn_string, False)


def query_all_data_types_binary(conn_string: str):
  query_all_data_types_with_fixed_format(conn_string, True)


def query_all_data_types_with_fixed_format(conn_string: str, binary: bool):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    curs = conn.cursor(binary=binary)
    row = curs.execute("SELECT * FROM all_types WHERE col_bigint=%s",
                       (1,)).fetchone()
    print_all_types(row)


def update_all_data_types(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    curs = conn.execute(
      "UPDATE all_types SET col_bool=%s, col_bytea=%s, col_float4=%s, "
      "col_float8=%s, col_int=%s, col_numeric=%s, col_timestamptz=%s, "
      "col_date=%s, col_varchar=%s, col_jsonb=%s WHERE col_varchar = %s",
      (True, bytearray(b'test_bytes'), 3.14, 3.14, 1,
       Decimal("6.626"), datetime(year=2022, month=3, day=24, hour=6, minute=39,
                                  second=10, microsecond=123456, tzinfo=None),
       date(2022, 4, 2), "test_string", Jsonb({"key": "value"}), "test",))
    print("Update count:", curs.rowcount)


def insert_all_data_types(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    curs = conn.execute(
      "INSERT INTO all_types "
      "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, "
      "col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
      "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
      (100, True, bytearray(b'test_bytes'), 3.14, 3.14, 100, Decimal("6.626"),
       datetime(year=2022, month=3, day=24, hour=6, minute=39, second=10,
                microsecond=123456, tzinfo=pytz.UTC),
       date(2022, 4, 2), "test_string", Jsonb({"key": "value"}),))
    print("Insert count:", curs.rowcount)


def insert_all_data_types_binary(conn_string: str):
  insert_all_data_types_with_format(
    conn_string, "(%b, %b, %b, %b, %b, %b, %b, %b, %b, %b, %b)")


def insert_all_data_types_text(conn_string: str):
  insert_all_data_types_with_format(
    conn_string, "(%t, %t, %t, %t, %t, %t, %t, %t, %t, %t, %t)")


def insert_all_data_types_with_format(conn_string: str, params_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    curs = conn.execute(
      "INSERT INTO all_types "
      "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, "
      "col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
      "values " + params_string,
      (100, True, bytearray(b'test_bytes'), 3.14, 3.14, 100, Decimal("6.626"),
       datetime(year=2022, month=3, day=24, hour=6, minute=39, second=10,
                microsecond=123456, tzinfo=pytz.UTC),
       date(2022, 4, 2), "test_string", Jsonb({"key": "value"}),))
    print("Insert count:", curs.rowcount)


def insert_nulls_all_data_types(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    curs = conn.execute(
      "INSERT INTO all_types "
      "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, "
      "col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
      "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
      (100, None, None, None, None, None, None, None, None, None, None,))
    print("Insert count:", curs.rowcount)


def insert_all_data_types_returning(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    curs = conn.execute(
      "INSERT INTO all_types "
      "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, "
      "col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
      "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning *",
      (1, True, bytearray(b'test'), 3.14, 3.14, 100, Decimal("6.626"),
       datetime(year=2022, month=2, day=16, hour=13, minute=18, second=2,
                microsecond=123456, tzinfo=None),
       date(2022, 3, 29), "test", Jsonb({"key": "value"}),))
    print_all_types(curs.fetchone())


def insert_batch(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    values = create_batch_insert_values(10)
    curs = conn.cursor()
    # executemany is automatically translated to Batch DML by PGAdapter.
    curs.executemany(
      "INSERT INTO all_types "
      "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, "
      "col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
      "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values)
    print("Insert count:", curs.rowcount)


def mixed_batch(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    values = create_batch_insert_values(5)
    insert_curs = conn.cursor()
    select_curs = conn.cursor()
    update_curs = conn.cursor()
    with conn.pipeline():
      insert_curs.executemany(
        "INSERT INTO all_types "
        "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, "
        "col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values)
      select_curs.execute(
        "select count(*) from all_types where col_bool=%s", (True,))
      update_curs.execute(
        "update all_types set col_bool=false where col_bool=%s", (True,))
    print("Insert count:", insert_curs.rowcount)
    print("Count:", select_curs.fetchone())
    print("Update count:", update_curs.rowcount)


def batch_execution_error(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    values = create_batch_insert_values(3)
    curs = conn.cursor()
    try:
      curs.executemany(
        "INSERT INTO all_types "
        "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, "
        "col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values)
      print("Insert count:", curs.rowcount)
    except Exception as exception:
      print("Executing batch failed with error:", exception)


def ddl_batch(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    curs = conn.cursor()
    with conn.pipeline():
      curs.execute("""
        create table singers (
          id         varchar not null primary key,
          version_id int not null,
          first_name varchar,
          last_name  varchar not null,
          full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,
          active     boolean,
          created_at timestamptz,
          updated_at timestamptz
        )""")
      curs.execute("""
        create table albums (
            id               varchar not null primary key,
            version_id       int not null,
            title            varchar not null,
            marketing_budget numeric,
            release_date     date,
            cover_picture    bytea,
            singer_id        varchar not null,
            created_at       timestamptz,
            updated_at       timestamptz,
            constraint fk_albums_singers foreign key (singer_id) references singers (id)
        )""")
      curs.execute("""
        create table tracks (
            id           varchar not null,
            track_number bigint not null,
            version_id   int not null,
            title        varchar not null,
            sample_rate  float8 not null,
            created_at   timestamptz,
            updated_at   timestamptz,
            primary key (id, track_number)
        ) interleave in parent albums on delete cascade""")
      curs.execute("""
        create table venues (
            id          varchar not null primary key,
            version_id  int not null,
            name        varchar not null,
            description jsonb not null,
            created_at  timestamptz,
            updated_at  timestamptz
        )""")
      curs.execute("""
        create table concerts (
            id          varchar not null primary key,
            version_id  int not null,
            venue_id    varchar not null,
            singer_id   varchar not null,
            name        varchar not null,
            start_time  timestamptz not null,
            end_time    timestamptz not null,
            created_at  timestamptz,
            updated_at  timestamptz,
            constraint fk_concerts_venues  foreign key (venue_id)  references venues  (id),
            constraint fk_concerts_singers foreign key (singer_id) references singers (id),
            constraint chk_end_time_after_start_time check (end_time > start_time)
        )""")
    print("Update count:", curs.rowcount)


def ddl_script(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    curs = conn.execute("""
      -- Executing the schema creation in a batch will improve execution speed.
      start batch ddl;
      
      create table singers (
          id         varchar not null primary key,
          version_id int not null,
          first_name varchar,
          last_name  varchar not null,
          full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,
          active     boolean,
          created_at timestamptz,
          updated_at timestamptz
      );
      
      create table albums (
          id               varchar not null primary key,
          version_id       int not null,
          title            varchar not null,
          marketing_budget numeric,
          release_date     date,
          cover_picture    bytea,
          singer_id        varchar not null,
          created_at       timestamptz,
          updated_at       timestamptz,
          constraint fk_albums_singers foreign key (singer_id) references singers (id)
      );
      
      create table tracks (
          id           varchar not null,
          track_number bigint not null,
          version_id   int not null,
          title        varchar not null,
          sample_rate  float8 not null,
          created_at   timestamptz,
          updated_at   timestamptz,
          primary key (id, track_number)
      ) interleave in parent albums on delete cascade;
      
      create table venues (
          id          varchar not null primary key,
          version_id  int not null,
          name        varchar not null,
          description jsonb not null,
          created_at  timestamptz,
          updated_at  timestamptz
      );
      
      create table concerts (
          id          varchar not null primary key,
          version_id  int not null,
          venue_id    varchar not null,
          singer_id   varchar not null,
          name        varchar not null,
          start_time  timestamptz not null,
          end_time    timestamptz not null,
          created_at  timestamptz,
          updated_at  timestamptz,
          constraint fk_concerts_venues  foreign key (venue_id)  references venues  (id),
          constraint fk_concerts_singers foreign key (singer_id) references singers (id),
          constraint chk_end_time_after_start_time check (end_time > start_time)
      );
      
      run batch;
    """)
    print("Update count:", curs.rowcount)


def binary_copy_in(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
      with cur.copy("COPY all_types FROM STDIN (FORMAT BINARY)") as copy:
        # We must instruct psycopg3 exactly which types we are using when using
        # binary copy.
        copy.set_types(["bigint", "boolean", "bytea", "float4", "float8",
                        "bigint", "numeric", "timestamptz", "date", "varchar",
                        "jsonb"])
        write_copy_data(copy)
      print("Copy count:", cur.rowcount)


def text_copy_in(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
      with cur.copy("COPY all_types FROM STDIN") as copy:
        write_copy_data(copy)
      print("Copy count:", cur.rowcount)


def write_copy_data(copy: Copy):
  copy.write_row((1, True, b'test_bytes', 3.14, 3.14, 10, Decimal("6.626"),
                  datetime(year=2022, month=3, day=24, hour=12, minute=39,
                           second=10, microsecond=123456, tzinfo=pytz.UTC),
                  date(2022, 7, 1), "test", Jsonb({"key": "value"})))
  copy.write_row((2,
                  None, None, None, None, None, None, None, None, None, None,))


def binary_copy_out(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
      with cur.copy("COPY all_types (col_bigint, col_bool, col_bytea, "
                    "col_float4, col_float8, col_int, col_numeric, "
                    "col_timestamptz, col_date, col_varchar, col_jsonb,"
                    "col_array_bigint, col_array_bool, col_array_bytea, "
                    "col_array_float4, col_array_float8, col_array_int, "
                    "col_array_numeric, col_array_timestamptz, col_array_date, "
                    "col_array_varchar, col_array_jsonb) "
                    "TO STDOUT (FORMAT BINARY)") as copy:
        # We must instruct psycopg3 exactly which types we are using when using
        # binary copy.
        copy.set_types(["bigint", "boolean", "bytea", "float4", "float8",
                        "bigint", "numeric", "timestamptz", "date", "varchar",
                        "jsonb", "bigint[]", "boolean[]", "bytea[]", "float4[]",
                        "float8[]", "bigint[]", "numeric[]", "timestamptz[]",
                        "date[]", "varchar[]", "jsonb[]"])
        for row in copy.rows():
          print_all_types(row)


def text_copy_out(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
      with cur.copy("COPY all_types (col_bigint, col_bool, col_bytea, "
                    "col_float4, col_float8, col_int, col_numeric, "
                    "col_timestamptz, col_date, col_varchar, col_jsonb,"
                    "col_array_bigint, col_array_bool, col_array_bytea, "
                    "col_array_float4, col_array_float8, col_array_int, "
                    "col_array_numeric, col_array_timestamptz, col_array_date, "
                    "col_array_varchar, col_array_jsonb) TO STDOUT") as copy:
        copy.set_types(["bigint", "boolean", "bytea", "float4", "float8",
                        "bigint", "numeric", "timestamptz", "date", "varchar",
                        "jsonb", "bigint[]", "boolean[]", "bytea[]", "float4[]",
                        "float8[]", "bigint[]", "numeric[]", "timestamptz[]",
                        "date[]", "varchar[]", "jsonb[]"])
        for row in copy.rows():
          print_all_types(row)


def prepare_query(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    for i in range(2):
      row = conn.execute("SELECT * FROM all_types WHERE col_bigint=%s",
                         (i+1,), prepare=True).fetchone()
      print_all_types(row)


def int8_param(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = True
    row = conn.execute("SELECT * FROM all_types WHERE col_bigint=%s",
                       (2147483648,), prepare=True).fetchone()
    print(row)


def read_write_transaction(conn_string: str):
  insert_sql = "INSERT INTO all_types " \
        "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, " \
        "col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) " \
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
  with psycopg.connect(conn_string) as conn:
    print(conn.execute("SELECT 1").fetchone())
    curs = conn.execute(
      insert_sql,
      (10, True, bytearray(b'test_bytes'), 3.14, 3.14, 100, Decimal("6.626"),
       datetime(year=2022, month=3, day=24, hour=6, minute=39, second=10,
                microsecond=123456, tzinfo=None),
       date(2022, 4, 2), "test_string", Jsonb({"key": "value"}),))
    print("Insert count:", curs.rowcount)
    curs = conn.execute(
      insert_sql,
      (20, True, bytearray(b'test_bytes'), 3.14, 3.14, 100, Decimal("6.626"),
       datetime(year=2022, month=3, day=24, hour=6, minute=39, second=10,
                microsecond=123456, tzinfo=None),
       date(2022, 4, 2), "test_string", Jsonb({"key": "value"}),))
    print("Insert count:", curs.rowcount)
    conn.commit()


def read_only_transaction(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.read_only = True
    print(conn.execute("SELECT 1").fetchone())
    print(conn.execute("SELECT 2").fetchone())
    conn.commit()


# This method is currently not being used, as named cursors are not yet
# supported.
def named_cursor(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    conn.autocommit = False
    with conn.cursor(name="my_cursor") as curs:
      row = curs.execute(
        "SELECT * FROM all_types WHERE col_bigint=1").fetchone()
      print_all_types(row)
    conn.commit()


def nested_transaction(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    with conn.transaction() as tx1:
      with conn.transaction() as tx2:
        row = conn.execute(
          "SELECT * FROM all_types WHERE col_bigint=%s", (1,)).fetchone()
        print_all_types(row)


def rollback_nested_transaction(conn_string: str):
  with psycopg.connect(conn_string) as conn:
    with conn.transaction():
      try:
        with conn.transaction():
          conn.execute(
            "SELECT * FROM all_types WHERE col_bigint=%s", (1,)).fetchone()
          raise ValueError("Test rollback of savepoint")
        print("Nested transaction succeeded")
      except ValueError as e:
        # We should come here, as the inner transaction always raises an error.
        print("Nested transaction failed with error:", e)
      try:
        conn.execute(
          "SELECT * FROM all_types WHERE col_bigint=%s", (1,)).fetchone()
        print("Rolling back to a savepoint succeeded")
      except InFailedSqlTransaction as e:
        print("Outer transaction failed with error:", e)
        raise Rollback()


def create_batch_insert_values(batch_size: int):
  values = []
  for i in range(batch_size):
    values.append(
      (100+i, i % 2 == 0, bytearray(b'%dtest_bytes' % i), 3.14 + i, 3.14 + i, i,
       Decimal(i) + Decimal("0.123"),
       datetime(year=2022, month=3, day=24, hour=i, minute=39, second=10,
                microsecond=123456, tzinfo=None),
       date(2022, 4, i + 1), "test_string%d" % i,
       Jsonb({"key": "value%d" % i}),)
    )
  return values


def print_all_types(row):
  print("col_bigint:",      row[0])
  print("col_bool:",        row[1])
  print("col_bytea:",       None if row[2] is None else bytes(row[2]))
  print("col_float4:",      row[3])
  print("col_float8:",      row[4])
  print("col_int:",         row[5])
  print("col_numeric:",     row[6])
  print("col_timestamptz:", None if row[7] is None else row[7].astimezone(pytz.UTC))
  print("col_date:",        row[8])
  print("col_string:",      row[9])
  print("col_jsonb:",       row[10])
  print("col_array_bigint:",      row[11])
  print("col_array_bool:",        row[12])
  print("col_array_bytea:",       None if row[13] is None else list(map(lambda x: None if x is None else bytes(x), row[13])))
  print("col_array_float4:",      row[14])
  print("col_array_float8:",      row[15])
  print("col_array_int:",         row[16])
  print("col_array_numeric:",     row[17])
  print("col_array_timestamptz:", None if row[18] is None else list(map(lambda x: None if x is None else x.astimezone(pytz.UTC), row[18])))
  print("col_array_date:",        row[19])
  print("col_array_string:",      row[20])
  print("col_array_jsonb:",       row[21])


parser = argparse.ArgumentParser(description='Run psycopg3 test.')
parser.add_argument('method', type=str, help='Test method to run')
parser.add_argument('conn_string', type=str, help='Connection string for '
                                                  'PGAdapter')
args = parser.parse_args()
method = globals()[args.method]
method(args.conn_string)
