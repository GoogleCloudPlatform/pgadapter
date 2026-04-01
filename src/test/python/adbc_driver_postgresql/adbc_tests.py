""" Copyright 2026 Google LLC

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
import sys
import adbc_driver_postgresql.dbapi as dbapi


def select1(conn_string: str):
  with dbapi.connect(conn_string) as conn:
    with conn.cursor() as cur:
      cur.execute("SELECT 1")
      print(cur.fetchone())


def select_string(conn_string: str):
  with dbapi.connect(conn_string) as conn:
    with conn.cursor() as cur:
      cur.execute("SELECT 'foo'")
      print(cur.fetchone())


def select_boolean(conn_string: str):
  with dbapi.connect(conn_string) as conn:
    with conn.cursor() as cur:
      cur.execute("SELECT true")
      print(cur.fetchone())


def select_timestamp(conn_string: str):
  with dbapi.connect(conn_string) as conn:
    with conn.cursor() as cur:
      cur.execute("SELECT '2020-01-01T00:00:00Z'::timestamp")
      print(cur.fetchone())


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Run ADBC tests.")
  parser.add_argument("method", type=str, help="Test method to run")
  parser.add_argument("conn_string", type=str, help="Connection string for PGAdapter")
  args = parser.parse_args()
  
  method = globals().get(args.method)
  if method:
      method(args.conn_string)
  else:
      print(f"Unknown method: {args.method}")
      sys.exit(1)
