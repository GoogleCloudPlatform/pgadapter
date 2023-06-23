""" Copyright 2022 Google LLC

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

from sqlalchemy import create_engine


def create_test_engine(autocommit=False, options=""):
  parser = argparse.ArgumentParser(description='Run SQLAlchemy tests.')
  parser.add_argument('host', type=str, help='host to connect to')
  parser.add_argument('port', type=int, help='port number to connect to')
  parser.add_argument('database', type=str, help='database to connect to')
  args = parser.parse_args()

  conn_string = "postgresql+psycopg://user:password@{host}:{port}/d{options}".format(
    host=args.host, port=args.port, options=options)
  if args.host == "":
    if options == "":
      conn_string = conn_string + "?host=/tmp"
    else:
      conn_string = conn_string + "&host=/tmp"
  conn = create_engine(conn_string, future=True)
  if autocommit:
    return conn.execution_options(isolation_level="AUTOCOMMIT")
  return conn
