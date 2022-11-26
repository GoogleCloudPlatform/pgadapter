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

from sqlalchemy import create_engine
import sys


def create_test_engine(autocommit=False, options=""):
  host = sys.argv[1]
  port = sys.argv[2]
  connString = "postgresql+psycopg2://user:password@{host}:{port}/d{options}".format(
    host=host, port=port, options=options)
  if host == "":
    if options == "":
      connString = connString + "?host=/tmp"
    else:
      connString = connString + "&host=/tmp"
  conn = create_engine(connString, future=True)
  if autocommit:
    return conn.execution_options(isolation_level="AUTOCOMMIT")
  return conn
