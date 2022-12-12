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

from sqlalchemy import create_engine, event
from model import *
import sys


def create_test_engine(autocommit=False, options=""):
  host = sys.argv[1]
  port = sys.argv[2]
  database = sys.argv[3] if len(sys.argv) > 3 else "d"
  conn_string = "postgresql+psycopg2://user:password@{host}:{port}/" \
               "{database}{options}".format(host=host,
                                            port=port,
                                            database=database,
                                            options=options)
  if host == "":
    if options == "":
      conn_string = conn_string + "?host=/tmp"
    else:
      conn_string = conn_string + "&host=/tmp"
  engine = create_engine(conn_string, future=True)
  if autocommit:
    engine = engine.execution_options(isolation_level="AUTOCOMMIT")
  return engine


def register_event_listener_for_prepared_statements(engine):
  # Register an event listener for this engine that creates a prepared statement
  # for each connection that is created.
  @event.listens_for(engine, "connect")
  def connect(dbapi_connection, connection_record):
    cursor_obj = dbapi_connection.cursor()
    for model in BaseMixin.__subclasses__():
      if not model.__prepare_statements__ is None:
        for prepare_statement in model.__prepare_statements__:
          cursor_obj.execute(prepare_statement)

    cursor_obj.close()
