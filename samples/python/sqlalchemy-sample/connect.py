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

from sqlalchemy import create_engine, event
from model import *
import sys


# Creates a database engine that can be used with the sample.py script in this
# directory. This function assumes that the host, port and database name was
# given as command line arguments.
def create_test_engine(autocommit: bool=False, options: str=""):
  parser = argparse.ArgumentParser(description='Run SQLAlchemy sample.')
  parser.add_argument('host', type=str, help='host to connect to')
  parser.add_argument('port', type=int, help='port number to connect to')
  parser.add_argument('database', type=str, help='database to connect to', default='d')
  args = parser.parse_args()

  conn_string = "postgresql+psycopg2://user:password@{host}:{port}/" \
               "{database}{options}".format(host=args.host,
                                            port=args.port,
                                            database=args.database,
                                            options=options)
  if args.host == "":
    if options == "":
      conn_string = conn_string + "?host=/tmp"
    else:
      conn_string = conn_string + "&host=/tmp"
  engine = create_engine(conn_string, future=True)
  if autocommit:
    engine = engine.execution_options(isolation_level="AUTOCOMMIT")
  return engine


def register_event_listener_for_prepared_statements(engine):
  # Register an event listener for this engine that creates prepared statements
  # for each connection that is created. The prepared statement definitions can
  # be added to the model classes.
  @event.listens_for(engine, "connect")
  def connect(dbapi_connection, connection_record):
    cursor_obj = dbapi_connection.cursor()
    for model in BaseMixin.__subclasses__():
      if model.__prepare_statements__ is not None:
        for prepare_statement in model.__prepare_statements__:
          cursor_obj.execute(prepare_statement)

    cursor_obj.close()
